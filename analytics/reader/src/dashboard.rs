//! Runs a table's dashboard panels and hands back plain values.
//!
//! The panels come from the plugin (see `wit/analytics.wit`). A table
//! without any gets panels made from its columns: a row count, rows over
//! time if it has a timestamp column, and the newest rows.

use anyhow::{Result, anyhow};
use deltalake::arrow::datatypes::DataType;
use futures_util::future::join_all;

use crate::frame::{Frame, Values};
use crate::manifest::{Format, Panel, PanelKind};
use crate::{Session, quote};

/// Most rows a table panel keeps, so a careless `SELECT *` can't make the
/// window sluggish.
pub const MAX_TABLE_ROWS: usize = 1_000;
/// Most points or bars a chart keeps.
pub const MAX_CHART_ROWS: usize = 5_000;

#[derive(Debug, Clone, PartialEq)]
pub struct Dashboard {
    /// The Delta version this was read from.
    pub version: Option<i64>,
    pub title: String,
    pub description: String,
    /// The panels came from the table's columns, not from the plugin.
    pub generated: bool,
    pub panels: Vec<PanelData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PanelData {
    pub panel: Panel,
    /// The rows, or why the query failed. One failing panel doesn't stop
    /// the others.
    pub frame: Result<Frame, String>,
    /// For time series: the bucket length chosen for `{bucket}`.
    pub bucket_seconds: Option<i64>,
    /// The result had more rows than are kept.
    pub truncated: bool,
    /// How long the panel's queries took, in milliseconds.
    pub millis: u64,
}

/// The calendar period a panel's `{period}` stands for, which the dashboard
/// lets the viewer switch: `INTERVAL '1 day'` or `INTERVAL '7 days'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Period {
    #[default]
    Day,
    Week,
}

impl Period {
    pub fn seconds(self) -> i64 {
        match self {
            Period::Day => 86_400,
            Period::Week => 7 * 86_400,
        }
    }
}

/// Whether a panel lets the viewer switch between days and weeks.
pub fn uses_period(panel: &Panel) -> bool {
    panel.sql.contains("{period}")
}

/// Runs every panel over the session's current version, all at once, with
/// `{period}` as a day.
pub async fn load(session: &Session) -> Result<Dashboard> {
    load_with(session, Period::Day).await
}

/// Runs every panel, with `{period}` as the given period.
pub async fn load_with(session: &Session, period: Period) -> Result<Dashboard> {
    let (panels, generated) = match session.manifest.as_ref().map(|m| &m.panels) {
        Some(panels) if !panels.is_empty() => (panels.clone(), false),
        _ => (generated_panels(session)?, true),
    };
    let panels = join_all(panels.into_iter().map(|p| run_panel(session, p, period))).await;
    let (title, description) = match &session.manifest {
        Some(m) => (m.name.clone(), m.description.clone()),
        None => (session.name.clone(), String::new()),
    };
    Ok(Dashboard {
        version: session.version(),
        title,
        description,
        generated,
        panels,
    })
}

async fn run_panel(session: &Session, panel: Panel, period: Period) -> PanelData {
    let started = std::time::Instant::now();
    let millis = || started.elapsed().as_millis() as u64;
    let mut bucket_seconds = None;
    let mut sql = panel.sql.clone();
    if uses_period(&panel) {
        let seconds = period.seconds();
        bucket_seconds = Some(seconds);
        sql = sql.replace("{period}", &format!("INTERVAL '{seconds} seconds'"));
    }
    if panel.kind == PanelKind::Timeseries && sql.contains("{bucket}") {
        let range = match &panel.time_column {
            Some(column) => time_range(session, column).await,
            None => Ok(None),
        };
        match range {
            // A number column, such as an offset: buckets of a round width.
            Ok(Some(Range {
                lo,
                hi,
                time: false,
            })) => {
                sql = sql.replace("{bucket}", &width_for(hi - lo).to_string());
            }
            // An empty table: a number column still needs a number, or the
            // query fails before it finds there are no rows.
            Ok(None) if is_number_column(session, panel.time_column.as_deref()) => {
                sql = sql.replace("{bucket}", "1");
            }
            Ok(range) => {
                let seconds = range.map_or(3_600, |r| bucket_for(r.hi - r.lo));
                bucket_seconds = Some(seconds);
                sql = sql.replace("{bucket}", &format!("INTERVAL '{seconds} seconds'"));
            }
            Err(e) => {
                return PanelData {
                    panel,
                    frame: Err(format!("{e:#}")),
                    bucket_seconds,
                    truncated: false,
                    millis: millis(),
                };
            }
        }
    }
    let limit = if panel.kind == PanelKind::Table {
        MAX_TABLE_ROWS
    } else {
        MAX_CHART_ROWS
    };
    let result = async {
        let batches = session.sql(&sql).await?;
        let mut frame = Frame::from_batches(&batches, |c| panel.format(c))?;
        let truncated = frame.rows() > limit;
        if truncated {
            truncate(&mut frame, limit);
        }
        if let (PanelKind::Timeseries, Some(bucket)) = (panel.kind, bucket_seconds) {
            fill_gaps(&mut frame, bucket);
        }
        Ok::<_, anyhow::Error>((frame, truncated))
    }
    .await;
    match result {
        Ok((frame, truncated)) => PanelData {
            panel,
            frame: Ok(frame),
            bucket_seconds,
            truncated,
            millis: millis(),
        },
        Err(e) => PanelData {
            panel,
            frame: Err(format!("{e:#}")),
            bucket_seconds,
            truncated: false,
            millis: millis(),
        },
    }
}

/// The smallest and largest value of a time series' x column.
struct Range {
    lo: i64,
    hi: i64,
    /// A timestamp column, in seconds; else a number column.
    time: bool,
}

async fn time_range(session: &Session, column: &str) -> Result<Option<Range>> {
    let c = quote(column);
    let sql = format!(
        "SELECT min({c}) AS lo, max({c}) AS hi FROM {}",
        quote(&session.name)
    );
    let frame = Frame::from_batches(&session.sql(&sql).await?, |_| Format::Auto)?;
    let get = |name: &str| -> Result<Option<(i64, bool)>> {
        let column = frame.column(name).ok_or_else(|| anyhow!("no {name}"))?;
        Ok(match &column.values {
            Values::Time(v) => v
                .first()
                .copied()
                .flatten()
                .map(|us| (us.div_euclid(1_000_000), true)),
            Values::Number(v) => v.first().copied().flatten().map(|n| (n as i64, false)),
            Values::Text(_) => {
                return Err(anyhow!(
                    "{column:?} is not a time column",
                    column = column.name
                ));
            }
        })
    };
    Ok(match (get("lo")?, get("hi")?) {
        (Some((lo, time)), Some((hi, _))) => Some(Range { lo, hi, time }),
        _ => None,
    })
}

/// Whether the table's `column` holds numbers rather than timestamps.
fn is_number_column(session: &Session, column: Option<&str>) -> bool {
    let (Some(column), Ok(schema)) = (column, session.schema()) else {
        return false;
    };
    schema
        .field_with_name(column)
        .is_ok_and(|f| f.data_type().is_numeric())
}

/// The smallest round width (1, 2 or 5 times a power of ten) that splits
/// `span` into fewer than 60 buckets, for a number x axis such as offsets.
pub fn width_for(span: i64) -> i64 {
    let mut power = 1_i64;
    loop {
        for step in [1, 2, 5] {
            let width = step * power;
            if span / width < 60 {
                return width;
            }
        }
        power = power.saturating_mul(10);
    }
}

/// Bucket lengths to choose from, in seconds.
// From a second, so a short run (a demo's few purchases) still spreads over
// several points instead of one.
const BUCKETS: [i64; 13] = [
    1,
    5,
    10,
    30,
    60,
    300,
    900,
    3_600,
    6 * 3_600,
    86_400,
    7 * 86_400,
    30 * 86_400,
    365 * 86_400,
];

/// The shortest bucket that splits `span` seconds into fewer than 60 buckets:
/// enough for a trend, few enough that each point means something.
pub fn bucket_for(span: i64) -> i64 {
    BUCKETS
        .into_iter()
        .find(|b| span / b < 60)
        .unwrap_or(BUCKETS[BUCKETS.len() - 1])
}

fn truncate(frame: &mut Frame, rows: usize) {
    for c in &mut frame.columns {
        match &mut c.values {
            Values::Number(v) => v.truncate(rows),
            Values::Time(v) => v.truncate(rows),
            Values::Text(v) => v.truncate(rows),
        }
    }
}

/// For a `(time, value)` series sorted by time: adds the empty buckets with
/// a value of 0, so a quiet hour shows as 0 rather than as a straight line
/// between its neighbours. A series with a third column is left alone.
fn fill_gaps(frame: &mut Frame, bucket: i64) {
    if frame.columns.len() != 2 {
        return;
    }
    let (Values::Time(times), Values::Number(values)) =
        (&frame.columns[0].values, &frame.columns[1].values)
    else {
        return;
    };
    let points: Vec<(i64, Option<f64>)> = times
        .iter()
        .zip(values)
        .filter_map(|(t, v)| t.map(|t| (t, *v)))
        .collect();
    let (Some(first), Some(last)) = (points.first(), points.last()) else {
        return;
    };
    let step = bucket * 1_000_000;
    let on_grid = points.iter().all(|(t, _)| (t - first.0) % step == 0);
    let sorted = points.windows(2).all(|w| w[0].0 < w[1].0);
    if !on_grid || !sorted || (last.0 - first.0) / step >= MAX_CHART_ROWS as i64 {
        return;
    }
    let mut filled_t = Vec::new();
    let mut filled_v = Vec::new();
    let mut next = points.iter().peekable();
    let mut at = first.0;
    while at <= last.0 {
        filled_t.push(Some(at));
        match next.peek() {
            Some((t, v)) if *t == at => {
                filled_v.push(*v);
                next.next();
            }
            _ => filled_v.push(Some(0.0)),
        }
        at += step;
    }
    frame.columns[0].values = Values::Time(filled_t);
    frame.columns[1].values = Values::Number(filled_v);
}

/// Panels for a table without any: what can be said from its columns.
pub fn generated_panels(session: &Session) -> Result<Vec<Panel>> {
    let schema = session.schema()?;
    let table = quote(&session.name);
    let time = schema
        .fields()
        .iter()
        .find(|f| {
            matches!(
                f.data_type(),
                DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
            )
        })
        .map(|f| f.name().clone());
    let mut panels = Vec::new();
    let mut stats = vec!["count(*) AS \"Rows\"".to_string()];
    if let Some(t) = &time {
        stats.push(format!("max({}) AS \"Newest {t}\"", quote(t)));
    }
    panels.push(Panel {
        title: "Totals".into(),
        kind: PanelKind::Stats,
        sql: format!("SELECT {} FROM {table}", stats.join(", ")),
        time_column: None,
        formats: Vec::new(),
    });
    if let Some(t) = &time {
        let c = quote(t);
        panels.push(Panel {
            title: format!("Rows per {t}"),
            kind: PanelKind::Timeseries,
            sql: format!(
                "SELECT date_bin({{bucket}}, {c}) AS time, count(*) AS rows FROM {table} \
                 WHERE {c} IS NOT NULL GROUP BY 1 ORDER BY 1"
            ),
            time_column: Some(t.clone()),
            formats: Vec::new(),
        });
    }
    let order = time
        .as_ref()
        .map(|t| format!(" ORDER BY {} DESC NULLS LAST", quote(t)))
        .unwrap_or_default();
    panels.push(Panel {
        title: "Newest rows".into(),
        kind: PanelKind::Table,
        sql: format!("SELECT * FROM {table}{order} LIMIT {MAX_TABLE_ROWS}"),
        time_column: None,
        formats: Vec::new(),
    });
    Ok(panels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::Column;

    #[test]
    fn picks_a_bucket_for_the_span() {
        assert_eq!(bucket_for(0), 1);
        assert_eq!(bucket_for(7), 1);
        assert_eq!(bucket_for(5 * 60), 10);
        assert_eq!(bucket_for(30 * 60), 60);
        assert_eq!(bucket_for(3 * 3_600), 300);
        assert_eq!(bucket_for(2 * 86_400), 3_600);
        assert_eq!(bucket_for(30 * 86_400), 86_400);
        assert_eq!(bucket_for(100 * 365 * 86_400), 365 * 86_400);
    }

    /// A time series over an offset column of a table with no rows yet:
    /// "no rows", not a query that mixes an offset and an interval.
    #[tokio::test]
    async fn an_offset_axis_works_on_an_empty_table() {
        use deltalake::kernel::{DataType as DeltaType, StructField};
        let dir = std::env::temp_dir().join(format!("reader-empty-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let location = crate::Location::parse(&dir.to_string_lossy(), Default::default()).unwrap();
        location
            .table()
            .await
            .unwrap()
            .create()
            .with_columns([StructField::new("offset", DeltaType::LONG, true)])
            .await
            .unwrap();
        let session = Session::open(&location).await.unwrap();
        let panel = Panel {
            title: "per offset".into(),
            kind: PanelKind::Timeseries,
            sql: format!(
                "SELECT \"offset\" - \"offset\" % {{bucket}} AS x, count(*) AS n FROM {} GROUP BY 1",
                quote(&session.name)
            ),
            time_column: Some("offset".into()),
            formats: Vec::new(),
        };
        let data = run_panel(&session, panel, Period::Day).await;
        assert_eq!(data.frame.map(|f| f.rows()), Ok(0));
        assert_eq!(data.bucket_seconds, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `{period}` is a day or a Monday-to-Sunday week, as the viewer picks.
    #[tokio::test]
    async fn period_switches_between_days_and_weeks() {
        use deltalake::kernel::{DataType as DeltaType, StructField};
        let dir = std::env::temp_dir().join(format!("reader-period-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let location = crate::Location::parse(&dir.to_string_lossy(), Default::default()).unwrap();
        location
            .table()
            .await
            .unwrap()
            .create()
            .with_columns([StructField::new("n", DeltaType::LONG, true)])
            .await
            .unwrap();
        let session = Session::open(&location).await.unwrap();
        // Monday 2 and Sunday 8 November are one week; Monday 9 starts the next.
        let panel = Panel {
            title: "per period".into(),
            kind: PanelKind::Timeseries,
            sql: "SELECT date_bin({period}, t, TIMESTAMP '2001-01-01T00:00:00Z') AS p, sum(n) AS n \
                  FROM (VALUES (TIMESTAMP '2026-11-02T10:00:00Z', 1), (TIMESTAMP '2026-11-08T10:00:00Z', 2), \
                  (TIMESTAMP '2026-11-09T10:00:00Z', 4)) AS v(t, n) GROUP BY 1 ORDER BY 1"
                .into(),
            time_column: None,
            formats: Vec::new(),
        };
        assert!(uses_period(&panel));
        let sums = |d: &PanelData| {
            let f = d.frame.as_ref().unwrap();
            (0..f.rows())
                .map(|r| f.columns[1].values.number(r))
                .collect::<Vec<_>>()
        };
        let weeks = run_panel(&session, panel.clone(), Period::Week).await;
        assert_eq!(weeks.bucket_seconds, Some(7 * 86_400));
        assert_eq!(sums(&weeks), [Some(3.0), Some(4.0)]);
        let days = run_panel(&session, panel, Period::Day).await;
        assert_eq!(days.bucket_seconds, Some(86_400));
        // Eight days, the quiet ones as 0.
        assert_eq!(
            sums(&days),
            [
                Some(1.0),
                Some(0.0),
                Some(0.0),
                Some(0.0),
                Some(0.0),
                Some(0.0),
                Some(2.0),
                Some(4.0)
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn widths_are_round_and_fewer_than_60() {
        assert_eq!(width_for(0), 1);
        assert_eq!(width_for(59), 1);
        assert_eq!(width_for(100), 2);
        assert_eq!(width_for(250), 5);
        assert_eq!(width_for(10_000_000), 200_000);
    }

    #[test]
    fn fills_empty_buckets_with_zero() {
        let h = 3_600_000_000;
        let mut frame = Frame {
            columns: vec![
                Column {
                    name: "t".into(),
                    format: Format::Auto,
                    values: Values::Time(vec![Some(0), Some(3 * h)]),
                },
                Column {
                    name: "n".into(),
                    format: Format::Auto,
                    values: Values::Number(vec![Some(2.0), Some(1.0)]),
                },
            ],
        };
        fill_gaps(&mut frame, 3_600);
        assert_eq!(
            frame.columns[0].values,
            Values::Time(vec![Some(0), Some(h), Some(2 * h), Some(3 * h)])
        );
        assert_eq!(
            frame.columns[1].values,
            Values::Number(vec![Some(2.0), Some(0.0), Some(0.0), Some(1.0)])
        );
    }
}
