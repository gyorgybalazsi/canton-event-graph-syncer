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
}

/// Runs every panel over the session's current version, all at once.
pub async fn load(session: &Session) -> Result<Dashboard> {
    let (panels, generated) = match session.manifest.as_ref().map(|m| &m.panels) {
        Some(panels) if !panels.is_empty() => (panels.clone(), false),
        _ => (generated_panels(session)?, true),
    };
    let panels = join_all(panels.into_iter().map(|p| run_panel(session, p))).await;
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

async fn run_panel(session: &Session, panel: Panel) -> PanelData {
    let mut bucket_seconds = None;
    let mut sql = panel.sql.clone();
    if panel.kind == PanelKind::Timeseries && sql.contains("{bucket}") {
        let seconds = match &panel.time_column {
            Some(column) => time_range(session, column)
                .await
                .map(|r| r.map(|(lo, hi)| bucket_for(hi - lo))),
            None => Ok(None),
        };
        match seconds {
            Ok(seconds) => {
                let seconds = seconds.unwrap_or(3_600);
                bucket_seconds = Some(seconds);
                sql = sql.replace("{bucket}", &format!("INTERVAL '{seconds} seconds'"));
            }
            Err(e) => {
                return PanelData {
                    panel,
                    frame: Err(format!("{e:#}")),
                    bucket_seconds,
                    truncated: false,
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
        },
        Err(e) => PanelData {
            panel,
            frame: Err(format!("{e:#}")),
            bucket_seconds,
            truncated: false,
        },
    }
}

/// The smallest and largest value of a timestamp column, in seconds.
async fn time_range(session: &Session, column: &str) -> Result<Option<(i64, i64)>> {
    let c = quote(column);
    let sql = format!(
        "SELECT min({c}) AS lo, max({c}) AS hi FROM {}",
        quote(&session.name)
    );
    let frame = Frame::from_batches(&session.sql(&sql).await?, |_| Format::Auto)?;
    let get = |name: &str| -> Result<Option<i64>> {
        let column = frame.column(name).ok_or_else(|| anyhow!("no {name}"))?;
        Ok(match &column.values {
            Values::Time(v) => v
                .first()
                .copied()
                .flatten()
                .map(|us| us.div_euclid(1_000_000)),
            Values::Number(v) => v.first().copied().flatten().map(|n| n as i64),
            Values::Text(_) => {
                return Err(anyhow!(
                    "{column:?} is not a time column",
                    column = column.name
                ));
            }
        })
    };
    Ok(match (get("lo")?, get("hi")?) {
        (Some(lo), Some(hi)) => Some((lo, hi)),
        _ => None,
    })
}

/// Bucket lengths to choose from, in seconds.
const BUCKETS: [i64; 9] = [
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

/// The shortest bucket that splits `span` seconds into at most 200 buckets.
pub fn bucket_for(span: i64) -> i64 {
    BUCKETS
        .into_iter()
        .find(|b| span / b < 200)
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
        assert_eq!(bucket_for(0), 60);
        assert_eq!(bucket_for(3 * 3_600), 60);
        assert_eq!(bucket_for(2 * 86_400), 900);
        assert_eq!(bucket_for(30 * 86_400), 6 * 3_600);
        assert_eq!(bucket_for(100 * 365 * 86_400), 365 * 86_400);
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
