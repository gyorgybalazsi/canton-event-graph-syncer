//! How long each Canton contract lived: one row per archived contract, with
//! the offsets and times of its create and its archive.
//!
//! Reads the `created` and `exercised` tables the syncer writes. Contracts
//! that are created but not yet archived stay in the plugin's memory between
//! steps; a row is only written once a consuming choice archives the
//! contract, so rows never change after they are written.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};

wit_bindgen::generate!({
    world: "analytics-plugin",
    path: "../../wit",
});

use exports::canton::analytics::plugin::{
    ColumnFormat, Guest, Panel, PanelKind, PluginInfo, QuerySource, ValueFormat, WriteMode,
};

struct Plugin;

export!(Plugin);

/// Creates and consuming choices with an offset in `($from, $to]`. Contracts
/// loaded from the active contract set (ACS) carry the ACS offset.
const QUERY: &str = "\
SELECT contract_id, template_name, offset, created_at FROM created \
    WHERE offset > $from AND offset <= $to ORDER BY offset;
SELECT target_contract_id, choice_name, offset, transaction_effective_at FROM exercised \
    WHERE consuming = true AND offset > $from AND offset <= $to ORDER BY offset;";

/// The syncer records the last offset it fully wrote in `sync_state:stream`;
/// without it (e.g. sample data) the highest transaction offset is used.
/// The runner takes the result of the last statement.
const HIGH_WATER: &str = "\
LET $synced = IF (INFO FOR DB).tables.sync_state != NONE { \
    (SELECT VALUE last_offset FROM ONLY sync_state:stream) } ELSE { NONE };
RETURN $synced ?? math::max(SELECT VALUE offset FROM transaction) ?? 0;";

/// A contract seen created and not yet archived.
#[derive(Serialize, Deserialize, Clone)]
struct Open {
    template: Option<String>,
    offset: i64,
    created_at: Option<i64>,
}

thread_local! {
    /// Contracts created and not yet archived, by contract id.
    static OPEN: RefCell<HashMap<String, Open>> = RefCell::new(HashMap::new());
}

fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("contract_id", DataType::Utf8, false),
        // Unknown when the contract was created before the data starts.
        Field::new("template_name", DataType::Utf8, true),
        Field::new("created_offset", DataType::Int64, true),
        Field::new("archived_offset", DataType::Int64, false),
        Field::new("created_at", timestamp(), true),
        Field::new("archived_at", timestamp(), true),
        Field::new("lifetime_seconds", DataType::Float64, true),
        Field::new("archived_by", DataType::Utf8, true),
    ]))
}

fn created_input() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("contract_id", DataType::Utf8, true),
        Field::new("template_name", DataType::Utf8, true),
        Field::new("offset", DataType::Int64, true),
        Field::new("created_at", timestamp(), true),
    ]))
}

fn exercised_input() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("target_contract_id", DataType::Utf8, true),
        Field::new("choice_name", DataType::Utf8, true),
        Field::new("offset", DataType::Int64, true),
        Field::new("transaction_effective_at", timestamp(), true),
    ]))
}

fn ipc(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, &batch.schema()).map_err(|e| e.to_string())?;
    writer.write(batch).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    drop(writer);
    Ok(out)
}

fn read(bytes: &[u8], what: &str) -> Result<Vec<RecordBatch>, String> {
    let reader = StreamReader::try_new(bytes, None).map_err(|e| format!("{what}: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{what}: {e}"))
}

/// An event, in ledger order. At the same offset creates come first: a
/// contract can be created and archived in one transaction, never the reverse.
enum Event {
    Created {
        id: String,
        open: Open,
    },
    Archived {
        id: String,
        offset: i64,
        at: Option<i64>,
        choice: Option<String>,
    },
}

impl Event {
    fn key(&self) -> (i64, u8) {
        match self {
            Event::Created { open, .. } => (open.offset, 0),
            Event::Archived { offset, .. } => (*offset, 1),
        }
    }
}

fn opt_str(a: &arrow_array::StringArray, i: usize) -> Option<String> {
    a.is_valid(i).then(|| a.value(i).to_string())
}

fn events(results: &[Vec<u8>]) -> Result<Vec<Event>, String> {
    let [created, exercised] = results else {
        return Err(format!("expected 2 query results, got {}", results.len()));
    };
    let mut events = Vec::new();
    for b in read(created, "created")? {
        let (id, template, offset, at) = (
            b.column(0).as_string::<i32>(),
            b.column(1).as_string::<i32>(),
            b.column(2).as_primitive::<Int64Type>(),
            b.column(3).as_primitive::<TimestampMicrosecondType>(),
        );
        for i in 0..b.num_rows() {
            if id.is_null(i) || offset.is_null(i) {
                continue;
            }
            let open = Open {
                template: opt_str(template, i),
                offset: offset.value(i),
                created_at: at.is_valid(i).then(|| at.value(i)),
            };
            events.push(Event::Created {
                id: id.value(i).to_string(),
                open,
            });
        }
    }
    for b in read(exercised, "exercised")? {
        let (id, choice, offset, at) = (
            b.column(0).as_string::<i32>(),
            b.column(1).as_string::<i32>(),
            b.column(2).as_primitive::<Int64Type>(),
            b.column(3).as_primitive::<TimestampMicrosecondType>(),
        );
        for i in 0..b.num_rows() {
            if id.is_null(i) || offset.is_null(i) {
                continue;
            }
            events.push(Event::Archived {
                id: id.value(i).to_string(),
                offset: offset.value(i),
                at: at.is_valid(i).then(|| at.value(i)),
                choice: opt_str(choice, i),
            });
        }
    }
    // A stable sort keeps the query's order within an offset.
    events.sort_by_key(Event::key);
    Ok(events)
}

fn transform(results: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    // Read everything before touching the state, so an error changes nothing.
    let events = events(results)?;

    let mut contract_id = StringBuilder::new();
    let mut template_name = StringBuilder::new();
    let mut created_offset = Int64Builder::new();
    let mut archived_offset = Int64Builder::new();
    let mut created_at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut archived_at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut lifetime = Float64Builder::new();
    let mut archived_by = StringBuilder::new();

    OPEN.with_borrow_mut(|open_contracts| {
        for event in events {
            match event {
                Event::Created { id, open } => {
                    open_contracts.insert(id, open);
                }
                Event::Archived {
                    id,
                    offset,
                    at,
                    choice,
                } => {
                    let open = open_contracts.remove(&id);
                    let created = open.as_ref().and_then(|o| o.created_at);
                    contract_id.append_value(&id);
                    template_name.append_option(open.as_ref().and_then(|o| o.template.as_deref()));
                    created_offset.append_option(open.as_ref().map(|o| o.offset));
                    archived_offset.append_value(offset);
                    created_at.append_option(created);
                    archived_at.append_option(at);
                    lifetime.append_option(match (created, at) {
                        (Some(c), Some(a)) => Some((a - c) as f64 / 1e6),
                        _ => None,
                    });
                    archived_by.append_option(choice);
                }
            }
        }
    });

    let columns: Vec<ArrayRef> = vec![
        Arc::new(contract_id.finish()),
        Arc::new(template_name.finish()),
        Arc::new(created_offset.finish()),
        Arc::new(archived_offset.finish()),
        Arc::new(created_at.finish()),
        Arc::new(archived_at.finish()),
        Arc::new(lifetime.finish()),
        Arc::new(archived_by.finish()),
    ];
    ipc(&RecordBatch::try_new(output_schema(), columns).map_err(|e| e.to_string())?)
}

fn panel(
    title: &str,
    kind: PanelKind,
    sql: &str,
    time_column: Option<&str>,
    formats: &[(&str, ValueFormat)],
) -> Panel {
    Panel {
        title: title.into(),
        kind,
        sql: sql.into(),
        time_column: time_column.map(Into::into),
        formats: formats
            .iter()
            .map(|(c, f)| ColumnFormat {
                column: (*c).into(),
                format: *f,
            })
            .collect(),
    }
}

impl Guest for Plugin {
    fn info() -> PluginInfo {
        PluginInfo {
            name: "contract-lifetimes".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            description: "How long each Canton contract lived, from its create to its archive."
                .into(),
        }
    }

    fn schema() -> Vec<u8> {
        ipc(&RecordBatch::new_empty(output_schema())).expect("the schema encodes")
    }

    fn source() -> QuerySource {
        QuerySource {
            query: QUERY.into(),
            inputs: vec![
                ipc(&RecordBatch::new_empty(created_input())).expect("encodes"),
                ipc(&RecordBatch::new_empty(exercised_input())).expect("encodes"),
            ],
            high_water: HIGH_WATER.into(),
        }
    }

    fn mode() -> WriteMode {
        WriteMode::Append
    }

    fn dashboard() -> Vec<Panel> {
        use ValueFormat::{DurationSeconds, Id};
        vec![
            panel(
                "Totals",
                PanelKind::Stats,
                "SELECT count(*) AS \"Archived\", avg(lifetime_seconds) AS \"Mean lifetime\", \
                 median(lifetime_seconds) AS \"Median lifetime\", max(archived_offset) AS \"Newest offset\" \
                 FROM contract_lifetimes",
                None,
                &[
                    ("Mean lifetime", DurationSeconds),
                    ("Median lifetime", DurationSeconds),
                ],
            ),
            panel(
                "Archives over time",
                PanelKind::Timeseries,
                "SELECT date_bin({bucket}, archived_at) AS time, count(*) AS archives FROM contract_lifetimes \
                 WHERE archived_at IS NOT NULL GROUP BY 1 ORDER BY 1",
                Some("archived_at"),
                &[],
            ),
            panel(
                "Mean lifetime per template",
                PanelKind::Bar,
                "SELECT concat(coalesce(template_name, '(created before the data)'), ' (', count(*), ')') AS template, \
                 avg(lifetime_seconds) AS mean_lifetime FROM contract_lifetimes \
                 GROUP BY template_name ORDER BY count(*) DESC LIMIT 15",
                None,
                &[("mean_lifetime", DurationSeconds)],
            ),
            panel(
                "Longest-lived contracts",
                PanelKind::Table,
                "SELECT contract_id, template_name, created_offset, archived_offset, lifetime_seconds, archived_by \
                 FROM contract_lifetimes ORDER BY lifetime_seconds DESC NULLS LAST LIMIT 500",
                None,
                &[("contract_id", Id), ("lifetime_seconds", DurationSeconds)],
            ),
        ]
    }

    fn load_state(state: Vec<u8>) -> Result<(), String> {
        let open: HashMap<String, Open> =
            serde_json::from_slice(&state).map_err(|e| format!("bad state: {e}"))?;
        OPEN.set(open);
        Ok(())
    }

    fn save_state() -> Vec<u8> {
        // Sorted, so the same state always gives the same bytes.
        OPEN.with_borrow(|open| {
            let sorted: std::collections::BTreeMap<&String, &Open> = open.iter().collect();
            serde_json::to_vec(&sorted).expect("the state encodes")
        })
    }

    fn transform(results: Vec<Vec<u8>>) -> Result<Vec<u8>, String> {
        transform(&results)
    }
}
