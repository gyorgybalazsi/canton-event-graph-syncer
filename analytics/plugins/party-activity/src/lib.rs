//! How active each Canton party is: one row per party, with how many
//! contracts it signed, how many choices it exercised, and when it was first
//! and last active.
//!
//! The table is written in merge mode, keyed on `party`: each step writes a
//! fresh row for every party that was active in it, and the runner replaces
//! that party's old row. The running totals stay in the plugin's memory
//! between steps and in its saved state across restarts.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
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

/// Who signed each create and who acted in each choice, with an offset in
/// `($from, $to]`.
const QUERY: &str = "\
SELECT signatories, offset, created_at FROM created \
    WHERE offset > $from AND offset <= $to;
SELECT acting_parties, offset, transaction_effective_at FROM exercised \
    WHERE offset > $from AND offset <= $to;";

/// Same as contract-lifetimes: the syncer's own mark, else the newest offset.
const HIGH_WATER: &str = "\
LET $synced = IF (INFO FOR DB).tables.sync_state != NONE { \
    (SELECT VALUE last_offset FROM ONLY sync_state:stream) } ELSE { NONE };
RETURN $synced ?? math::max(SELECT VALUE offset FROM transaction) ?? 0;";

/// One party's running totals.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
struct Totals {
    signed: i64,
    exercised: i64,
    first_offset: Option<i64>,
    last_offset: Option<i64>,
    first_at: Option<i64>,
    last_at: Option<i64>,
}

impl Totals {
    fn saw(&mut self, offset: i64, at: Option<i64>) {
        self.first_offset = Some(self.first_offset.map_or(offset, |o| o.min(offset)));
        self.last_offset = Some(self.last_offset.map_or(offset, |o| o.max(offset)));
        if let Some(at) = at {
            self.first_at = Some(self.first_at.map_or(at, |t| t.min(at)));
            self.last_at = Some(self.last_at.map_or(at, |t| t.max(at)));
        }
    }
}

thread_local! {
    static PARTIES: RefCell<HashMap<String, Totals>> = RefCell::new(HashMap::new());
}

fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn parties() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("party", DataType::Utf8, false),
        Field::new("contracts_signed", DataType::Int64, false),
        Field::new("choices_exercised", DataType::Int64, false),
        Field::new("first_offset", DataType::Int64, true),
        Field::new("last_offset", DataType::Int64, true),
        Field::new("first_active_at", timestamp(), true),
        Field::new("last_active_at", timestamp(), true),
    ]))
}

fn input(time: &str) -> SchemaRef {
    let who = if time == "created_at" {
        "signatories"
    } else {
        "acting_parties"
    };
    Arc::new(Schema::new(vec![
        Field::new(who, parties(), true),
        Field::new("offset", DataType::Int64, true),
        Field::new(time, timestamp(), true),
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

/// One party taking part: (party, offset, time, signed a create?).
type Act = (String, i64, Option<i64>, bool);

fn acts(bytes: &[u8], what: &str, signed: bool, out: &mut Vec<Act>) -> Result<(), String> {
    for b in read(bytes, what)? {
        let (who, offset, at) = (
            b.column(0).as_list::<i32>(),
            b.column(1).as_primitive::<Int64Type>(),
            b.column(2).as_primitive::<TimestampMicrosecondType>(),
        );
        for i in 0..b.num_rows() {
            if who.is_null(i) || offset.is_null(i) {
                continue;
            }
            let list = who.value(i);
            let names = list.as_string::<i32>();
            for j in 0..names.len() {
                if names.is_valid(j) {
                    out.push((
                        names.value(j).to_string(),
                        offset.value(i),
                        at.is_valid(i).then(|| at.value(i)),
                        signed,
                    ));
                }
            }
        }
    }
    Ok(())
}

fn transform(results: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let [created, exercised] = results else {
        return Err(format!("expected 2 query results, got {}", results.len()));
    };
    // Read everything before touching the state, so an error changes nothing.
    let mut all = Vec::new();
    acts(created, "created", true, &mut all)?;
    acts(exercised, "exercised", false, &mut all)?;

    // The parties active in this step, sorted so the output is deterministic.
    let touched: BTreeMap<String, Totals> = PARTIES.with_borrow_mut(|parties| {
        let mut touched = BTreeMap::new();
        for (party, offset, at, signed) in all {
            let totals = parties.entry(party.clone()).or_default();
            if signed {
                totals.signed += 1;
            } else {
                totals.exercised += 1;
            }
            totals.saw(offset, at);
            touched.insert(party, totals.clone());
        }
        touched
    });

    let mut party = StringBuilder::new();
    let mut signed = Int64Builder::new();
    let mut exercised = Int64Builder::new();
    let mut first_offset = Int64Builder::new();
    let mut last_offset = Int64Builder::new();
    let mut first_at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut last_at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    for (name, t) in &touched {
        party.append_value(name);
        signed.append_value(t.signed);
        exercised.append_value(t.exercised);
        first_offset.append_option(t.first_offset);
        last_offset.append_option(t.last_offset);
        first_at.append_option(t.first_at);
        last_at.append_option(t.last_at);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(party.finish()),
        Arc::new(signed.finish()),
        Arc::new(exercised.finish()),
        Arc::new(first_offset.finish()),
        Arc::new(last_offset.finish()),
        Arc::new(first_at.finish()),
        Arc::new(last_at.finish()),
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
            name: "party-activity".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            description: "How active each Canton party is: contracts signed, choices exercised, first and last activity.".into(),
        }
    }

    fn schema() -> Vec<u8> {
        ipc(&RecordBatch::new_empty(output_schema())).expect("the schema encodes")
    }

    fn source() -> QuerySource {
        QuerySource {
            query: QUERY.into(),
            inputs: vec![
                ipc(&RecordBatch::new_empty(input("created_at"))).expect("encodes"),
                ipc(&RecordBatch::new_empty(input("transaction_effective_at"))).expect("encodes"),
            ],
            high_water: HIGH_WATER.into(),
        }
    }

    fn mode() -> WriteMode {
        WriteMode::Merge(vec!["party".into()])
    }

    fn dashboard() -> Vec<Panel> {
        use ValueFormat::{Id, Timestamp};
        vec![
            panel(
                "Totals",
                PanelKind::Stats,
                "SELECT count(*) AS \"Parties\", sum(contracts_signed) AS \"Contracts signed\", \
                 sum(choices_exercised) AS \"Choices exercised\", max(last_active_at) AS \"Last activity\" \
                 FROM party_activity",
                None,
                &[("Last activity", Timestamp)],
            ),
            panel(
                "Parties by last activity",
                PanelKind::Timeseries,
                "SELECT date_bin({bucket}, last_active_at) AS time, count(*) AS parties FROM party_activity \
                 WHERE last_active_at IS NOT NULL GROUP BY 1 ORDER BY 1",
                Some("last_active_at"),
                &[],
            ),
            panel(
                "Most active parties",
                PanelKind::Bar,
                "SELECT party, contracts_signed + choices_exercised AS actions FROM party_activity \
                 ORDER BY actions DESC LIMIT 15",
                None,
                &[("party", Id)],
            ),
            panel(
                "All parties",
                PanelKind::Table,
                "SELECT party, contracts_signed, choices_exercised, first_offset, last_offset, \
                 first_active_at, last_active_at FROM party_activity ORDER BY last_offset DESC NULLS LAST",
                None,
                &[
                    ("party", Id),
                    ("first_active_at", Timestamp),
                    ("last_active_at", Timestamp),
                ],
            ),
        ]
    }

    fn load_state(state: Vec<u8>) -> Result<(), String> {
        let parties: HashMap<String, Totals> =
            serde_json::from_slice(&state).map_err(|e| format!("bad state: {e}"))?;
        PARTIES.set(parties);
        Ok(())
    }

    fn save_state() -> Vec<u8> {
        PARTIES.with_borrow(|parties| {
            let sorted: BTreeMap<&String, &Totals> = parties.iter().collect();
            serde_json::to_vec(&sorted).expect("the state encodes")
        })
    }

    fn transform(results: Vec<Vec<u8>>) -> Result<Vec<u8>, String> {
        transform(&results)
    }
}
