//! The Delta Lake table a plugin writes to.
//!
//! Each commit holds the new Parquet files and an application transaction
//! `(plugin name, cursor)`, so the rows and the cursor always move together:
//! if the runner stops halfway, the next run starts from the last commit.
//!
//! The plugin's state is saved now and then, not in every commit: as a
//! compressed file in `_canton_analytics/state/`, named by its cursor, which
//! the commit's info points to. On start, the runner reads the newest saved
//! state and replays the cursor range after it to rebuild the rest (see
//! `runner.rs`). So a large state costs a file write every few minutes
//! instead of a log entry per commit.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use analytics_reader::location::{Location, read_file, write_file};
use analytics_reader::manifest::{self, Manifest, Mode};
use analytics_reader::status::Status;
use anyhow::{Context, Result, anyhow};
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::array::UInt32Array;
use deltalake::arrow::compute::{concat_batches, take_record_batch};
use deltalake::arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use deltalake::arrow::row::{RowConverter, SortField};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{StructType, Transaction};
use deltalake::logstore::object_store::path::Path as StorePath;
use deltalake::logstore::object_store::{ObjectStore, ObjectStoreExt};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DeltaTable, TableProperty};
use futures_util::StreamExt;
use tracing::{debug, info};

/// Key of the runner's entry in each commit's info.
const INFO_KEY: &str = "canton_analytics";
/// Parquet files the optimizer aims for.
const TARGET_FILE_SIZE: u64 = 128 * 1024 * 1024;

/// Where the table stands: the cursor of the last commit, and the newest
/// saved state with the cursor it belongs to.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Saved {
    pub cursor: i64,
    pub state_cursor: i64,
    pub state: Vec<u8>,
}

pub struct Table {
    pub table: DeltaTable,
    pub location: Location,
    /// The table's own store, for the runner's files next to it.
    store: Arc<dyn ObjectStore>,
    app_id: String,
    pub mode: Mode,
    schema: SchemaRef,
    /// Cursors of the saved states that must be kept: the newest one, and
    /// the one before in case the newest commit didn't land.
    kept_states: Vec<i64>,
    /// Small files that compaction would merge.
    pub small_files: usize,
}

impl Table {
    /// Opens the table at `path`, or creates it with the plugin's schema.
    /// Writes the plugin's manifest next to it either way.
    pub async fn open_or_create(
        location: &Location,
        manifest: &Manifest,
        schema: &SchemaRef,
    ) -> Result<Table> {
        let table = location.table().await?;
        let table = if table.version().is_some() {
            check_schema(&table, schema, &manifest.name)?;
            table
        } else {
            let columns: StructType = schema.as_ref().try_into_kernel()?;
            table
                .create()
                .with_table_name(&manifest.name)
                .with_comment(format!(
                    "Written by the canton-analytics plugin {}",
                    manifest.name
                ))
                .with_columns(columns.fields().cloned())
                // Readers load a log checkpoint plus a few commits, not the
                // whole log.
                .with_configuration_property(TableProperty::CheckpointInterval, Some("20"))
                // Files replaced by optimize are kept an hour for readers
                // still on an older version, then vacuum removes them.
                .with_configuration_property(
                    TableProperty::DeletedFileRetentionDuration,
                    Some("interval 1 hours"),
                )
                .await
                .with_context(|| format!("could not create the Delta table at {location}"))?
        };
        let store = location.store()?;
        manifest.write(store.as_ref()).await?;
        // Count the small files already there, so compaction keeps its pace
        // across restarts. One small file is expected: the newest data.
        let small_files = table
            .snapshot()
            .map(|s| {
                s.log_data()
                    .into_iter()
                    .filter(|f| (f.size() as u64) < TARGET_FILE_SIZE / 2)
                    .count()
            })
            .unwrap_or(0)
            .saturating_sub(1);
        Ok(Table {
            table,
            location: location.clone(),
            store,
            app_id: manifest.name.clone(),
            mode: manifest.mode.clone(),
            schema: schema.clone(),
            kept_states: Vec::new(),
            small_files,
        })
    }

    fn state_file(cursor: i64) -> String {
        format!("{}/state/{cursor:020}.zst", manifest::DIR)
    }

    pub async fn write_status(&self, status: &Status) -> Result<()> {
        status.write(self.store.as_ref()).await
    }

    /// The cursor of the last commit and the newest saved state.
    pub async fn saved(&mut self) -> Result<Saved> {
        let snapshot = self.table.snapshot()?;
        let Some(cursor) = snapshot
            .transaction_version(&self.table.log_store(), &self.app_id)
            .await?
        else {
            return Ok(Saved::default());
        };
        // Look back through the newest commits for the one that saved a
        // state; states are saved every few commits, so this is short.
        let mut limit = 64;
        loop {
            let commits: Vec<_> = {
                let mut history = self.table.history(Some(limit));
                let mut commits = Vec::new();
                while let Some(c) = history.next().await {
                    commits.push(c?);
                }
                commits
            };
            for commit in &commits {
                let Some(info) = commit.info.get(INFO_KEY) else {
                    continue;
                };
                if let Some(state_cursor) = info.get("state").and_then(|s| s.as_i64()) {
                    let state = self.read_state(state_cursor).await?;
                    self.kept_states = vec![state_cursor];
                    return Ok(Saved {
                        cursor,
                        state_cursor,
                        state,
                    });
                }
            }
            if commits.len() < limit {
                // The whole log: no state was saved yet, so replay from 0.
                return Ok(Saved {
                    cursor,
                    state_cursor: 0,
                    state: Vec::new(),
                });
            }
            limit *= 4;
        }
    }

    /// The state saved at `cursor`.
    pub async fn read_state(&self, cursor: i64) -> Result<Vec<u8>> {
        let file = Table::state_file(cursor);
        let bytes = read_file(self.store.as_ref(), &file)
            .await?
            .ok_or_else(|| anyhow!("{file} is missing"))?;
        zstd::decode_all(bytes.as_slice()).with_context(|| format!("{file} is damaged"))
    }

    /// Writes `batches` and moves the cursor to `cursor`, in one commit. With
    /// `state`, also saves the plugin's state as of `cursor`.
    pub async fn commit(
        &mut self,
        batches: Vec<RecordBatch>,
        cursor: i64,
        state: Option<&[u8]>,
    ) -> Result<usize> {
        let mut info = serde_json::json!({ "cursor": cursor });
        if let Some(state) = state {
            // Object stores replace a file whole, so a reader never sees half.
            write_file(
                self.store.as_ref(),
                &Table::state_file(cursor),
                zstd::encode_all(state, 3)?,
            )
            .await?;
            info["state"] = cursor.into();
        }
        let properties = CommitProperties::default()
            .with_application_transaction(Transaction::new(&self.app_id, cursor))
            .with_metadata([(INFO_KEY.to_string(), info)]);
        let batches: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        match (&self.mode, batches.is_empty()) {
            (Mode::Merge(keys), false) => {
                let keys = keys.clone();
                self.merge(batches, &keys, properties).await?;
            }
            _ => {
                let mut writer =
                    RecordBatchWriter::for_table(&self.table)?.with_commit_properties(properties);
                for batch in batches {
                    writer.write(batch).await?;
                    self.small_files += 1;
                }
                writer.flush_and_commit(&mut self.table).await?;
            }
        }
        if state.is_some() {
            self.kept_states.push(cursor);
            self.drop_old_states().await?;
        }
        Ok(rows)
    }

    /// Upserts on `keys`. Within the new rows, the last row for a key wins.
    async fn merge(
        &mut self,
        batches: Vec<RecordBatch>,
        keys: &[String],
        properties: CommitProperties,
    ) -> Result<()> {
        let batch = last_per_key(&self.schema, &batches, keys)?;
        let ctx = SessionContext::new();
        let source = ctx.read_batch(batch)?;
        let on = keys
            .iter()
            .map(|k| format!("target.{k} = source.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let columns: Vec<String> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let update_columns = columns.clone();
        let (table, metrics) = self
            .table
            .clone()
            .merge(source, on)
            .with_source_alias("source")
            .with_target_alias("target")
            .with_commit_properties(properties)
            .when_matched_update(|u| {
                update_columns
                    .iter()
                    .fold(u, |u, c| u.update(c.as_str(), format!("source.{c}")))
            })?
            .when_not_matched_insert(|i| {
                columns
                    .iter()
                    .fold(i, |i, c| i.set(c.as_str(), format!("source.{c}")))
            })?
            .await?;
        // A merge rewrites the files it touches: count what it adds net.
        self.small_files = (self.small_files + metrics.num_target_files_added)
            .saturating_sub(metrics.num_target_files_removed);
        debug!(
            inserted = metrics.num_target_rows_inserted,
            updated = metrics.num_target_rows_updated,
            "merged"
        );
        self.table = table;
        Ok(())
    }

    async fn drop_old_states(&mut self) -> Result<()> {
        // Keep the two newest: the newest commit may not have landed.
        self.kept_states.sort_unstable();
        let keep_from = self
            .kept_states
            .iter()
            .rev()
            .nth(1)
            .copied()
            .unwrap_or(i64::MIN);
        self.kept_states.retain(|c| *c >= keep_from);
        let dir = StorePath::from(format!("{}/state", manifest::DIR));
        let files: Vec<_> = self.store.list(Some(&dir)).collect().await;
        for file in files.into_iter().flatten() {
            let cursor = file
                .location
                .filename()
                .and_then(|n| n.strip_suffix(".zst"))
                .and_then(|n| n.parse::<i64>().ok());
            if matches!(cursor, Some(c) if c < keep_from) {
                let _ = self.store.delete(&file.location).await;
            }
        }
        Ok(())
    }

    /// Rewrites the small Parquet files into a few large ones.
    pub async fn optimize(&mut self) -> Result<()> {
        let (table, metrics) = self
            .table
            .clone()
            .optimize()
            .with_target_size(NonZeroU64::new(TARGET_FILE_SIZE).expect("not zero"))
            .await?;
        self.table = table;
        self.small_files = 0;
        info!(
            removed = metrics.num_files_removed,
            added = metrics.num_files_added,
            "compacted"
        );
        Ok(())
    }

    /// Deletes the files no version newer than `retention` uses.
    pub async fn vacuum(&mut self, retention: Duration) -> Result<()> {
        let retention = chrono::Duration::from_std(retention).map_err(|e| anyhow!("{e}"))?;
        let (table, metrics) = self
            .table
            .clone()
            .vacuum()
            .with_retention_period(retention)
            .with_enforce_retention_duration(false)
            .await?;
        self.table = table;
        if !metrics.files_deleted.is_empty() {
            info!(deleted = metrics.files_deleted.len(), "vacuumed");
        }
        Ok(())
    }
}

/// The table must still have the plugin's columns: a plugin version that
/// changes them needs a new table.
fn check_schema(table: &DeltaTable, schema: &ArrowSchema, plugin: &str) -> Result<()> {
    use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
    let existing: ArrowSchema = table.snapshot()?.schema().as_ref().try_into_arrow()?;
    let names = |s: &ArrowSchema| {
        s.fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    };
    if names(&existing) != names(schema) {
        return Err(anyhow!(
            "the plugin {plugin} now writes the columns {:?}, but its table has {:?}; \
             move the old table away to start a new one",
            names(schema),
            names(&existing)
        ));
    }
    Ok(())
}

/// The rows of `batches`, keeping only the last row for each key.
fn last_per_key(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    keys: &[String],
) -> Result<RecordBatch> {
    let batch = concat_batches(schema, batches)?;
    let key_columns: Vec<_> = keys
        .iter()
        .map(|k| Ok(batch.column(schema.index_of(k)?).clone()))
        .collect::<Result<_>>()?;
    let converter = RowConverter::new(
        key_columns
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(&key_columns)?;
    let mut seen = std::collections::HashSet::new();
    let mut keep: Vec<u32> = Vec::new();
    for i in (0..batch.num_rows()).rev() {
        if seen.insert(rows.row(i)) {
            keep.push(i as u32);
        }
    }
    keep.reverse();
    if keep.len() == batch.num_rows() {
        return Ok(batch);
    }
    Ok(take_record_batch(&batch, &UInt32Array::from(keep))?)
}

#[cfg(test)]
mod tests {
    use deltalake::arrow::array::{AsArray, Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Int64Type};

    use super::*;

    #[test]
    fn keeps_the_last_row_per_key() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]));
        let batch = |k: Vec<&str>, n: Vec<i64>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(k)),
                    Arc::new(Int64Array::from(n)),
                ],
            )
            .unwrap()
        };
        let out = last_per_key(
            &schema,
            &[batch(vec!["a", "b"], vec![1, 2]), batch(vec!["a"], vec![3])],
            &["k".into()],
        )
        .unwrap();
        assert_eq!(out.num_rows(), 2);
        let n: Vec<i64> = out.column(1).as_primitive::<Int64Type>().values().to_vec();
        assert_eq!(n, vec![2, 3]);
    }
}
