//! Reads the Delta Lake tables that canton-analytics plugins write, with
//! DataFusion SQL. Used by the runner's `sql` and `dashboard` commands and by
//! the explorer's dashboard; it doesn't depend on wasmtime or SurrealDB.
//!
//! - [`Session`] opens one table and runs SQL over its newest version.
//! - [`dashboard`] runs a plugin's panels (or panels made from the table's
//!   columns) and hands back plain values.
//! - [`catalog`] lists the tables in a folder.
//! - [`manifest`] is what the runner records about the plugin.

pub mod catalog;
pub mod dashboard;
pub mod frame;
pub mod location;
pub mod manifest;
pub mod status;

use std::sync::Arc;

use anyhow::{Result, anyhow};
use deltalake::DeltaTable;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::logstore::object_store::ObjectStore;

pub use location::Location;
pub use manifest::Manifest;
pub use status::Status;

/// One table, registered in a DataFusion session under its SQL name (the
/// plugin name with `-` as `_`, or the folder name). Keep it to refresh
/// cheaply: [`Session::refresh`] reads only the commits since the last one.
pub struct Session {
    pub location: Location,
    pub ctx: SessionContext,
    pub table: DeltaTable,
    /// The table's own store, for the runner's files next to it.
    pub store: Arc<dyn ObjectStore>,
    /// The name the SQL uses for the table.
    pub name: String,
    pub manifest: Option<Manifest>,
}

impl Session {
    pub async fn open(location: &Location) -> Result<Session> {
        let table = location.table().await?;
        if table.version().is_none() {
            return Err(anyhow!(
                "no Delta table at {location} yet; start canton-analytics first"
            ));
        }
        let store = location.store()?;
        let manifest = Manifest::read(store.as_ref()).await?;
        let name = match &manifest {
            Some(m) => m.sql_name(),
            None => manifest::sql_name(&location.name()),
        };
        // Dashboards are small queries over one table: a few partitions are
        // plenty, and keep the explorer responsive.
        let partitions = std::thread::available_parallelism().map_or(4, |n| n.get().min(8));
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(partitions),
        );
        let mut session = Session {
            location: location.clone(),
            ctx,
            table,
            store,
            name,
            manifest,
        };
        session.register().await?;
        Ok(session)
    }

    async fn register(&mut self) -> Result<()> {
        self.table.update_datafusion_session(&self.ctx.state())?;
        let provider = self.table.table_provider().await?;
        let _ = self.ctx.deregister_table(self.name.as_str())?;
        self.ctx.register_table(self.name.as_str(), provider)?;
        Ok(())
    }

    /// The version the session reads.
    pub fn version(&self) -> Option<i64> {
        self.table.version().map(|v| v as i64)
    }

    /// The newest version in the log, without loading it: a listing of the
    /// log, cheap enough to poll.
    pub async fn latest_version(&self) -> Result<i64> {
        Ok(self.table.get_latest_version().await? as i64)
    }

    /// Moves to the newest version, reading only the new commits. Also
    /// rereads the manifest, which changes when a new plugin version runs.
    pub async fn refresh(&mut self) -> Result<Option<i64>> {
        let before = self.version();
        self.table.update_incremental(None).await?;
        if self.version() != before {
            self.manifest = Manifest::read(self.store.as_ref()).await?;
            self.register().await?;
        }
        Ok(self.version())
    }

    /// The runner's heartbeat for this table, if it has written one.
    pub async fn status(&self) -> Result<Option<Status>> {
        Status::read(self.store.as_ref()).await
    }

    pub fn schema(&self) -> Result<SchemaRef> {
        use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
        let schema = self.table.snapshot()?.schema();
        let arrow: deltalake::arrow::datatypes::Schema = schema.as_ref().try_into_arrow()?;
        Ok(Arc::new(arrow))
    }

    /// Runs one SQL statement and collects its rows.
    pub async fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let frame = self.ctx.sql(sql).await.map_err(|e| anyhow!("{e}"))?;
        frame.collect().await.map_err(|e| anyhow!("{e}"))
    }
}

/// Quotes a column name for SQL.
pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
