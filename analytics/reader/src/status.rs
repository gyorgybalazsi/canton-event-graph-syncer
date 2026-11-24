//! The runner's heartbeat: `_canton_analytics/status.json` in each table,
//! rewritten every few seconds while the runner runs. The explorer shows it,
//! so a stale dashboard can be told apart from a stopped runner.

use anyhow::{Context, Result};
use deltalake::logstore::object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::location::{read_file, write_file};
use crate::manifest::DIR;

const FILE: &str = "status.json";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Status {
    /// When this was written, in milliseconds since the epoch.
    pub updated_at_ms: i64,
    /// How often the runner rewrites it while running, in milliseconds.
    pub every_ms: i64,
    /// The runner stopped cleanly (it writes this as it exits).
    #[serde(default)]
    pub stopped: bool,
    pub plugin_version: String,
    /// The cursor of the last commit.
    pub committed: i64,
    /// How far the steps have got, including rows not yet committed.
    pub cursor: i64,
    /// The database's high-water mark, when last read.
    pub high_water: i64,
    /// The last error, while the plugin is failing; `None` once it works again.
    #[serde(default)]
    pub error: Option<String>,
    /// Which runner wrote it: host name and process id.
    #[serde(default)]
    pub host: String,
    /// The commit thresholds in force (see [`crate::Tuning`]); 0 in a
    /// heartbeat from an older runner.
    #[serde(default)]
    pub min_rows: u64,
    #[serde(default)]
    pub max_wait_ms: i64,
}

impl Status {
    /// Cursor values not yet in the table.
    pub fn behind(&self) -> i64 {
        (self.high_water - self.committed).max(0)
    }

    /// The runner wrote it within three of its intervals and didn't stop.
    pub fn alive(&self, now_ms: i64) -> bool {
        !self.stopped && now_ms - self.updated_at_ms <= 3 * self.every_ms.max(1_000)
    }

    pub async fn read(store: &dyn ObjectStore) -> Result<Option<Status>> {
        let Some(bytes) = read_file(store, &format!("{DIR}/{FILE}")).await? else {
            return Ok(None);
        };
        Ok(Some(
            serde_json::from_slice(&bytes).with_context(|| format!("bad {DIR}/{FILE}"))?,
        ))
    }

    pub async fn write(&self, store: &dyn ObjectStore) -> Result<()> {
        write_file(store, &format!("{DIR}/{FILE}"), serde_json::to_vec(self)?).await
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
