//! How quickly the runner brings a table up to date:
//! `_canton_analytics/tuning.json` in each table. The explorer's dashboard
//! settings write it; the runner reads it before each check, so a change
//! takes effect within one check interval, without a restart. A field left
//! out keeps the runner's own (command-line) value.
//!
//! A new row reaches the table once the runner has seen it (`every_seconds`)
//! and committed it, which it does when `min_rows` rows are waiting or the
//! oldest has waited `max_wait_seconds`. So on a quiet ledger the lag is at
//! most about `every_seconds + max_wait_seconds`.

use anyhow::{Context, Result};
use deltalake::logstore::object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::location::{Location, read_file, write_file};
use crate::manifest::DIR;

const FILE: &str = "tuning.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Tuning {
    /// Seconds between checks for new data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_seconds: Option<u64>,
    /// Commit once this many rows are waiting…
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows: Option<u64>,
    /// …or once the oldest has waited this many seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wait_seconds: Option<u64>,
}

impl Tuning {
    /// The table's tuning, or `None` when it has none.
    pub async fn read(store: &dyn ObjectStore) -> Result<Option<Tuning>> {
        let Some(bytes) = read_file(store, &format!("{DIR}/{FILE}")).await? else {
            return Ok(None);
        };
        Ok(Some(
            serde_json::from_slice(&bytes).with_context(|| format!("bad {DIR}/{FILE}"))?,
        ))
    }

    /// The tuning of the table at `location`.
    pub async fn read_at(location: &Location) -> Result<Option<Tuning>> {
        Tuning::read(location.store()?.as_ref()).await
    }

    /// Sets the tuning of the table at `location`.
    pub async fn write_at(&self, location: &Location) -> Result<()> {
        self.write(location.store()?.as_ref()).await
    }

    pub async fn write(&self, store: &dyn ObjectStore) -> Result<()> {
        write_file(
            store,
            &format!("{DIR}/{FILE}"),
            serde_json::to_vec_pretty(self)?,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::Tuning;

    #[test]
    fn left_out_fields_keep_the_runners_values() {
        let t: Tuning = serde_json::from_str(r#"{"max_wait_seconds": 2}"#).unwrap();
        assert_eq!(
            t,
            Tuning {
                max_wait_seconds: Some(2),
                ..Tuning::default()
            }
        );
        assert_eq!(serde_json::to_string(&Tuning::default()).unwrap(), "{}");
    }
}
