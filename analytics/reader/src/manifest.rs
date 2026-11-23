//! What the runner records about a plugin next to its table, in
//! `_canton_analytics/plugin.json`: its name and version, how it writes, and
//! its dashboard. The explorer reads it, so it never has to load WebAssembly.
//!
//! Delta Lake ignores folders whose names start with `_`, so this folder
//! doesn't disturb other readers, and `vacuum` leaves it alone.

use anyhow::{Context, Result};
use deltalake::logstore::object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::location::{read_file, write_file};

/// The runner's folder inside a table folder.
pub const DIR: &str = "_canton_analytics";
const FILE: &str = "plugin.json";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub mode: Mode,
    /// Empty: the dashboard is made from the table's columns.
    #[serde(default)]
    pub panels: Vec<Panel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Append,
    /// Upsert on these key columns.
    Merge(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PanelKind {
    Stats,
    Timeseries,
    Bar,
    Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Auto,
    Number,
    DurationSeconds,
    Timestamp,
    Id,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Panel {
    pub title: String,
    pub kind: PanelKind,
    pub sql: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,
    /// Column name → format; columns not listed use `Format::Auto`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub formats: Vec<(String, Format)>,
}

impl Panel {
    pub fn format(&self, column: &str) -> Format {
        self.formats
            .iter()
            .find(|(c, _)| c == column)
            .map(|(_, f)| *f)
            .unwrap_or_default()
    }
}

impl Manifest {
    /// The table name the panels' SQL uses: the plugin name with `-` as `_`.
    pub fn sql_name(&self) -> String {
        sql_name(&self.name)
    }

    /// The manifest of a table, from the table's own store, or `None` if
    /// it has none (a table not written by the runner).
    pub async fn read(store: &dyn ObjectStore) -> Result<Option<Manifest>> {
        let Some(bytes) = read_file(store, &format!("{DIR}/{FILE}")).await? else {
            return Ok(None);
        };
        Ok(Some(
            serde_json::from_slice(&bytes).with_context(|| format!("bad {DIR}/{FILE}"))?,
        ))
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

/// A plugin or folder name as a SQL table name.
pub fn sql_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}
