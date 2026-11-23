//! The tables in a location, for the explorer's table picker.

use anyhow::{Result, anyhow};
use deltalake::logstore::object_store::ObjectStore;
use deltalake::logstore::object_store::path::Path as StorePath;
use futures_util::StreamExt;

use crate::location::Location;
use crate::manifest::Manifest;
use crate::status::Status;

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// The folder name.
    pub name: String,
    pub location: Location,
    /// `None` for a Delta table the runner didn't write.
    pub manifest: Option<Manifest>,
    pub status: Option<Status>,
}

/// The Delta tables directly in `root`, by name; `root` itself counts too,
/// if it is a table. A root that doesn't exist yet gives an empty list.
pub async fn list(root: &Location) -> Result<Vec<Entry>> {
    let store = root.store()?;
    let mut entries = Vec::new();
    if is_table(store.as_ref(), None).await {
        entries.push(entry(root.clone()).await);
    }
    let listing = match store.list_with_delimiter(None).await {
        Ok(listing) => listing,
        Err(deltalake::logstore::object_store::Error::NotFound { .. }) => return Ok(entries),
        Err(e) => return Err(anyhow!("could not list {root}: {e}")),
    };
    for prefix in listing.common_prefixes {
        let Some(name) = prefix.filename().map(str::to_string) else {
            continue;
        };
        if name.starts_with(['_', '.']) {
            continue;
        }
        if is_table(store.as_ref(), Some(&prefix)).await {
            entries.push(entry(root.join(&name)).await);
        }
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

async fn is_table(store: &dyn ObjectStore, prefix: Option<&StorePath>) -> bool {
    let log = match prefix {
        Some(p) => p.clone().join("_delta_log"),
        None => StorePath::from("_delta_log"),
    };
    matches!(store.list(Some(&log)).next().await, Some(Ok(_)))
}

async fn entry(location: Location) -> Entry {
    let store = location.store().ok();
    let (manifest, status) = match &store {
        Some(s) => (
            Manifest::read(s.as_ref()).await.ok().flatten(),
            Status::read(s.as_ref()).await.ok().flatten(),
        ),
        None => (None, None),
    };
    Entry {
        name: location.name(),
        location,
        manifest,
        status,
    }
}
