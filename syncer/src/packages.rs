//! Looks up the name and version of each package the synced contracts come
//! from, and writes them to the `package` table.
//!
//! Contracts link to `package:<package_id>` from `template.package`, so a query
//! can read `template.package.package_version`. The lookup uses the Ledger API
//! (see [`client::packages::PackageClient`]), so no DARs or generated code are
//! needed. A failed lookup is logged and never stops the sync.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use client::jwt::TokenManager;
use client::packages::{PackageClient, PackageInfo};
use tracing::{debug, warn};

use crate::model::{WriteBatch, package_id, package_row};

/// How long to wait before asking again about a package the ledger didn't
/// know (or whose lookup failed).
const RETRY_MISSING_AFTER: Duration = Duration::from_secs(600);

/// Remembers which packages are written, and asks the ledger about new ones.
///
/// Call [`Self::add_package_rows`] before writing a batch and
/// [`Self::mark_written`] once the write has committed. Until then the rows
/// stay pending, so a batch that fails and is replayed gets them again.
pub struct PackageResolver {
    client: PackageClient,
    tokens: Arc<TokenManager>,
    /// Package ids whose row has committed in this run.
    written: HashSet<String>,
    /// Looked up but not committed yet.
    resolved: HashMap<String, PackageInfo>,
    /// Package ids the ledger didn't know (or whose lookup failed), and when
    /// to ask again. They are asked about again even if no later batch
    /// refers to them.
    retry_at: HashMap<String, Instant>,
}

impl PackageResolver {
    pub fn new(ledger_url: String, tokens: Arc<TokenManager>) -> Self {
        Self {
            client: PackageClient::new(ledger_url),
            tokens,
            written: HashSet::new(),
            resolved: HashMap::new(),
            retry_at: HashMap::new(),
        }
    }

    /// Adds a `package` row to `batch` for every package it references, or
    /// that is due for a retry, and that isn't written yet. The rows commit in
    /// the same transaction as the batch.
    pub async fn add_package_rows(&mut self, batch: &mut WriteBatch) {
        let now = Instant::now();
        let mut wanted = pending_ids(&batch.package_refs, &self.written);
        wanted.extend(
            self.retry_at
                .iter()
                .filter(|(_, at)| now >= **at)
                .map(|(id, _)| id.clone()),
        );

        let to_look_up: Vec<String> = wanted
            .iter()
            .filter(|id| !self.resolved.contains_key(*id))
            .filter(|id| self.retry_at.get(*id).is_none_or(|at| now >= *at))
            .cloned()
            .collect();
        if !to_look_up.is_empty() {
            self.look_up(&to_look_up, now).await;
        }

        for id in &wanted {
            if let Some(info) = self.resolved.get(id) {
                batch.packages.push(package_row(info));
            }
        }
    }

    async fn look_up(&mut self, ids: &[String], now: Instant) {
        let result = async {
            let token = self.tokens.get_token().await?;
            self.client.lookup(Some(&token), ids).await
        }
        .await;
        match result {
            Ok(found) => {
                for info in found {
                    self.retry_at.remove(&info.package_id);
                    self.resolved.insert(info.package_id.clone(), info);
                }
                for id in ids.iter().filter(|id| !self.resolved.contains_key(*id)) {
                    debug!(package_id = %id, "Ledger doesn't know the package; will ask again later");
                    self.retry_at.insert(id.clone(), now + RETRY_MISSING_AFTER);
                }
            }
            Err(e) => {
                warn!("Could not look up package names and versions: {e:#}");
                for id in ids {
                    self.retry_at.insert(id.clone(), now + RETRY_MISSING_AFTER);
                }
            }
        }
    }

    /// Records that `batch` has committed, so its package rows aren't
    /// written again.
    pub fn mark_written(&mut self, batch: &WriteBatch) {
        let committed: Vec<String> = self
            .resolved
            .keys()
            .filter(|id| batch.packages.iter().any(|r| r.id == package_id(id)))
            .cloned()
            .collect();
        for id in committed {
            self.resolved.remove(&id);
            self.written.insert(id);
        }
    }
}

/// The referenced package ids that aren't written yet. Package-name
/// references (`#name`, as used in template filters) are skipped: they aren't
/// package ids.
pub fn pending_ids(refs: &BTreeSet<String>, written: &HashSet<String>) -> BTreeSet<String> {
    refs.iter()
        .filter(|id| !id.is_empty() && !id.starts_with('#'))
        .filter(|id| !written.contains(*id))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    /// A resolver whose ledger can't be reached, so every lookup fails.
    fn offline_resolver() -> PackageResolver {
        PackageResolver::new(
            "http://127.0.0.1:1".into(),
            Arc::new(TokenManager::new(client::jwt::TokenSource::Static(
                "t".into(),
            ))),
        )
    }

    fn info(id: &str) -> PackageInfo {
        PackageInfo {
            package_id: id.into(),
            package_name: "my-assets".into(),
            package_version: "1.0.0".into(),
        }
    }

    fn batch_referring_to(ids: &[&str]) -> WriteBatch {
        WriteBatch {
            package_refs: set(ids),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn rows_come_back_until_the_write_commits() {
        let mut r = offline_resolver();
        r.resolved.insert("p1".into(), info("p1"));

        // The first write fails: the replayed batch gets the row again,
        // without asking the ledger.
        let mut first = batch_referring_to(&["p1"]);
        r.add_package_rows(&mut first).await;
        assert_eq!(first.packages.len(), 1);
        let mut replay = batch_referring_to(&["p1"]);
        r.add_package_rows(&mut replay).await;
        assert_eq!(replay.packages.len(), 1);
        assert!(r.retry_at.is_empty());

        // Once it commits, later batches don't carry it.
        r.mark_written(&replay);
        let mut later = batch_referring_to(&["p1"]);
        r.add_package_rows(&mut later).await;
        assert!(later.packages.is_empty());
        assert!(r.written.contains("p1"));
    }

    #[tokio::test]
    async fn failed_lookups_are_retried_without_a_new_reference() {
        let mut r = offline_resolver();
        let mut batch = batch_referring_to(&["p2"]);
        r.add_package_rows(&mut batch).await;
        assert!(batch.packages.is_empty());
        let first_retry = r.retry_at["p2"];
        assert!(first_retry > Instant::now());

        // Not due yet: nothing is asked.
        r.add_package_rows(&mut WriteBatch::default()).await;
        assert_eq!(r.retry_at["p2"], first_retry);

        // Due: asked again even though this batch doesn't refer to it.
        r.retry_at.insert("p2".into(), Instant::now());
        r.add_package_rows(&mut WriteBatch::default()).await;
        assert!(r.retry_at["p2"] > first_retry);
    }

    #[test]
    fn skips_written_and_name_references() {
        let written: HashSet<String> = ["a".to_string()].into();
        assert_eq!(
            pending_ids(&set(&["a", "b", "#pkg", ""]), &written),
            set(&["b"])
        );
    }
}
