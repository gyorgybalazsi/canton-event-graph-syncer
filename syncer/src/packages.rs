//! Looks up the name and version of each package the synced contracts come
//! from, and writes them to the `package` table.
//!
//! Contracts link to `package:<package_id>` from `template.package`, so a query
//! can read `template.package.package_version`. The lookup uses the Ledger API
//! (`PackageService.ListVettedPackages`), so no DARs or generated code are
//! needed. A failed lookup is logged and never stops the sync.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use client::jwt::TokenManager;
use client::packages::list_vetted_packages;
use tracing::{debug, warn};

use crate::model::{WriteBatch, package_row};

/// How long to wait before asking again about a package the ledger didn't
/// list (or whose lookup failed).
const RETRY_MISSING_AFTER: Duration = Duration::from_secs(600);

/// Remembers which packages are already written, and asks the ledger about
/// new ones.
pub struct PackageResolver {
    ledger_url: String,
    tokens: Arc<TokenManager>,
    /// Package ids already written in this run.
    known: HashSet<String>,
    /// Package ids the ledger didn't list, and when to ask again.
    retry_at: HashMap<String, Instant>,
}

impl PackageResolver {
    pub fn new(ledger_url: String, tokens: Arc<TokenManager>) -> Self {
        Self {
            ledger_url,
            tokens,
            known: HashSet::new(),
            retry_at: HashMap::new(),
        }
    }

    /// Adds a `package` row to `batch` for every package it references that
    /// isn't written yet, so the rows commit in the same transaction.
    pub async fn add_package_rows(&mut self, batch: &mut WriteBatch) {
        let now = Instant::now();
        let ids = ids_to_look_up(&batch.package_refs, &self.known, &self.retry_at, now);
        if ids.is_empty() {
            return;
        }
        let result = async {
            let token = self.tokens.get_token().await?;
            list_vetted_packages(&self.ledger_url, Some(&token), &ids).await
        }
        .await;
        match result {
            Ok(found) => {
                for info in &found {
                    batch.packages.push(package_row(info));
                    self.known.insert(info.package_id.clone());
                    self.retry_at.remove(&info.package_id);
                }
                for id in ids.iter().filter(|id| !self.known.contains(*id)) {
                    debug!(package_id = %id, "Package not listed as vetted; will ask again later");
                    self.retry_at.insert(id.clone(), now + RETRY_MISSING_AFTER);
                }
            }
            Err(e) => {
                warn!("Could not look up package names and versions: {e:#}");
                for id in ids {
                    self.retry_at.insert(id, now + RETRY_MISSING_AFTER);
                }
            }
        }
    }
}

/// The referenced package ids that are neither written nor waiting for a
/// retry. Package-name references (`#name`, as used in template filters) are
/// skipped: they aren't package ids.
pub fn ids_to_look_up(
    refs: &BTreeSet<String>,
    known: &HashSet<String>,
    retry_at: &HashMap<String, Instant>,
    now: Instant,
) -> Vec<String> {
    refs.iter()
        .filter(|id| !id.is_empty() && !id.starts_with('#'))
        .filter(|id| !known.contains(*id))
        .filter(|id| retry_at.get(*id).is_none_or(|t| now >= *t))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn skips_known_and_name_references() {
        let known: HashSet<String> = ["a".to_string()].into();
        let ids = ids_to_look_up(
            &set(&["a", "b", "#pkg", ""]),
            &known,
            &HashMap::new(),
            Instant::now(),
        );
        assert_eq!(ids, vec!["b".to_string()]);
    }

    #[test]
    fn waits_for_retry_time() {
        let now = Instant::now();
        let mut retry_at = HashMap::new();
        retry_at.insert("b".to_string(), now + Duration::from_secs(5));
        let refs = set(&["b", "c"]);
        assert_eq!(
            ids_to_look_up(&refs, &HashSet::new(), &retry_at, now),
            vec!["c".to_string()]
        );
        assert_eq!(
            ids_to_look_up(
                &refs,
                &HashSet::new(),
                &retry_at,
                now + Duration::from_secs(5)
            ),
            vec!["b".to_string(), "c".to_string()]
        );
    }
}
