//! Shared reads from SurrealDB, for several plugins in one runner.
//!
//! Plugins often ask the same thing at the same moment: the same high-water
//! query every few seconds, and, once caught up, the same query over the
//! same offset range. The fetcher runs each distinct request once and hands
//! the result to everyone who asked for it within a short window.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use surrealdb::types::Value;

use crate::source::{self, Db};

/// How long a finished result is reused.
const REUSE: Duration = Duration::from_secs(2);

type Pending<T> = Shared<BoxFuture<'static, Result<T, Arc<String>>>>;

struct Coalescer<K, T: Clone> {
    entries: Mutex<HashMap<K, (Instant, Pending<T>)>>,
}

impl<K: std::hash::Hash + Eq + Clone, T: Clone + Send + Sync + 'static> Coalescer<K, T> {
    fn new() -> Self {
        Coalescer {
            entries: Mutex::new(HashMap::new()),
        }
    }

    async fn get(&self, key: K, make: impl FnOnce() -> BoxFuture<'static, Result<T>>) -> Result<T> {
        let pending = {
            let mut entries = self.entries.lock().expect("not poisoned");
            let now = Instant::now();
            entries.retain(|_, (at, p)| now.duration_since(*at) < REUSE || p.peek().is_none());
            match entries.get(&key) {
                Some((_, p)) => p.clone(),
                None => {
                    let p = make()
                        .map(|r| r.map_err(|e| Arc::new(format!("{e:#}"))))
                        .boxed()
                        .shared();
                    entries.insert(key.clone(), (now, p.clone()));
                    p
                }
            }
        };
        let result = pending.await;
        if result.is_err() {
            // Don't hand an error to the next caller; let it try again.
            self.entries.lock().expect("not poisoned").remove(&key);
        }
        result.map_err(|e| anyhow!("{e}"))
    }
}

pub struct Fetcher {
    pub db: Db,
    high_water: Coalescer<String, i64>,
    queries: Coalescer<(String, i64, i64), Arc<Vec<Value>>>,
}

impl Fetcher {
    pub fn new(db: Db) -> Arc<Fetcher> {
        Arc::new(Fetcher {
            db,
            high_water: Coalescer::new(),
            queries: Coalescer::new(),
        })
    }

    pub async fn high_water(&self, sql: &str) -> Result<i64> {
        let db = self.db.clone();
        let owned = sql.to_string();
        self.high_water
            .get(sql.to_string(), move || {
                async move { source::high_water(&db, &owned).await }.boxed()
            })
            .await
    }

    /// The query's results over `(from, to]`, one value per statement.
    pub async fn run(&self, sql: &str, from: i64, to: i64) -> Result<Arc<Vec<Value>>> {
        let db = self.db.clone();
        let owned = sql.to_string();
        self.queries
            .get((sql.to_string(), from, to), move || {
                async move { Ok(Arc::new(source::run(&db, &owned, from, to).await?)) }.boxed()
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn the_same_request_runs_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c: Coalescer<u8, u8> = Coalescer::new();
        let make = |calls: Arc<AtomicUsize>| {
            move || {
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(7)
                }
                .boxed()
            }
        };
        let (a, b) = tokio::join!(c.get(1, make(calls.clone())), c.get(1, make(calls.clone())));
        assert_eq!((a.unwrap(), b.unwrap()), (7, 7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        c.get(2, make(calls.clone())).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
