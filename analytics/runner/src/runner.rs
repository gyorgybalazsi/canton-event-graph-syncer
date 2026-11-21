//! One plugin and its table, stepping through the database in cursor ranges.
//!
//! - A **step** runs the plugin's query over the next cursor range (at most
//!   `batch` offsets) and its `transform`, and keeps the rows in memory.
//! - A **commit** writes the kept rows and the new cursor to Delta, once
//!   there are `min_rows` of them or the oldest has waited `max_wait`. So a
//!   quiet ledger makes a few files an hour instead of one per poll.
//! - The plugin keeps its state in its own memory. Every few commits (or
//!   minutes) the commit also **saves the state**. On start, the runner
//!   loads the newest saved state and replays the range from it to the last
//!   commit, throwing those rows away, since they are already in the table.
//!   A failed call is handled the same way, with a fresh instance.
//! - After commits, **maintenance** compacts small files and vacuums old ones.

use std::path::Path;
use std::sync::Arc;

use analytics_reader::location::Location;
use analytics_reader::status::{Status, now_ms};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use deltalake::arrow::record_batch::RecordBatch;
use tokio::sync::watch;
use tracing::{info, warn};
use wasmtime::Engine;

use crate::convert;
use crate::fetch::Fetcher;
use crate::plugin::Plugin;
use crate::table::Table;

#[derive(Debug, Clone)]
pub struct Options {
    /// Largest cursor range per step, e.g. this many offsets.
    pub batch: i64,
    /// Commit once this many rows are waiting…
    pub min_rows: usize,
    /// …or once the oldest has waited this long.
    pub max_wait: Duration,
    /// How often to look for new data when caught up.
    pub every: Duration,
    /// Save the state at least every this many commits…
    pub state_commits: usize,
    /// …and at least this often.
    pub state_every: Duration,
    /// Compact once this many files were added.
    pub compact_after: usize,
    /// Vacuum this often…
    pub vacuum_every: Duration,
    /// …deleting files unused for this long.
    pub retention: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            batch: 10_000,
            min_rows: 50_000,
            max_wait: Duration::from_secs(30),
            every: Duration::from_secs(5),
            state_commits: 20,
            state_every: Duration::from_secs(600),
            compact_after: 16,
            vacuum_every: Duration::from_secs(3_600),
            retention: Duration::from_secs(3_600),
        }
    }
}

/// Longest wait after failures before trying again.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

pub struct Runner {
    pub plugin: Plugin,
    pub table: Table,
    pub options: Options,
    fetcher: Arc<Fetcher>,
    /// How far the steps have got, including rows not yet committed.
    pub cursor: i64,
    /// How far the table has got.
    pub committed: i64,
    /// The cursor of the newest saved state (0: none, start from nothing).
    state_cursor: i64,
    /// A rebuild after a failure didn't finish: redo it up to here first.
    rebuild_to: Option<i64>,
    /// The database's high-water mark, when last read.
    high_water: i64,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    pending_since: Option<Instant>,
    commits_since_state: usize,
    last_state_save: Instant,
    last_vacuum: Instant,
}

/// What a call to [`Runner::catch_up`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Summary {
    pub steps: usize,
    pub commits: usize,
    pub rows: usize,
}

impl Runner {
    /// Loads the plugin, opens its table in `tables` (a folder named after
    /// the plugin), and rebuilds its state.
    pub async fn new(
        engine: &Engine,
        plugin_path: &Path,
        tables: &Location,
        fetcher: Arc<Fetcher>,
        options: Options,
    ) -> Result<Runner> {
        let plugin = tokio::task::block_in_place(|| Plugin::load(engine, plugin_path))?;
        let location = tables.join(&plugin.info.name);
        Runner::with_table(plugin, &location, fetcher, options).await
    }

    pub async fn with_table(
        plugin: Plugin,
        location: &Location,
        fetcher: Arc<Fetcher>,
        options: Options,
    ) -> Result<Runner> {
        let mut table = Table::open_or_create(location, &plugin.manifest, &plugin.schema).await?;
        let saved = table.saved().await?;
        let mut runner = Runner {
            plugin,
            table,
            options,
            fetcher,
            cursor: saved.state_cursor,
            committed: saved.cursor,
            state_cursor: saved.state_cursor,
            rebuild_to: None,
            high_water: 0,
            pending: Vec::new(),
            pending_rows: 0,
            pending_since: None,
            commits_since_state: 0,
            last_state_save: Instant::now(),
            last_vacuum: Instant::now(),
        };
        if !saved.state.is_empty() {
            runner.plugin.load_state(&saved.state)?;
        }
        // Rebuild the state up to the last commit; its rows are in the table.
        runner.replay(saved.cursor).await?;
        info!(
            plugin = %runner.plugin.info.name,
            version = %runner.plugin.info.version,
            cursor = runner.committed,
            table = %runner.table.location,
            "plugin ready"
        );
        Ok(runner)
    }

    pub fn name(&self) -> &str {
        &self.plugin.info.name
    }

    /// Brings the plugin's state from `self.cursor` to `to`, throwing the
    /// rows away: they are already in the table, or already waiting.
    async fn replay(&mut self, to: i64) -> Result<()> {
        let from = self.cursor;
        let (pending, pending_rows) = (std::mem::take(&mut self.pending), self.pending_rows);
        while self.cursor < to {
            let next = to.min(self.cursor.saturating_add(self.options.batch));
            self.transform_range(self.cursor, next).await?;
            self.cursor = next;
        }
        self.pending = pending;
        self.pending_rows = pending_rows;
        if to > from {
            info!(plugin = %self.plugin.info.name, from, to, "state rebuilt");
        }
        Ok(())
    }

    /// After a failed call: a new instance, with the saved state, replayed
    /// up to `to`.
    async fn rebuild(&mut self, to: i64) -> Result<()> {
        self.plugin.reset()?;
        if self.state_cursor > 0 {
            let state = self.table.read_state(self.state_cursor).await?;
            self.plugin.load_state(&state)?;
        }
        self.cursor = self.state_cursor;
        self.replay(to).await
    }

    /// Runs the plugin over `(from, to]` and keeps the rows.
    async fn transform_range(&mut self, from: i64, to: i64) -> Result<usize> {
        let results = self.fetcher.run(&self.plugin.query, from, to).await?;
        if results.len() != self.plugin.inputs.len() {
            return Err(anyhow!(
                "the plugin {}'s query has {} statements but it declares {} inputs",
                self.plugin.info.name,
                results.len(),
                self.plugin.inputs.len()
            ));
        }
        let inputs = results
            .iter()
            .zip(&self.plugin.inputs)
            .enumerate()
            .map(|(i, (value, schema))| {
                convert::to_batch(schema, &convert::rows(value))
                    .map_err(|e| anyhow!("statement {} of {}: {e:#}", i + 1, self.plugin.info.name))
            })
            .collect::<Result<Vec<_>>>()?;
        let plugin = &mut self.plugin;
        let batches = tokio::task::block_in_place(|| plugin.transform(&inputs))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if rows > 0 {
            self.pending.extend(batches);
            self.pending_rows += rows;
        }
        Ok(rows)
    }

    /// Steps until caught up with the database, committing as rows pile up.
    /// With `finish`, also commits what is left and saves the state.
    pub async fn catch_up(&mut self, finish: bool) -> Result<Summary> {
        let mut summary = Summary::default();
        if let Some(to) = self.rebuild_to {
            self.rebuild(to).await?;
            self.rebuild_to = None;
        }
        let high = self.fetcher.high_water(&self.plugin.high_water).await?;
        self.high_water = high;
        while self.cursor < high {
            let from = self.cursor;
            let to = high.min(from.saturating_add(self.options.batch));
            match self.transform_range(from, to).await {
                Ok(rows) => summary.rows += rows,
                Err(e) => {
                    // The call may have changed the plugin's memory halfway:
                    // start a new instance at `from` before trying again.
                    if let Err(rebuild) = self.rebuild(from).await {
                        warn!(plugin = %self.plugin.info.name, error = %format!("{rebuild:#}"), "could not rebuild the state");
                        self.rebuild_to = Some(from);
                    }
                    return Err(e);
                }
            }
            self.cursor = to;
            self.pending_since.get_or_insert_with(Instant::now);
            summary.steps += 1;
            if self.pending_rows >= self.options.min_rows {
                // The last step of a finishing catch-up saves the state
                // with its rows, rather than in a commit of its own.
                self.commit(finish && to >= high).await?;
                summary.commits += 1;
            }
        }
        let waited = self
            .pending_since
            .is_some_and(|t| t.elapsed() >= self.options.max_wait);
        if self.cursor > self.committed && (finish || waited) {
            self.commit(finish).await?;
            summary.commits += 1;
        } else if finish && self.commits_since_state > 0 {
            // Nothing new, but the state isn't saved yet: save it now.
            self.commit(true).await?;
            summary.commits += 1;
        }
        Ok(summary)
    }

    /// Commits the rows waiting and the cursor; saves the state when it's
    /// due, or with `save_state`.
    async fn commit(&mut self, save_state: bool) -> Result<()> {
        let save_state = save_state
            || self.commits_since_state + 1 >= self.options.state_commits
            || self.last_state_save.elapsed() >= self.options.state_every;
        let state = if save_state {
            let plugin = &mut self.plugin;
            Some(tokio::task::block_in_place(|| plugin.save_state())?)
        } else {
            None
        };
        // Arrow batches are reference-counted, so the copy is cheap; it keeps
        // the rows if the commit fails.
        let batches = self.pending.clone();
        let rows = self
            .table
            .commit(batches, self.cursor, state.as_deref())
            .await?;
        self.pending.clear();
        info!(plugin = %self.plugin.info.name, from = self.committed, to = self.cursor, rows, state_saved = save_state, "committed");
        self.committed = self.cursor;
        self.pending_rows = 0;
        self.pending_since = None;
        if save_state {
            self.state_cursor = self.cursor;
            self.commits_since_state = 0;
            self.last_state_save = Instant::now();
        } else {
            self.commits_since_state += 1;
        }
        self.maintain().await
    }

    async fn maintain(&mut self) -> Result<()> {
        if self.table.small_files >= self.options.compact_after {
            self.table.optimize().await?;
        }
        if self.last_vacuum.elapsed() >= self.options.vacuum_every {
            self.table.vacuum(self.options.retention).await?;
            self.last_vacuum = Instant::now();
        }
        Ok(())
    }

    /// Keeps following the database until `stop` turns true, then commits
    /// what is left and saves the state. A failure pauses this plugin, with
    /// a growing wait, and doesn't stop the others.
    pub async fn follow(&mut self, mut stop: watch::Receiver<bool>) -> Result<()> {
        let mut backoff = self.options.every;
        loop {
            let result = self.catch_up(false).await;
            let error = result.as_ref().err().map(|e| format!("{e:#}"));
            self.write_status(error, false).await;
            let wait = match result {
                Ok(_) => {
                    backoff = self.options.every;
                    self.options.every
                }
                Err(e) => {
                    // The failed range runs again next time; the rows of
                    // the steps before it are still waiting to be committed.
                    warn!(plugin = %self.plugin.info.name, error = %format!("{e:#}"), retry_in = ?backoff, "step failed");
                    let wait = backoff;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    wait
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = stop.changed() => break,
            }
            if *stop.borrow() {
                break;
            }
        }
        let saved = if self.cursor > self.committed || self.commits_since_state > 0 {
            self.commit(true).await
        } else {
            Ok(())
        };
        self.write_status(saved.as_ref().err().map(|e| format!("{e:#}")), true)
            .await;
        info!(plugin = %self.plugin.info.name, cursor = self.committed, "stopped");
        saved
    }

    /// Writes the heartbeat the explorer shows. A failure to write it is
    /// only logged: it mustn't stop the work. (`&mut`, because a shared
    /// borrow held across the write would need the plugin to be `Sync`.)
    pub async fn write_status(&mut self, error: Option<String>, stopped: bool) {
        let status = Status {
            updated_at_ms: now_ms(),
            every_ms: self.options.every.as_millis() as i64,
            stopped,
            plugin_version: self.plugin.info.version.clone(),
            committed: self.committed,
            cursor: self.cursor,
            high_water: self.high_water,
            error,
            host: format!(
                "{}:{}",
                std::env::var("HOSTNAME").unwrap_or_default(),
                std::process::id()
            ),
        };
        if let Err(e) = self.table.write_status(&status).await {
            warn!(plugin = %self.plugin.info.name, error = %format!("{e:#}"), "could not write the status");
        }
    }
}
