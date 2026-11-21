//! `canton-analytics`: runs WASM analytics plugins over SurrealDB into Delta
//! Lake tables, and queries those tables with SQL.

use std::path::PathBuf;
use std::time::Duration;

use analytics_reader::location::{Location, parse_options};
use analytics_reader::{Session, dashboard};
use anyhow::{Result, anyhow};
use canton_analytics::fetch::Fetcher;
use canton_analytics::runner::{Options, Runner};
use canton_analytics::{plugin, source};
use clap::{Args, Parser, Subcommand};
use deltalake::arrow::util::pretty::pretty_format_batches;
use tracing::error;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
// Parsed once at start, so the size of `Run` doesn't matter.
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run plugins: catch up with the database, then keep following it.
    Run {
        /// A plugin, a WebAssembly component (.wasm). May be given more than once.
        #[arg(long = "plugin")]
        plugins: Vec<PathBuf>,
        /// A folder of plugins: every .wasm file in it.
        #[arg(long)]
        plugins_dir: Option<PathBuf>,
        /// Where the tables go: a folder, or s3://, az:// or gs:// for cloud
        /// storage. Each plugin writes to a folder named after it.
        #[arg(long, env = "CANTON_ANALYTICS_TABLES", default_value = "tables")]
        tables: String,
        #[command(flatten)]
        storage: Storage,
        #[command(flatten)]
        connection: source::Connection,
        #[command(flatten)]
        tuning: Tuning,
        /// Stop once caught up, instead of following the database.
        #[arg(long)]
        once: bool,
    },
    /// Run a SQL query over a table and print the result.
    Sql {
        /// The table: a folder, or an s3://, az:// or gs:// URL.
        #[arg(long)]
        table: String,
        #[command(flatten)]
        storage: Storage,
        /// DataFusion SQL; the table is named after its plugin, with `-` as `_`.
        query: String,
    },
    /// Print a table's dashboard panels as text.
    Dashboard {
        #[arg(long)]
        table: String,
        #[command(flatten)]
        storage: Storage,
    },
    /// Rewrite a table's small files into large ones.
    Optimize {
        #[arg(long)]
        table: String,
        #[command(flatten)]
        storage: Storage,
    },
    /// Delete the files a table no longer uses.
    Vacuum {
        #[arg(long)]
        table: String,
        #[command(flatten)]
        storage: Storage,
        /// Keep files unused for less than this many minutes.
        #[arg(long, default_value_t = 60)]
        retention_minutes: u64,
    },
}

#[derive(Args)]
struct Storage {
    /// An object store option, as key=value (e.g. aws_endpoint=http://minio:9000).
    /// Credentials are usually taken from the cloud's environment variables.
    #[arg(long = "storage-option")]
    options: Vec<String>,
}

impl Storage {
    fn location(&self, text: &str) -> Result<Location> {
        Location::parse(text, parse_options(&self.options)?)
    }
}

#[derive(Args)]
struct Tuning {
    /// Largest cursor range (e.g. offsets) per step.
    #[arg(long, default_value_t = 10_000)]
    batch: i64,
    /// Commit once this many rows are waiting…
    #[arg(long, default_value_t = 50_000)]
    min_rows: usize,
    /// …or once the oldest has waited this many seconds.
    #[arg(long, default_value_t = 30)]
    max_wait: u64,
    /// Seconds between checks for new data while following.
    #[arg(long, default_value_t = 5)]
    every: u64,
    /// Save the plugin state at least every this many commits.
    #[arg(long, default_value_t = 20)]
    state_commits: usize,
    /// Compact once the table has this many small files.
    #[arg(long, default_value_t = 16)]
    compact_after: usize,
    /// Minutes between vacuums, which delete files unused for as long.
    #[arg(long, default_value_t = 60)]
    vacuum_minutes: u64,
}

impl Tuning {
    fn options(&self) -> Options {
        Options {
            batch: self.batch.max(1),
            min_rows: self.min_rows,
            max_wait: Duration::from_secs(self.max_wait),
            every: Duration::from_secs(self.every.max(1)),
            state_commits: self.state_commits.max(1),
            compact_after: self.compact_after.max(1),
            vacuum_every: Duration::from_secs(self.vacuum_minutes.max(1) * 60),
            retention: Duration::from_secs(self.vacuum_minutes.max(1) * 60),
            ..Options::default()
        }
    }
}

fn plugin_paths(plugins: Vec<PathBuf>, dir: Option<PathBuf>) -> Result<Vec<PathBuf>> {
    let mut paths = plugins;
    if let Some(dir) = dir {
        let mut found: Vec<PathBuf> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|e| e == "wasm"))
            .collect();
        found.sort();
        paths.extend(found);
    }
    if paths.is_empty() {
        return Err(anyhow!("no plugins: give --plugin or --plugins-dir"));
    }
    Ok(paths)
}

async fn print_sql(session: &Session, query: &str) -> Result<()> {
    let batches = session.sql(query).await?;
    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}

/// Waits for Ctrl-C, or for SIGTERM from a service manager.
async fn stop_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            r = tokio::signal::ctrl_c() => r?,
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Run {
            plugins,
            plugins_dir,
            tables,
            storage,
            connection,
            tuning,
            once,
        } => {
            let paths = plugin_paths(plugins, plugins_dir)?;
            let tables = storage.location(&tables)?;
            let fetcher = Fetcher::new(source::connect(&connection).await?);
            let engine = plugin::engine()?;
            let options = tuning.options();
            let mut runners = Vec::new();
            for path in &paths {
                runners.push(
                    Runner::new(&engine, path, &tables, fetcher.clone(), options.clone()).await?,
                );
            }
            let names: Vec<String> = runners.iter().map(|r| r.name().to_string()).collect();
            if let Some(dup) = names
                .iter()
                .enumerate()
                .find_map(|(i, n)| names[..i].contains(n).then_some(n))
            {
                return Err(anyhow!(
                    "two plugins are named {dup}; each needs its own table"
                ));
            }
            if once {
                let results =
                    futures_util::future::join_all(runners.iter_mut().map(|r| async move {
                        let summary = r.catch_up(true).await;
                        (r.name().to_string(), r.committed, summary)
                    }))
                    .await;
                let mut failed = false;
                for (name, cursor, summary) in results {
                    match summary {
                        Ok(s) => eprintln!(
                            "{name}: {} steps, {} commits, {} rows; cursor at {cursor}",
                            s.steps, s.commits, s.rows
                        ),
                        Err(e) => {
                            failed = true;
                            eprintln!("{name}: failed: {e:#}");
                        }
                    }
                }
                if failed {
                    return Err(anyhow!("some plugins failed"));
                }
            } else {
                let (stop_tx, stop) = tokio::sync::watch::channel(false);
                let mut tasks = Vec::new();
                for mut runner in runners {
                    let stop = stop.clone();
                    tasks.push(tokio::spawn(async move {
                        if let Err(e) = runner.follow(stop).await {
                            error!(plugin = %runner.name(), error = %format!("{e:#}"), "could not save on stop");
                        }
                    }));
                }
                stop_signal().await?;
                eprintln!("stopping: committing and saving state…");
                let _ = stop_tx.send(true);
                for t in tasks {
                    let _ = t.await;
                }
            }
        }
        Command::Sql {
            table,
            storage,
            query,
        } => print_sql(&Session::open(&storage.location(&table)?).await?, &query).await?,
        Command::Dashboard { table, storage } => {
            let session = Session::open(&storage.location(&table)?).await?;
            let d = dashboard::load(&session).await?;
            println!("{} (version {})", d.title, d.version.unwrap_or(-1));
            for p in d.panels {
                println!("\n## {} ({:?})", p.panel.title, p.panel.kind);
                match p.frame {
                    Ok(frame) => {
                        for row in 0..frame.rows().min(20) {
                            let cells: Vec<String> = frame
                                .columns
                                .iter()
                                .map(|c| c.values.text(row).unwrap_or_else(|| "∅".into()))
                                .collect();
                            println!("{}", cells.join(" | "));
                        }
                    }
                    Err(e) => println!("error: {e}"),
                }
            }
        }
        Command::Optimize { table, storage } => {
            let t = storage.location(&table)?.table().await?;
            let (_, m) = t.optimize().await?;
            eprintln!(
                "{} files rewritten into {}",
                m.num_files_removed, m.num_files_added
            );
        }
        Command::Vacuum {
            table,
            storage,
            retention_minutes,
        } => {
            let t = storage.location(&table)?.table().await?;
            let retention = chrono::Duration::minutes(retention_minutes as i64);
            let (_, m) = t
                .vacuum()
                .with_retention_period(retention)
                .with_enforce_retention_duration(false)
                .await?;
            eprintln!("{} files deleted", m.files_deleted.len());
        }
    }
    Ok(())
}
