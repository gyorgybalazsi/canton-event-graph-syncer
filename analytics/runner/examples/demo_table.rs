//! Writes demo tables without a ledger or a SurrealDB server: loads the
//! Canton sample into an in-memory database and runs the given plugins over
//! it once, into a folder the explorer's dashboard can open.
//!
//! ```text
//! cargo run -p canton-analytics --example demo_table -- tables \
//!     analytics/plugins/contract-lifetimes/target/wasm32-wasip2/release/contract_lifetimes.wasm
//! ```

use std::collections::HashMap;
use std::path::PathBuf;

use analytics_reader::location::Location;
use anyhow::{Context, Result};
use canton_analytics::fetch::Fetcher;
use canton_analytics::plugin;
use canton_analytics::runner::{Options, Runner};
use canton_analytics::source::{self, Connection, UserLevel};

const SAMPLE: &str = include_str!("../tests/canton_sample.surql");

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let tables = args
        .next()
        .context("usage: demo_table <tables folder> <plugin.wasm>...")?;
    let plugins: Vec<PathBuf> = args.map(PathBuf::from).collect();
    anyhow::ensure!(!plugins.is_empty(), "give at least one plugin .wasm file");
    // Embedded SurrealDB needs a bigger stack than the default.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 << 20)
        .build()?;
    // On a worker thread: the runner uses `block_in_place`.
    rt.block_on(async move { tokio::spawn(work(tables, plugins)).await? })
}

async fn work(tables: String, plugins: Vec<PathBuf>) -> Result<()> {
    {
        let db = source::connect(&Connection {
            url: "mem://".into(),
            namespace: "canton".into(),
            database: "event_graph".into(),
            username: String::new(),
            password: String::new(),
            user_level: UserLevel::Database,
        })
        .await?;
        db.query(SAMPLE).await?.check()?;
        let tables = Location::parse(&tables, HashMap::new())?;
        let engine = plugin::engine()?;
        let fetcher = Fetcher::new(db);
        for path in &plugins {
            let mut runner =
                Runner::new(&engine, path, &tables, fetcher.clone(), Options::default()).await?;
            let summary = runner.catch_up(true).await?;
            runner.write_status(None, true).await;
            println!(
                "{}: {} rows in {}",
                runner.name(),
                summary.rows,
                runner.table.location
            );
        }
        Ok(())
    }
}
