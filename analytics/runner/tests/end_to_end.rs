//! The two example plugins over a small Canton sample, in an in-memory
//! SurrealDB, writing local Delta tables that the reader then queries.
//!
//! Builds the plugins first, so it needs the `wasm32-wasip2` target
//! (`rustup target add wasm32-wasip2`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use analytics_reader::frame::Frame;
use analytics_reader::location::Location;
use analytics_reader::manifest::Mode;
use analytics_reader::{Session, Tuning, catalog, dashboard};
use canton_analytics::fetch::Fetcher;
use canton_analytics::plugin;
use canton_analytics::runner::{Options, Runner, Summary};
use canton_analytics::source::{self, Connection, Db, UserLevel};
use futures_util::StreamExt;

const SAMPLE: &str = include_str!("canton_sample.surql");

/// Builds a plugin for wasm32-wasip2 and returns the `.wasm` file.
fn build_plugin(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../plugins")
        .join(name);
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| dir.join("target"));
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "--release",
            "--target",
            "wasm32-wasip2",
            "--target-dir",
        ])
        .arg(&target)
        .current_dir(&dir)
        .status()
        .expect("cargo runs");
    assert!(
        status.success(),
        "building {name} failed; is the wasm32-wasip2 target installed?"
    );
    target
        .join("wasm32-wasip2/release")
        .join(format!("{}.wasm", name.replace('-', "_")))
}

fn temp_dir(name: &str) -> Location {
    let dir = std::env::temp_dir().join(format!("canton-analytics-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    Location::parse(&dir.to_string_lossy(), HashMap::new()).unwrap()
}

/// Embedded SurrealDB needs more stack than test threads have.
fn run<F: std::future::Future<Output = ()> + Send + 'static>(f: F) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 << 20)
        .build()
        .unwrap();
    rt.block_on(async { tokio::spawn(f).await.unwrap() });
}

async fn sample_db() -> Db {
    let db = source::connect(&Connection {
        url: "mem://".into(),
        namespace: "canton".into(),
        database: "event_graph".into(),
        username: String::new(),
        password: String::new(),
        user_level: UserLevel::Database,
    })
    .await
    .unwrap();
    db.query(SAMPLE).await.unwrap().check().unwrap();
    db
}

async fn add(db: &Db, surql: &str) {
    db.query(surql).await.unwrap().check().unwrap();
}

/// Small steps, a commit for every step with rows, and the state saved
/// only when asked: so a restart has to replay.
fn small_steps() -> Options {
    Options {
        batch: 2,
        min_rows: 1,
        max_wait: Duration::from_secs(3_600),
        state_commits: 1_000,
        state_every: Duration::from_secs(3_600),
        ..Options::default()
    }
}

async fn query(session: &Session, sql: &str) -> Frame {
    Frame::from_batches(&session.sql(sql).await.unwrap(), |_| Default::default()).unwrap()
}

fn texts(frame: &Frame, column: &str) -> Vec<String> {
    let c = frame.column(column).unwrap();
    (0..frame.rows())
        .map(|i| c.values.text(i).unwrap_or_default())
        .collect()
}

fn numbers(frame: &Frame, column: &str) -> Vec<Option<f64>> {
    let c = frame.column(column).unwrap();
    (0..frame.rows()).map(|i| c.values.number(i)).collect()
}

#[test]
fn lifetimes_are_appended_once_and_resume_from_a_saved_state() {
    let wasm = build_plugin("contract-lifetimes");
    let tables = temp_dir("lifetimes");
    run(async move {
        let engine = plugin::engine().unwrap();
        let db = sample_db().await;
        // A new fetcher per runner, as in a new process: a fetcher reuses a
        // result for a moment, which would hide the rows just added.
        let fetcher = || Fetcher::new(db.clone());

        // Offsets 1..=5 in steps of 2. The archives at offsets 3 and 4 need
        // creates from earlier steps, which the plugin keeps in memory.
        let mut runner = Runner::new(&engine, &wasm, &tables, fetcher(), small_steps())
            .await
            .unwrap();
        assert_eq!(runner.name(), "contract-lifetimes");
        let summary = runner.catch_up(true).await.unwrap();
        // One commit for the rows of (2, 4], one at the end with the state.
        assert_eq!(
            summary,
            Summary {
                steps: 3,
                commits: 2,
                rows: 3
            }
        );
        assert_eq!((runner.cursor, runner.committed), (5, 5));
        assert_eq!(
            runner.catch_up(true).await.unwrap(),
            Summary::default(),
            "nothing new"
        );
        drop(runner);

        // Offset 6 creates a contract and archives Carol's ticket. It is
        // committed without saving the state, so the state stays at 5.
        add(
            &db,
            "UPSERT transaction:6 MERGE { offset: 6, effective_at: '2026-11-02T22:00:00Z' };
             UPSERT created:⟨00c6aaaa⟩ MERGE { contract_id: '00c6aaaa', template_name: 'Main.Cash', offset: 6, node_id: 1,
                signatories: ['Bank::1220f3a9c1d2'], created_at: '2026-11-02T22:00:00Z' };
             UPSERT exercised:[6, 0] MERGE { offset: 6, node_id: 0, choice_name: 'Ticket_Redeem', target_contract_id: '00c5e9f6',
                acting_parties: ['Carol::1220d2a6b3c7'], consuming: true, transaction_effective_at: '2026-11-02T22:00:00Z' };",
        )
        .await;
        let mut runner = Runner::new(&engine, &wasm, &tables, fetcher(), small_steps())
            .await
            .unwrap();
        assert_eq!(runner.committed, 5);
        assert_eq!(
            runner.catch_up(false).await.unwrap(),
            Summary {
                steps: 1,
                commits: 1,
                rows: 1
            }
        );
        drop(runner);

        // Offset 7 archives the contract made at 6. After a restart the
        // runner loads the state saved at 5 and replays (5, 6] without
        // writing its rows again, so it still knows that contract.
        add(
            &db,
            "UPSERT transaction:7 MERGE { offset: 7, effective_at: '2026-11-02T23:00:00Z' };
             UPSERT exercised:[7, 0] MERGE { offset: 7, node_id: 0, choice_name: 'Cash_Spend', target_contract_id: '00c6aaaa',
                acting_parties: ['Bank::1220f3a9c1d2'], consuming: true, transaction_effective_at: '2026-11-02T23:00:00Z' };",
        )
        .await;
        // Two small files are there already (offsets 4 and 6): the next
        // commit makes a third, and compaction merges them.
        let options = Options {
            compact_after: 2,
            ..small_steps()
        };
        let mut runner = Runner::new(&engine, &wasm, &tables, fetcher(), options)
            .await
            .unwrap();
        assert_eq!(
            (runner.cursor, runner.committed),
            (6, 6),
            "replayed up to the last commit"
        );
        assert_eq!(
            runner.catch_up(true).await.unwrap(),
            Summary {
                steps: 1,
                commits: 1,
                rows: 1
            }
        );
        // The explorer's lag settings, taken up before the next check.
        Tuning {
            min_rows: Some(10),
            max_wait_seconds: Some(2),
            ..Tuning::default()
        }
        .write_at(&tables.join("contract-lifetimes"))
        .await
        .unwrap();
        runner.apply_tuning().await;
        assert_eq!(
            (runner.options.min_rows, runner.options.max_wait),
            (10, Duration::from_secs(2))
        );
        runner.write_status(None, true).await;

        // Each contract once, with what the plugin knew when it was archived.
        let location = tables.join("contract-lifetimes");
        let session = Session::open(&location).await.unwrap();
        assert_eq!(session.name, "contract_lifetimes");
        let frame = query(
            &session,
            "SELECT contract_id, created_offset, archived_offset, lifetime_seconds FROM contract_lifetimes ORDER BY contract_id",
        )
        .await;
        assert_eq!(
            texts(&frame, "contract_id"),
            ["00c1a5b2", "00c2b6c3", "00c4d8e5", "00c5e9f6", "00c6aaaa"]
        );
        assert_eq!(
            numbers(&frame, "created_offset"),
            [Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(6.0)]
        );
        assert_eq!(
            numbers(&frame, "archived_offset"),
            [Some(3.0), Some(3.0), Some(4.0), Some(6.0), Some(7.0)]
        );
        // Cash 09:00 -> 09:12, offer 09:05 -> 09:12, Bob's ticket 09:12 -> 10:30,
        // Carol's ticket 10:30 -> 22:00, the new cash 22:00 -> 23:00.
        assert_eq!(
            numbers(&frame, "lifetime_seconds"),
            [
                Some(720.0),
                Some(420.0),
                Some(4680.0),
                Some(41400.0),
                Some(3600.0)
            ]
        );

        let mut history = session.table.history(Some(50));
        let mut operations = Vec::new();
        while let Some(c) = history.next().await {
            operations.extend(c.unwrap().operation);
        }
        assert_eq!(
            operations.first().map(String::as_str),
            Some("OPTIMIZE"),
            "{operations:?}"
        );

        // The manifest and the heartbeat the explorer reads.
        let manifest = session.manifest.clone().unwrap();
        assert_eq!(manifest.mode, Mode::Append);
        let status = session.status().await.unwrap().unwrap();
        assert!(status.stopped);
        assert_eq!((status.committed, status.high_water), (7, 7));
        assert_eq!((status.min_rows, status.max_wait_ms), (10, 2_000));

        // The plugin's own panels, all of which run.
        let board = dashboard::load(&session).await.unwrap();
        assert!(!board.generated);
        assert_eq!(board.panels.len(), 4);
        for p in &board.panels {
            assert!(p.frame.is_ok(), "{}: {:?}", p.panel.title, p.frame);
        }
        let totals = board.panels[0].frame.as_ref().unwrap();
        assert_eq!(
            totals.column("Archived").unwrap().values.number(0),
            Some(5.0)
        );
        let over_time = &board.panels[1];
        assert!(over_time.bucket_seconds.is_some());
        assert!(over_time.frame.as_ref().unwrap().rows() >= 2);
        let longest = board.panels[3].frame.as_ref().unwrap();
        assert_eq!(texts(longest, "contract_id")[0], "00c5e9f6");
    });
}

#[test]
fn two_plugins_share_one_database_and_party_activity_merges() {
    let lifetimes = build_plugin("contract-lifetimes");
    let parties = build_plugin("party-activity");
    let tables = temp_dir("two");
    run(async move {
        let engine = plugin::engine().unwrap();
        let fetcher = Fetcher::new(sample_db().await);
        let options = small_steps();
        let mut a = Runner::new(
            &engine,
            &lifetimes,
            &tables,
            fetcher.clone(),
            options.clone(),
        )
        .await
        .unwrap();
        let mut b = Runner::new(&engine, &parties, &tables, fetcher, options)
            .await
            .unwrap();
        let (sa, sb) = tokio::join!(a.catch_up(true), b.catch_up(true));
        assert_eq!(sa.unwrap().rows, 3);
        // Parties seen per step: Bank and Venue, then Bank, Venue and Bob,
        // then Carol. Merging leaves one row per party.
        assert_eq!(
            sb.unwrap(),
            Summary {
                steps: 3,
                commits: 3,
                rows: 6
            }
        );

        let entries = catalog::list(&tables).await.unwrap();
        let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["contract-lifetimes", "party-activity"]);
        assert!(entries.iter().all(|e| e.manifest.is_some()));

        let session = Session::open(&tables.join("party-activity")).await.unwrap();
        assert_eq!(
            session.manifest.as_ref().unwrap().mode,
            Mode::Merge(vec!["party".into()])
        );
        let frame = query(
            &session,
            "SELECT party, contracts_signed, choices_exercised, first_offset, last_offset FROM party_activity ORDER BY party",
        )
        .await;
        assert_eq!(
            texts(&frame, "party"),
            [
                "Bank::1220f3a9c1d2",
                "Bob::12209c5d1e8a",
                "Carol::1220d2a6b3c7",
                "Venue::1220b7e4a0f5"
            ]
        );
        assert_eq!(
            numbers(&frame, "contracts_signed"),
            [Some(2.0), Some(0.0), Some(0.0), Some(3.0)]
        );
        assert_eq!(
            numbers(&frame, "choices_exercised"),
            [Some(0.0), Some(3.0), Some(1.0), Some(0.0)]
        );
        assert_eq!(
            numbers(&frame, "first_offset"),
            [Some(1.0), Some(3.0), Some(5.0), Some(2.0)]
        );
        assert_eq!(
            numbers(&frame, "last_offset"),
            [Some(3.0), Some(4.0), Some(5.0), Some(4.0)]
        );

        // Upserts, which rewrite the one file rather than adding more.
        let mut history = session.table.history(Some(50));
        let mut operations = Vec::new();
        while let Some(c) = history.next().await {
            operations.extend(c.unwrap().operation);
        }
        assert_eq!(operations, ["MERGE", "MERGE", "MERGE", "CREATE TABLE"]);

        let board = dashboard::load(&session).await.unwrap();
        for p in &board.panels {
            assert!(p.frame.is_ok(), "{}: {:?}", p.panel.title, p.frame);
        }
    });
}
