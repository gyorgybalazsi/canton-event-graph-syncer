//! Integration tests against a real SurrealDB server.
//!
//! Each test starts its own throwaway in-memory server with the `surreal`
//! program on a free port (`surreal start ... memory`), so no real data is
//! ever touched. If `surreal` is not installed, the tests print a message and
//! pass without running. Set `SURREAL_BIN` to use a specific binary.

use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use client::packages::PackageInfo;
use ledger_api::v2::{
    CreatedEvent, Event, ExercisedEvent, GetUpdatesResponse, Identifier, OffsetCheckpoint,
    ParticipantAuthorizationAdded, ParticipantAuthorizationChanged,
    ParticipantAuthorizationRevoked, ParticipantPermission, Record, RecordField, TopologyEvent,
    TopologyTransaction, Transaction, Value as DamlValue, event, get_updates_response::Update,
    topology_event, value::Sum,
};
use surrealdb::engine::any;
use surrealdb::opt::auth::Root;
use surrealdb::types::{Number, Value};
use syncer::config::{self, DbConfig};
use syncer::model::{self, WriteBatch};
use syncer::store::{BACKGROUND_INDEXES, Store};
use syncer::writer::{StreamSettings, apply_batch_stream};

const NS: &str = "test";
const DB: &str = "test";
const USER: &str = "streamer";
const PASS: &str = "streamer-test";

fn surreal_bin() -> Option<String> {
    let bin = std::env::var("SURREAL_BIN").unwrap_or_else(|_| "surreal".to_string());
    let ok = Command::new(&bin)
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    ok.then_some(bin)
}

macro_rules! require_surreal {
    () => {
        match surreal_bin() {
            Some(bin) => bin,
            None => {
                eprintln!(
                    "SKIPPED: the `surreal` program is not installed (or set SURREAL_BIN); \
                     skipping SurrealDB integration test"
                );
                return;
            }
        }
    };
}

/// A throwaway `surreal start ... memory` server, killed on drop.
struct TestServer {
    bin: String,
    port: u16,
    child: Option<Child>,
}

impl TestServer {
    async fn start(bin: &str) -> TestServer {
        let port = free_port();
        let mut server = TestServer {
            bin: bin.to_string(),
            port,
            child: None,
        };
        server.spawn().await;
        server
    }

    async fn spawn(&mut self) {
        let child = Command::new(&self.bin)
            .args([
                "start",
                "--log",
                "warn",
                "--user",
                "root",
                "--pass",
                "root",
                "--bind",
                &format!("127.0.0.1:{}", self.port),
                "memory",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start surreal");
        self.child = Some(child);
        wait_for_port(self.port).await;
        self.setup_as_root().await;
    }

    fn stop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    async fn restart(&mut self) {
        self.stop();
        self.spawn().await;
    }

    fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }

    /// What an administrator does once: create the namespace, database and
    /// the app's database user.
    async fn setup_as_root(&self) {
        let db = any::connect(self.url()).await.expect("root connect");
        db.signin(Root {
            username: "root".into(),
            password: "root".into(),
        })
        .await
        .expect("root signin");
        let mut r = db
            .query(format!(
                "DEFINE NAMESPACE IF NOT EXISTS {NS}; USE NS {NS};
                 DEFINE DATABASE IF NOT EXISTS {DB}; USE DB {DB};
                 DEFINE USER IF NOT EXISTS {USER} ON DATABASE PASSWORD '{PASS}' ROLES EDITOR;"
            ))
            .await
            .expect("setup query");
        assert!(r.take_errors().is_empty(), "setup failed");
    }

    /// The app's connection settings, read through its normal config file.
    fn db_config(&self, password: &str) -> DbConfig {
        let dir = std::env::temp_dir().join(format!("egi-test-{}", self.port));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            format!(
                r#"
active_profile = "local"

[logging]
level = "info"

[surrealdb]
url = "{}"
namespace = "{NS}"
database = "{DB}"
username = "{USER}"
password = "{password}"

[profiles.local.ledger]
fake_jwt_user = "alice"
url = "http://localhost:6865"
"#,
                self.url()
            ),
        )
        .unwrap();
        let cfg = config::read_config(&path, None).expect("read config");
        let _ = std::fs::remove_dir_all(&dir);
        cfg.surrealdb.resolve().expect("resolve db config")
    }

    /// A connected store that gives up after a few failed writes, so a
    /// broken statement fails the test instead of retrying forever.
    async fn store(&self) -> Store {
        self.unbounded_store().await.with_max_write_attempts(3)
    }

    /// A connected store that retries writes until they succeed (production behaviour).
    async fn unbounded_store(&self) -> Store {
        Store::connect(&self.db_config(PASS))
            .await
            .expect("store connect")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "surreal did not start on {port}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------- fixtures

fn text(s: &str) -> DamlValue {
    DamlValue {
        sum: Some(Sum::Text(s.into())),
    }
}

fn field(label: &str, value: DamlValue) -> RecordField {
    RecordField {
        label: label.into(),
        value: Some(value),
    }
}

fn ts(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos: 0 })
}

fn template(entity: &str) -> Option<Identifier> {
    Some(Identifier {
        package_id: "#pkg".into(),
        module_name: "Main".into(),
        entity_name: entity.into(),
    })
}

/// A contract that exists before the stream starts (loaded from the ACS).
fn acs_contract() -> CreatedEvent {
    CreatedEvent {
        contract_id: "cid-old".into(),
        template_id: template("Asset"),
        signatories: vec!["alice::1220".into()],
        created_at: ts(1_700_000_000),
        ..Default::default()
    }
}

/// Alice exercises `Transfer` on cid-old (consuming), which creates cid-new
/// whose arguments hold a nested `address` record.
fn transfer_tx(offset: i64) -> Transaction {
    let address = DamlValue {
        sum: Some(Sum::Record(Record {
            record_id: None,
            fields: vec![field("city", text("Zurich")), field("zip", text("8000"))],
        })),
    };
    let created = CreatedEvent {
        offset,
        node_id: 1,
        contract_id: format!("cid-new-{offset}"),
        template_id: template("Asset"),
        signatories: vec!["bob::1220".into()],
        created_at: ts(1_704_067_200),
        create_arguments: Some(Record {
            record_id: None,
            fields: vec![field("owner", text("bob")), field("address", address)],
        }),
        ..Default::default()
    };
    let exercised = ExercisedEvent {
        offset,
        node_id: 0,
        contract_id: "cid-old".into(),
        template_id: template("Asset"),
        choice: "Transfer".into(),
        choice_argument: Some(DamlValue {
            sum: Some(Sum::Record(Record {
                record_id: None,
                fields: vec![field("newOwner", text("bob"))],
            })),
        }),
        acting_parties: vec!["alice::1220".into()],
        consuming: true,
        last_descendant_node_id: 1,
        ..Default::default()
    };
    Transaction {
        offset,
        update_id: format!("update-{offset}"),
        command_id: "cmd".into(),
        effective_at: ts(1_704_067_200),
        record_time: ts(1_704_067_201),
        events: vec![
            Event {
                event: Some(event::Event::Exercised(exercised)),
            },
            Event {
                event: Some(event::Event::Created(created)),
            },
        ],
        ..Default::default()
    }
}

fn update(tx: Transaction) -> GetUpdatesResponse {
    GetUpdatesResponse {
        update: Some(Update::Transaction(tx)),
    }
}

const ALICE: &str = "alice::1220";
const P1: &str = "PAR::participant1::1220";

fn topology(offset: i64, event: topology_event::Event) -> GetUpdatesResponse {
    GetUpdatesResponse {
        update: Some(Update::TopologyTransaction(TopologyTransaction {
            update_id: format!("topo-{offset}"),
            offset,
            synchronizer_id: "sync::1220".into(),
            record_time: ts(1_700_000_000 + offset),
            events: vec![TopologyEvent { event: Some(event) }],
            ..Default::default()
        })),
    }
}

fn hosting_added(offset: i64) -> GetUpdatesResponse {
    topology(
        offset,
        topology_event::Event::ParticipantAuthorizationAdded(ParticipantAuthorizationAdded {
            party_id: ALICE.into(),
            participant_id: P1.into(),
            participant_permission: ParticipantPermission::Submission as i32,
        }),
    )
}

fn hosting_changed(offset: i64) -> GetUpdatesResponse {
    topology(
        offset,
        topology_event::Event::ParticipantAuthorizationChanged(ParticipantAuthorizationChanged {
            party_id: ALICE.into(),
            participant_id: P1.into(),
            participant_permission: ParticipantPermission::Observation as i32,
        }),
    )
}

fn hosting_revoked(offset: i64) -> GetUpdatesResponse {
    topology(
        offset,
        topology_event::Event::ParticipantAuthorizationRevoked(ParticipantAuthorizationRevoked {
            party_id: ALICE.into(),
            participant_id: P1.into(),
        }),
    )
}

async fn count(store: &Store, table: &str) -> i64 {
    let mut r = store
        .client()
        .query("RETURN count(SELECT id FROM type::table($t));")
        .bind(("t", table.to_string()))
        .await
        .unwrap();
    assert!(r.take_errors().is_empty());
    match r.take::<Value>(0).unwrap() {
        Value::Number(Number::Int(n)) => n,
        other => panic!("unexpected count {other:?}"),
    }
}

async fn query_value(store: &Store, q: &str) -> Value {
    let mut r = store.client().query(q).await.unwrap();
    let errors = r.take_errors();
    assert!(errors.is_empty(), "{q}: {errors:?}");
    r.take::<Value>(0).unwrap()
}

fn first_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Array(a) => a.first().and_then(first_string),
        _ => None,
    }
}

async fn write_fixture(store: &Store) {
    store
        .write_acs_batch(&model::acs_created_to_batch(&acs_contract()), 5, None, true)
        .await
        .unwrap();
    store
        .write_batch(&model::update_to_batch(&update(transfer_tx(10))))
        .await
        .unwrap();
}

// ------------------------------------------------------------------- tests

#[tokio::test]
async fn connects_and_refuses_wrong_password() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;

    let store = server.store().await;
    assert_eq!(store.last_processed_offset().await.unwrap(), None);

    let wrong = server.db_config("not-the-password");
    assert!(
        Store::connect(&wrong).await.is_err(),
        "a wrong password must be refused"
    );

    // The retrying connect gives up at once instead of waiting forever.
    let err = tokio::time::timeout(Duration::from_secs(10), Store::connect_with_retry(&wrong))
        .await
        .expect("connect_with_retry must not retry a refused login")
        .err()
        .expect("a wrong password must be refused");
    assert!(format!("{err:#}").contains("refused user"), "{err:#}");
}

#[tokio::test]
async fn messages_produce_records_nested_fields_and_relations() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;
    write_fixture(&store).await;

    // Records
    assert_eq!(count(&store, "transaction").await, 1);
    assert_eq!(count(&store, "created").await, 2);
    assert_eq!(count(&store, "exercised").await, 1);
    assert_eq!(count(&store, "party").await, 1); // alice requested it

    // Nested arguments are queryable by path.
    let v = query_value(
        &store,
        "SELECT VALUE contract_id FROM created WHERE create_arguments.address.city = 'Zurich';",
    )
    .await;
    assert_eq!(first_string(&v).as_deref(), Some("cid-new-10"));
    let v = query_value(
        &store,
        "SELECT VALUE choice_argument.newOwner FROM ONLY exercised:[10, 0];",
    )
    .await;
    assert_eq!(first_string(&v).as_deref(), Some("bob"));

    // Arrays and nested objects, not joined strings.
    let v = query_value(
        &store,
        "SELECT VALUE signatories FROM ONLY created:⟨cid-new-10⟩;",
    )
    .await;
    assert!(matches!(&v, Value::Array(a) if a.len() == 1), "{v:?}");
    let v = query_value(
        &store,
        "SELECT VALUE template.entity_name FROM ONLY created:⟨cid-old⟩;",
    )
    .await;
    assert_eq!(first_string(&v).as_deref(), Some("Asset"));

    // Timestamps are datetimes.
    let v = query_value(
        &store,
        "RETURN [
            type::is_datetime((SELECT VALUE effective_at FROM ONLY transaction:10)),
            type::is_datetime((SELECT VALUE created_at FROM ONLY created:⟨cid-new-10⟩)),
            type::is_datetime((SELECT VALUE transaction_effective_at FROM ONLY exercised:[10, 0]))
        ];",
    )
    .await;
    assert_eq!(
        v,
        Value::Array(vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)].into())
    );

    // ACS contract flag and label.
    let v = query_value(&store, "SELECT VALUE name FROM ONLY created:⟨cid-old⟩;").await;
    assert_eq!(first_string(&v).as_deref(), Some("Asset@ACS"));

    // Relations
    for (table, expected) in [
        ("action", 1),
        ("consequence", 1),
        ("target", 1),
        ("consumes", 1),
        ("requested", 1),
    ] {
        assert_eq!(count(&store, table).await, expected, "{table}");
    }
    let v = query_value(
        &store,
        "SELECT VALUE ->action->exercised->consequence->created.contract_id FROM ONLY transaction:10;",
    )
    .await;
    assert_eq!(first_string(&v).as_deref(), Some("cid-new-10"));
    let v = query_value(
        &store,
        "SELECT VALUE ->consumes->created.contract_id FROM ONLY exercised:[10, 0];",
    )
    .await;
    assert_eq!(first_string(&v).as_deref(), Some("cid-old"));
    let v = query_value(
        &store,
        "SELECT VALUE ->requested->transaction.update_id FROM ONLY party:⟨alice::1220⟩;",
    )
    .await;
    assert_eq!(first_string(&v).as_deref(), Some("update-10"));

    // Resume point and ACS marker.
    assert_eq!(store.last_processed_offset().await.unwrap(), Some(10));
    let marker = store.read_acs_marker().await.unwrap().unwrap();
    assert_eq!(marker.acs_offset, 5);
    assert!(marker.acs_complete);

    // A non-transaction update writes nothing.
    let checkpoint = GetUpdatesResponse {
        update: Some(Update::OffsetCheckpoint(OffsetCheckpoint {
            offset: 11,
            synchronizer_times: vec![],
        })),
    };
    assert!(model::update_to_batch(&checkpoint).is_empty());
}

#[tokio::test]
async fn package_rows_resolve_through_the_template_link() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;

    let mut created = acs_contract();
    created.template_id = Some(Identifier {
        package_id: "pkg-abc".into(),
        module_name: "Main".into(),
        entity_name: "Asset".into(),
    });
    created.package_name = "my-assets".into();
    let mut batch = model::acs_created_to_batch(&created);
    assert!(batch.package_refs.contains("pkg-abc"));
    batch.packages.push(model::package_row(&PackageInfo {
        package_id: "pkg-abc".into(),
        package_name: "my-assets".into(),
        package_version: "1.2.0".into(),
    }));
    store.write_acs_batch(&batch, 5, None, true).await.unwrap();

    assert_eq!(count(&store, "package").await, 1);
    let name = query_value(
        &store,
        "SELECT VALUE template.package_name FROM created:`cid-old`;",
    )
    .await;
    assert_eq!(first_string(&name).as_deref(), Some("my-assets"));
    let version = query_value(
        &store,
        "SELECT VALUE template.package.package_version FROM created \
         WHERE template.package_name = 'my-assets';",
    )
    .await;
    assert_eq!(first_string(&version).as_deref(), Some("1.2.0"));
}

#[tokio::test]
async fn target_to_unknown_contract_is_skipped() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;
    // No ACS: cid-old is unknown, so there is nothing to point TARGET/CONSUMES at.
    store
        .write_batch(&model::update_to_batch(&update(transfer_tx(10))))
        .await
        .unwrap();
    assert_eq!(count(&store, "target").await, 0);
    assert_eq!(count(&store, "consumes").await, 0);
    assert_eq!(count(&store, "consequence").await, 1);
}

#[tokio::test]
async fn repeated_message_creates_no_duplicates() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;
    write_fixture(&store).await;
    write_fixture(&store).await;
    // And once more as part of a bigger batch.
    let mut batch = model::update_to_batch(&update(transfer_tx(10)));
    batch.extend(model::update_to_batch(&update(transfer_tx(10))));
    store.write_batch(&batch).await.unwrap();

    for (table, expected) in [
        ("transaction", 1),
        ("created", 2),
        ("exercised", 1),
        ("party", 1),
        ("action", 1),
        ("consequence", 1),
        ("target", 1),
        ("consumes", 1),
        ("requested", 1),
    ] {
        assert_eq!(count(&store, table).await, expected, "{table}");
    }
}

#[tokio::test]
async fn topology_events_are_logged_and_hosting_tracks_the_latest_state() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;

    store
        .write_batch(&model::update_to_batch(&hosting_added(20)))
        .await
        .unwrap();
    // Added and changed in one batch: applied in order.
    let mut batch = model::update_to_batch(&hosting_changed(21));
    batch.extend(model::update_to_batch(&hosting_revoked(22)));
    store.write_batch(&batch).await.unwrap();

    assert_eq!(count(&store, "topology_event").await, 3);
    assert_eq!(count(&store, "participant").await, 1);
    assert_eq!(count(&store, "party").await, 1);
    assert_eq!(count(&store, "hosted_on").await, 1);
    assert_eq!(store.last_processed_offset().await.unwrap(), Some(22));

    // Log records link to their party and participant, in both directions.
    let kinds = query_value(
        &store,
        "SELECT VALUE kind FROM topology_event WHERE participant.name = 'participant1' ORDER BY offset;",
    )
    .await;
    assert_eq!(
        kinds,
        Value::Array(vec!["added", "changed", "revoked"].into_iter().collect())
    );
    let via_party = query_value(
        &store,
        "SELECT VALUE count(<~topology_event) FROM ONLY type::record('party', 'alice::1220');",
    )
    .await;
    assert_eq!(via_party, Value::Number(Number::Int(3)));
    let record_time = query_value(
        &store,
        "SELECT VALUE type::is_datetime(record_time) FROM ONLY topology_event:[20, 0];",
    )
    .await;
    assert_eq!(record_time, Value::Bool(true));

    // The edge holds the latest state: revoked, last permission kept.
    let edge = query_value(
        &store,
        "SELECT VALUE [active, permission, since_offset, revoked_offset, last_offset] \
         FROM ONLY hosted_on:['alice::1220', 'PAR::participant1::1220'];",
    )
    .await;
    let expected: Value = Value::Array(
        vec![
            Value::Bool(false),
            Value::String("observation".into()),
            Value::Number(Number::Int(20)),
            Value::Number(Number::Int(22)),
            Value::Number(Number::Int(22)),
        ]
        .into_iter()
        .collect(),
    );
    assert_eq!(edge, expected);
    let hosted = query_value(
        &store,
        "SELECT VALUE ->hosted_on->participant.name FROM ONLY type::record('party', 'alice::1220');",
    )
    .await;
    assert_eq!(first_string(&hosted).as_deref(), Some("participant1"));

    // Replaying an older update adds no records and doesn't roll the edge back.
    store
        .write_batch(&model::update_to_batch(&hosting_added(20)))
        .await
        .unwrap();
    assert_eq!(count(&store, "topology_event").await, 3);
    let active = query_value(
        &store,
        "SELECT VALUE active FROM ONLY hosted_on:['alice::1220', 'PAR::participant1::1220'];",
    )
    .await;
    assert_eq!(active, Value::Bool(false));

    // Hosting again after the revocation reactivates the edge.
    store
        .write_batch(&model::update_to_batch(&hosting_added(30)))
        .await
        .unwrap();
    let edge = query_value(
        &store,
        "SELECT VALUE [active, since_offset, revoked_offset] \
         FROM ONLY hosted_on:['alice::1220', 'PAR::participant1::1220'];",
    )
    .await;
    let expected: Value = Value::Array(
        vec![
            Value::Bool(true),
            Value::Number(Number::Int(30)),
            Value::None,
        ]
        .into_iter()
        .collect(),
    );
    assert_eq!(edge, expected);

    store.clear_all().await.unwrap();
    for table in ["topology_event", "participant", "hosted_on"] {
        assert_eq!(count(&store, table).await, 0, "{table}");
    }
}

#[tokio::test]
async fn batch_write_through_the_stream_writer() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;

    let batches: Vec<WriteBatch> = (1..=250)
        .map(|o| model::update_to_batch(&update(transfer_tx(o))))
        .collect();
    let stream = tokio_stream::iter(batches);
    let outcome = apply_batch_stream(
        &store,
        stream,
        StreamSettings {
            batch_size: 100,
            flush_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(60),
            rotation_deadline: None,
        },
        None,
    )
    .await
    .unwrap();

    assert_eq!(outcome.before_offset, None);
    assert_eq!(outcome.after_offset, Some(250));
    assert_eq!(count(&store, "transaction").await, 250);
    assert_eq!(count(&store, "exercised").await, 250);
    assert_eq!(count(&store, "created").await, 250);
    assert_eq!(count(&store, "action").await, 250);
    assert_eq!(count(&store, "requested").await, 250);
}

#[tokio::test]
async fn reconnects_after_server_restart() {
    let bin = require_surreal!();
    let mut server = TestServer::start(&bin).await;
    let store = server.unbounded_store().await;
    write_fixture(&store).await;

    // The server goes away while the app has data to write.
    server.stop();
    let writer = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .write_batch(&model::update_to_batch(&update(transfer_tx(20))))
                .await
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!writer.is_finished(), "write must wait for the server");

    // It comes back (empty, since it is in-memory; an admin recreates the user).
    server.restart().await;
    tokio::time::timeout(Duration::from_secs(90), writer)
        .await
        .expect("write did not finish after restart")
        .unwrap()
        .unwrap();

    assert_eq!(count(&store, "transaction").await, 1);
    assert_eq!(store.last_processed_offset().await.unwrap(), Some(20));
}

#[tokio::test]
async fn failed_writes_are_reported_when_retries_are_limited() {
    let bin = require_surreal!();
    let mut server = TestServer::start(&bin).await;
    let store = server
        .unbounded_store()
        .await
        .with_max_write_attempts(2)
        .with_attempt_timeout(Duration::from_secs(2));
    server.stop();
    let err = store
        .write_batch(&model::update_to_batch(&update(transfer_tx(1))))
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("failed after 2 attempts"),
        "{err:#}"
    );
}

#[tokio::test]
async fn reset_and_clear() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;
    write_fixture(&store).await;

    store.reset_acs_state().await.unwrap();
    assert!(store.read_acs_marker().await.unwrap().is_none());
    assert_eq!(count(&store, "created").await, 1); // only the streamed one
    assert_eq!(count(&store, "target").await, 0); // edges to the ACS contract went with it

    store.clear_all().await.unwrap();
    for table in [
        "transaction",
        "created",
        "exercised",
        "party",
        "action",
        "sync_state",
    ] {
        assert_eq!(count(&store, table).await, 0, "{table}");
    }
    assert_eq!(store.last_processed_offset().await.unwrap(), None);
}

#[tokio::test]
async fn offset_indexes_are_built_before_writing_to_an_existing_ledger() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;

    // A ledger synced before these indexes existed.
    let root = any::connect(server.url()).await.unwrap();
    root.signin(Root {
        username: "root".into(),
        password: "root".into(),
    })
    .await
    .unwrap();
    root.use_ns(NS).use_db(DB).await.unwrap();
    let mut r = root
        .query(
            "FOR $i IN 0..30000 { CREATE type::record('exercised', [$i, 0]) SET offset = $i, node_id = 0; };
             FOR $i IN 0..30000 { CREATE type::record('transaction', $i) SET offset = $i; };",
        )
        .await
        .unwrap();
    assert!(r.take_errors().is_empty());

    // Connecting builds the indexes in the background and waits for them, so
    // the writes that follow don't race the build.
    let store = server.store().await;
    for (index, table) in BACKGROUND_INDEXES {
        let info = query_value(&store, &format!("INFO FOR INDEX {index} ON {table};")).await;
        let status = match &info {
            Value::Object(o) => match o.get("building") {
                Some(Value::Object(b)) => b.get("status").and_then(first_string),
                _ => None,
            },
            _ => None,
        };
        assert_eq!(status.as_deref(), Some("ready"), "{index}: {info:?}");
    }
    write_fixture(&store).await;
    store.clear_all().await.unwrap();
    assert_eq!(count(&store, "exercised").await, 0);
}

#[tokio::test]
async fn offsets_are_indexed_for_readers() {
    let bin = require_surreal!();
    let server = TestServer::start(&bin).await;
    let store = server.store().await;
    write_fixture(&store).await;

    // Readers load the newest transactions and the events in an offset range;
    // with these indexes they don't scan the whole table.
    for (table, index) in [
        ("transaction", "transaction_offset"),
        ("exercised", "exercised_offset"),
    ] {
        let info = query_value(&store, &format!("INFO FOR TABLE {table};")).await;
        let Value::Object(info) = info else {
            panic!("unexpected info {info:?}")
        };
        let Some(Value::Object(indexes)) = info.get("indexes") else {
            panic!("no indexes in {info:?}")
        };
        assert!(
            indexes.contains_key(index),
            "{table} has no {index}: {indexes:?}"
        );
    }
}
