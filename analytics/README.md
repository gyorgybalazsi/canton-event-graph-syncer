# Canton analytics

Analytics plugins written as WebAssembly components. The `canton-analytics`
daemon runs them over the SurrealDB that the [syncer](../syncer) fills, and
writes what they produce to Delta Lake tables. Anything that reads Delta can
query the tables; the [SurrealDB explorer](https://github.com/gyorgybalazsi/surrealdb-explorer)
shows them as dashboards, through the `reader` crate here.

```
Canton ──▶ syncer ──▶ SurrealDB ──query($from, $to)──▶ canton-analytics ──Arrow──▶ plugin.wasm
                                                         │        ▲                    │
                                                         │        └──── Arrow rows ────┘
                                                         ▼
                                     Delta tables (a folder, or S3 / Azure / Google Cloud Storage)
                                                         │
                                           DataFusion SQL (reader crate)
                                                         │
                                   explorer dashboard · canton-analytics sql · any Delta reader
```

The syncer and the analytics daemon are separate programs. Both run all the
time; the explorer only reads, whenever someone opens it.

## The parts

| Folder | What it is |
|---|---|
| `wit/analytics.wit` | The plugin interface, `canton:analytics@0.2.0`. |
| `runner` | The `canton-analytics` program: the daemon (`run`) and tools (`sql`, `dashboard`, `optimize`, `vacuum`). Uses wasmtime and SurrealDB. |
| `reader` | Reads the tables with DataFusion: SQL, the table catalog, dashboard panels, the daemon's heartbeat. No wasmtime, no SurrealDB, so the explorer can use it. |
| `plugins/contract-lifetimes` | Example plugin, **append** mode: one row per archived contract, with its template, create and archive offsets and times, lifetime, and the archiving choice. |
| `plugins/party-activity` | Example plugin, **merge** mode: one row per party, with contracts signed, choices exercised, first and last activity. Rows are upserted on `party`. |
| `deploy` | A systemd unit, a launchd agent and an environment file for running the daemon as a service. |

The runner and reader are workspace members but not default members, so a
plain `cargo build` skips them; `--workspace` (and CI) builds them. The
plugins are their own little workspaces, built for `wasm32-wasip2`.

## Quick start (no ledger needed)

```bash
rustup target add wasm32-wasip2
(cd analytics/plugins/contract-lifetimes && cargo build --release --target wasm32-wasip2)
(cd analytics/plugins/party-activity && cargo build --release --target wasm32-wasip2)
mkdir -p plugins && cp analytics/plugins/*/target/wasm32-wasip2/release/*.wasm plugins/

# Demo tables from a built-in sample, in an in-memory SurrealDB:
cargo run -p canton-analytics --example demo_table -- tables plugins/*.wasm
cargo run -p canton-analytics -- dashboard --table tables/contract-lifetimes
cargo run -p canton-analytics -- sql --table tables/party-activity \
    "SELECT party, contracts_signed, choices_exercised FROM party_activity ORDER BY party"
```

Against the syncer's database:

```bash
SURREAL_URL=ws://127.0.0.1:8000 SURREAL_NAMESPACE=canton SURREAL_DATABASE=event_graph \
SURREAL_USER=analytics SURREAL_PASSWORD=… \
cargo run --release -p canton-analytics -- run --plugins-dir plugins --tables tables
```

`run` catches up, then keeps following the database until Ctrl-C or
SIGTERM, when it commits what it has and saves the plugins' state. `--once`
stops once caught up. A read-only (VIEWER) SurrealDB user is enough.

## Where the tables go

`--tables` (or `CANTON_ANALYTICS_TABLES`) is a folder or an object store URL.
Each plugin writes to a sub-folder named after it. The daemon and the
explorer are configured separately, so testing can use a local folder and
production a bucket.

| Storage | `--tables` | Credentials (the cloud's usual environment variables) |
|---|---|---|
| Local folder | `tables`, `/var/lib/canton-analytics/tables` | none |
| Amazon S3 | `s3://bucket/canton-analytics` | `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (or an instance role, `AWS_PROFILE`) |
| S3-compatible (MinIO…) | `s3://bucket/canton-analytics` | as S3, plus `--storage-option aws_endpoint=http://minio:9000 --storage-option aws_allow_http=true` |
| Azure Blob / ADLS | `az://container/canton-analytics` | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY` (or a service principal) |
| Google Cloud Storage | `gs://bucket/canton-analytics` | `GOOGLE_APPLICATION_CREDENTIALS` |

`--storage-option key=value` passes any other
[object store option](https://docs.rs/object_store/latest/object_store/). The
runner is built with all three clouds by default; the reader has `s3`, `azure`
and `gcs` features (or `cloud` for all three), so the explorer only pulls in
the ones it needs.

On S3, Delta needs a way to commit safely when several writers share a
table. One daemon per table is the rule here, so the default is fine; to run
two on purpose, see delta-rs' S3 locking options.

## How the daemon works

For each plugin:

1. **High water.** The plugin's high-water query says how far the database
   has got: the syncer's `sync_state:stream.last_offset`, so a half-written
   transaction is never read.
2. **Steps.** The plugin's query runs over the next offset range,
   `($from, $to]`, at most `--batch` offsets. Each statement's rows become an
   Arrow batch with the schema the plugin declared, and `transform` returns
   Arrow rows. The plugin keeps its own state (e.g. open contracts) in memory,
   so a step costs the size of the step, not of the state.
3. **Commits.** Rows wait in memory until there are `--min-rows` of them or
   the oldest has waited `--max-wait` seconds. Then one Delta commit writes the
   rows **and** the new cursor (as a Delta application transaction), so a
   crash can never write a row twice or lose one. A quiet ledger makes a few
   files an hour rather than one per poll.
4. **State.** Every `--state-commits` commits (or 10 minutes), the commit
   also saves the plugin's state, zstd-compressed, under
   `_canton_analytics/state/`. On start the daemon loads the newest state and
   replays the offsets from there to the last commit, throwing those rows
   away. After a failed call it does the same with a fresh instance.
5. **Upkeep.** After `--compact-after` small files the table is compacted
   into large files; old files are vacuumed hourly. The Delta log is
   checkpointed every 20 commits, so readers load a checkpoint and a few
   commits rather than the whole log.
6. **Heartbeat.** `_canton_analytics/status.json` says when the daemon last
   looked, how far it has committed, how far the database is, the commit
   thresholds in force, and the last error. The explorer shows it.
7. **Lag settings.** `_canton_analytics/tuning.json`, which the explorer's
   dashboard settings write, overrides `--every`, `--min-rows` and
   `--max-wait` for that table, for example
   `{"every_seconds": 2, "min_rows": 1000, "max_wait_seconds": 5}`. The
   daemon reads it before each check, so a change takes effect without a
   restart; a field left out (or no file) keeps the command-line value. On a
   quiet ledger a new row shows up after at most about `every + max-wait`
   seconds, plus the explorer's own check interval.

Plugins share the database: when several plugins ask for the same query and
range at once, it runs once. Each plugin has a fuel budget per call and a
1 GB memory limit, and a failing plugin pauses (with a growing wait) without
stopping the others. Compiled plugins are cached on disk, so a restart
doesn't recompile them.

## Writing a plugin

A plugin exports, from `wit/analytics.wit`:

- `info`: name, version, description. The name is the table's folder; the
  table's SQL name is the same with `-` as `_`.
- `schema`: its output columns, as an empty Arrow IPC stream.
- `source`: the SurrealQL query (one statement per input, using `$from` and
  `$to`), the Arrow schema of each statement's rows, and the high-water query.
- `mode`: `append`, or `merge(keys)` to upsert on key columns.
- `dashboard`: panels for the explorer, each a DataFusion SQL query and a
  kind: `stats`, `timeseries`, `bar` or `table`. A time series can use
  `{bucket}`, which the reader fills with an interval that suits the data's
  time span, e.g. `date_bin({bucket}, archived_at)`. Its x axis can also be
  a number column such as an offset: then `{bucket}` is a round width that
  gives fewer than 60 points, e.g. `"offset" - "offset" % {bucket}`. A panel
  can instead use `{period}`, a calendar day or week that the viewer picks
  with the dashboard's Day / Week switch, e.g.
  `date_bin({period}, day_start, TIMESTAMP '2001-01-01T00:00:00Z')` (that
  origin is a Monday, so weeks run Monday to Sunday). The reader times every
  panel. Columns can be formatted
  as numbers, durations, timestamps or ids. With no panels, the explorer makes
  some from the columns.
- `load-state` / `save-state`: the plugin's memory as bytes. Save the same
  bytes for the same state (sort maps), and keep `transform` deterministic: a
  replay must give the same state again.
- `transform`: one Arrow IPC stream per statement in, one out. Read all
  inputs before changing the state, so an error changes nothing.

Copy one of the example plugins to start. The
[canton-event-graph-demo](https://github.com/gyorgybalazsi/canton-event-graph-demo)
repo has plugins for its ticket model, and a benchmark of the dashboard over
a 10-million-offset ledger. A plugin can't reach the network
or the file system.

## Running it as a service

See [`deploy/`](deploy): `canton-analytics.service` for systemd (with
`canton-analytics.env.example`), and `com.canton.analytics.plist` for
launchd on a Mac. Both stop the daemon with SIGTERM and give it two minutes
to save.

## Tests

```bash
cargo test -p canton-analytics -p canton-analytics-reader
```

The end-to-end tests build both plugins, load a small Canton sample into an
in-memory SurrealDB and check: steps and commits, a restart that replays from
a saved state without writing rows twice, compaction, merge upserts, two
plugins sharing one database, the table catalog, the heartbeat, and every
dashboard panel. They need the `wasm32-wasip2` target.
