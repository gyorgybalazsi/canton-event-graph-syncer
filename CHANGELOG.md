# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Analytics dashboards: `{period_name}` in a panel's query is `day` or
  `week`, as the Day / Week switch says, so column names can name the period
  a figure covers (`"last full {period_name}"`).

### Fixed
- The SurrealDB integration tests start their server with `surreal db start`,
  as SurrealDB 4 requires, and detect the `surreal` program with `--help`.
  With SurrealDB 4 the old `surreal version` check could make them skip
  themselves silently. The README's server command uses `surreal db start`
  too.

## [0.1.0] - 2026-11-23

The first tagged release of canton-event-graph-syncer. It covers everything
since the repository was published (2026-11-10).

### Added
- `syncer` stores each event's witness parties (`witness_parties` on `created`
  and `exercised` records), so a party's projection of a transaction is the set
  of nodes that list it. The syncer subscribes for all its parties at once, and
  used to lose which party sees which node.
- `syncer` records the package name and version of every contract and
  exercise: `template.package_name`, and a `template.package` link to a new
  `package` table (`package_name`, `package_version`), looked up from the
  Ledger API (`ListVettedPackages`, then `GetPackage` for unvetted packages).
- `syncer-ui`: an optional desktop window (iced) that starts and stops the
  `syncer` program and shows its log (`just sync-ui`). It is not in
  `default-members`, so a plain `cargo build` skips it.
- `syncer` indexes `transaction.offset` and `exercised.offset`. Readers that load the
  newest transactions or an offset range, such as surrealdb-explorer, no longer
  scan the whole table. On an existing ledger the indexes are built in the
  background, in batches; the syncer waits until they are ready before it
  writes, logging the progress.
- `syncer` now syncs topology transactions (party-to-participant hosting
  changes): a `topology_event` log with record links to `party` and the new
  `participant` table, plus a `hosted_on` edge holding each party's current
  hosting. `client::stream_updates` takes an `include_topology` flag; the
  syncer's `include_topology_events` profile setting defaults to true.

### Changed
- Repository renamed `rust-canton-toolbox` → `canton-event-graph-syncer`
- Renamed crate, folder and binary `event-graph-indexer` → `syncer`, and its
  justfile recipes `indexer-*` → `sync-*` (`sync-run`, `sync-fresh`,
  `sync-sandbox`, `sync-sandbox-fresh`, `sync-stop`). Its local config now lives in
  `syncer/config/config.toml`.
- Daml examples in `_daml/` and codegen's Daml-LF protos moved to SDK 3.7.1
  (`daml-nested-test` was on 3.4.11, the rest on 3.4.8)
- Vendored Ledger API v2 protos bumped from Canton 3.6.1 to 3.7.1 (adds
  `size_config_options.proto`, `account_id` traffic fields, and
  `include_transactions_and_reassignments` / `include_incomplete_reassignments`)
- ACS readers (`active_contracts`, `get_blob`) set
  `include_incomplete_reassignments = true` so incomplete (un)assigned entries
  are still returned, as before 3.7
- `event-graph-indexer` (now `syncer`) writes to a SurrealDB 4 server instead of Neo4j.
  Neo4j labels became tables (`transaction`, `created`, `exercised`, `party`,
  `sync_state`) and relationship types became relation tables (`action`,
  `consequence`, `target`, `consumes`, `requested`). Contract and choice
  arguments are stored as nested objects instead of flattened properties or
  JSON strings; party lists and result contract ids are arrays; timestamps are
  datetimes. See `syncer/README.md`.
- Config: the `[neo4j]` section is replaced by `[surrealdb]`
  (`url`, `namespace`, `database`, `username`, `password`), each overridable
  with a `SURREAL_*` environment variable. The syncer signs in as a database
  user, never as root.
- Database writes retry with a growing delay (1 s up to 30 s) while the server
  is down, keeping the batch in memory instead of failing.
- `print-cypher` is now `print-surql`.
- The syncer stops with an explanation, instead of retrying forever, when its
  start offset is past the ledger end (for example after a sandbox restart).
- `syncer` also finds `syncer/config/config.toml` when started from the
  repository root (e.g. `target/release/syncer`).
- The syncer stops with an error when SurrealDB refuses its login (wrong
  password, missing user, namespace or database) instead of retrying forever.
- `ledger-api` generates its bindings into Cargo's `OUT_DIR`; the generated
  `ledger-api/src/pb/*.rs` files are no longer checked in.
- Moved to tonic 0.14: `ledger-api` now depends on `tonic-prost` and
  `tonic-prost-build`, and prost / prost-types / prost-wkt moved to the matching
  0.14 / 0.7 releases.
- `client` no longer depends on `daml-type-rep` and `derive-lapi-access`.
- Dependency updates (Dependabot), including openssl 0.10.82, quinn-proto
  0.11.19, zip 9, syn 3, base64 0.23 and nix 0.31.

### Security
- Added `SECURITY.md`: report vulnerabilities privately through GitHub.
- The CI workflow's token is limited to `contents: read`.
- Dependabot opens weekly grouped updates for Cargo and GitHub Actions (but
  not for the pinned Rust toolchain action).

### Fixed
- `sandbox-init` keeps reading the sandbox's output, so the sandbox no longer
  dies of a broken pipe after start-up, and it exits when the sandbox does.
  It also stops the sandbox on SIGTERM (e.g. `kill`), not only on Ctrl+C.
- `client get-update-by-id` (`just get-update`) now fills in `${VAR}`
  placeholders in the Keycloak settings, like the syncer; it used to send the
  placeholder text as the secret.
- In `syncer-ui`, *Reload* now picks up edits to `.env` when the window was
  opened with `just sync-ui` (which had already loaded the old values).

### Removed
- `codegen` moved to its own repository, https://github.com/gyorgybalazsi/daml-rust-codegen, with its
  history. Its checked-in output for the example DARs now lives in
  `test/generated/`, and `client` no longer depends on it (it never used it).
- `event-graph-ui` (the Dioxus graph visualization).
- The `[storage]` config section (`flatten_arguments`, `flatten_max_depth`,
  `store_arguments_json`): arguments are always stored nested.
- The `neo4rs` dependency.
- `jwt_generator.nu` (use `cargo run -p client -- fake-access-token`) and
  `_daml/daml-ticketoffer-explicit-disclosure/start.sh` (use
  `just sandbox-init-ticketoffer-explicit-disclosure`).

## Before 0.1.0 (rust-canton-toolbox, 2026-07-01, never tagged)

This section was labelled 0.1.0 before the repository was published; no tag
was made for it, and the first tagged 0.1.0 is the release above.

### Added
- Dual-licensed under MIT OR Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`)
- Flatten create/choice arguments into dot-separated Neo4j node properties
  (e.g., `create_arg.person.homeAddress.city = "Zurich"`) for direct querying
- Configurable `[storage]` section in config.toml: `flatten_arguments`,
  `flatten_max_depth`, `store_arguments_json`
- Auto-discover parties from ledger when none configured (`ListKnownParties`)
- `store_arguments_json` flag to optionally store raw JSON blobs alongside
  flattened properties (default: false)

### Changed
- Renamed crate `ledger-explorer` → `event-graph-indexer` and `ledger-graph-ui`
  → `event-graph-ui` to better reflect the Canton event-graph functionality
  (binary, config-discovery paths, and `-p` package references updated to match)
- Renamed justfile recipes: `indexer-run`, `indexer-sandbox`,
  `indexer-fresh`, `indexer-sandbox-fresh`, `indexer-stop`
- `PrintCypher` and `Benchmark` commands now respect storage config instead of
  using hardcoded flatten settings

### Removed
- Removed the placeholder `wallet` crate (an empty `Hello, world!` stub with no
  dependents) from the workspace

[Unreleased]: https://github.com/gyorgybalazsi/canton-event-graph-syncer/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/gyorgybalazsi/canton-event-graph-syncer/releases/tag/v0.1.0
