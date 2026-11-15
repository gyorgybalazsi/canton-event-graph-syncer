# syncer

Streams Daml/Canton ledger data into a [SurrealDB](https://surrealdb.com) server
as a graph: transactions, created contracts, exercised choices and parties,
linked by relations. It follows the ledger in real time and resumes where it
left off.

The syncer is one client of the database among others (a graph viewer,
SurrealDB Studio, ...). It connects over the network as a database user and
never opens a database folder directly.

## Supported Daml SDK

The Ledger API v2 bindings are built from the Canton 3.7.1 protos. The syncer was last tested end to end against SDK 3.7.1 (a local sandbox and SurrealDB 4.0).

## Features

- **Resilient sync**: reconnects to the ledger with exponential backoff; database
  writes are retried with a growing delay (1 s, 2 s, 5 s, … up to 30 s) while the
  server is down, keeping the data in memory instead of dropping it
- **Resume**: continues from the last offset stored in the database
- **ACS loading**: loads the Active Contract Set before streaming, so contracts
  used later already exist
- **JWT token management**: Keycloak OAuth2 (client credentials or password flow)
  with automatic token refresh
- **Fresh start**: deletes the synced data and starts from the current ledger end
- **Batched writes**: one database transaction per batch of updates, one
  statement per table
- **Topology**: party-to-participant hosting changes (topology transactions)
  are kept as a log, plus the current hosting state per party and participant.
  Turn off with `include_topology_events = false` in the ledger profile

## Data model

### Tables

| Table | Record id | Main fields |
|-------|-----------|-------------|
| `transaction` | `transaction:<offset>` | `name` (`TX@<offset>`), `offset`, `update_id`, `command_id`, `workflow_id`, `synchronizer_id`, `effective_at`, `record_time`, `trace_context { traceparent, tracestate }` |
| `created` | `created:<contract_id>` | `name` (`<Entity>@<offset>`), `contract_id`, `template_name`, `template { package_id, package_name, package, module_name, entity_name }`, `signatories [..]`, `offset`, `node_id`, `created_at`, `create_arguments { .. }`, `from_acs` |
| `exercised` | `exercised:[<offset>, <node_id>]` | `name` (`<Choice>@<offset>`), `choice_name`, `template { .. }`, `interface { .. }` (for interface choices), `target_contract_id`, `acting_parties [..]`, `consuming`, `result_contract_ids [..]`, `offset`, `node_id`, `last_descendant_node_id`, `transaction_effective_at`, `choice_argument { .. }` |
| `party` | `party:<party_id>` | `party_id`, `name` (the part before `::`) |
| `participant` | `participant:<participant_id>` | `participant_id`, `name` (`participant1` for `PAR::participant1::1220...`) |
| `topology_event` | `topology_event:[<offset>, <index>]` | `name` (`<kind>:<party>@<offset>`), `kind` (`added`, `changed`, `onboarding`, `revoked`), `party` and `participant` (record links), `permission` (`submission`, `confirmation`, `observation`), `offset`, `index`, `update_id`, `synchronizer_id`, `record_time` |
| `package` | `package:<package_id>` | `package_id`, `package_name`, `package_version` |
| `sync_state` | `sync_state:singleton`, `sync_state:stream` | ACS load marker (`acs_offset`, `acs_continuation_token`, `acs_complete`); last written offset (`last_offset`) |

Record ids come from the ledger, so replaying an update updates the same
records instead of adding new ones. Timestamps are SurrealQL datetimes.
Contracts loaded from the ACS have `offset = -1`, `node_id = 0` and
`from_acs = true`.

### Package names and versions

A package id changes with every version of a Daml package, so the syncer also
records which package and version each id is. `template.package_name` comes
with every event. `template.package` is a link to the `package:<package_id>`
record, which the syncer fills in by asking the ledger
(`PackageService.ListVettedPackages`) the first time it sees an id. No DARs or
generated code are involved. Only vetted packages are listed; if the ledger
doesn't list an id, or the lookup fails, the sync carries on and asks again
about ten minutes later.

```sql
-- All contracts of a template, across package versions
SELECT * FROM created
WHERE template.package_name = 'my-assets' AND template.entity_name = 'Asset';

-- Active contracts per package version
SELECT template.package.package_version AS version, count() AS active
FROM created WHERE template.package_name = 'my-assets' AND count(<-consumes) = 0
GROUP BY version;
```

### Relations

| Relation | From → To | Meaning |
|----------|-----------|---------|
| `action` | transaction → created / exercised | Root-level events of a transaction |
| `consequence` | exercised → created / exercised | Child events of an exercise |
| `target` | exercised → created | The contract the choice was exercised on |
| `consumes` | exercised → created | Contract archived by a consuming choice |
| `requested` | party → transaction | Party that submitted the transaction |
| `hosted_on` | party → participant | Current hosting: `permission`, `active` (false once revoked), `onboarding`, `since_offset`, `revoked_offset`, `last_offset` |

Each edge has a stable id (for example `target:[<offset>, <node_id>]`) and a
unique index on `in, out`, so it is written at most once. `target` and
`consumes` are only written when the contract is in the database.

Topology changes are a log, not a graph, so each change is a
`topology_event` record with record links to its party and participant
(`<~topology_event` from a party lists its history). Only the current state is
an edge: `hosted_on:[<party_id>, <participant_id>]` is updated by each change
in offset order, and a replayed older change is ignored. A revoked hosting
stays as an edge with `active = false`.

### Contract and choice arguments

`create_arguments` and `choice_argument` keep the shape of the Daml value:

| Daml | Stored as |
|------|-----------|
| Record | object |
| List | array |
| TextMap | object |
| GenMap | array of `{ key, value }` |
| Variant | `{ constructor, value }` |
| Enum | the constructor name |
| Optional | the value, or `null` |
| Timestamp / Date | datetime |
| Numeric | decimal |

So nested fields can be queried directly:

```sql
SELECT contract_id, create_arguments.owner
FROM created
WHERE create_arguments.address.city = 'Zurich';
```

### Indexes

Defined at startup with `DEFINE ... IF NOT EXISTS`: the unique `in, out`
indexes on every relation, plus `created(offset, node_id)`,
`created(template_name)`, `created(template.package_name)`, `created(from_acs)`, `exercised(choice_name)`,
`exercised(target_contract_id)` and `transaction(update_id)`.

## Prerequisites

### SurrealDB

Install SurrealDB 4 ([instructions](https://surrealdb.com/install)) and run a
server, for example with on-disk storage:

```bash
surreal start --user root --pass '<root password>' --bind 127.0.0.1:8000 surrealkv://data/event-graph
```

As root, create the namespace, the database and the syncer's user once (the
syncer itself never signs in as root):

```sql
DEFINE NAMESPACE canton; USE NS canton;
DEFINE DATABASE event_graph; USE DB event_graph;
DEFINE USER streamer ON DATABASE PASSWORD '<password>' ROLES EDITOR;
```

### Canton / Sandbox

You can connect to:
- Canton Sandbox (default settings)
- Canton 3.4+ via Docker
- Canton Network validator node with port forwarding

## Installation

```bash
cargo build --release -p syncer
```

## Configuration

Create `config/config.toml` (in this folder) from the example. From the
repository root:

```bash
cp syncer/config/config.toml.example syncer/config/config.toml
```

The `[surrealdb]` section holds the connection settings. Each one can be set (or
overridden) with an environment variable, which is the recommended way for the
password:

| Setting | Environment variable | Example |
|---------|----------------------|---------|
| `url` | `SURREAL_URL` | `ws://127.0.0.1:8000` |
| `namespace` | `SURREAL_NAMESPACE` | `canton` |
| `database` | `SURREAL_DATABASE` | `event_graph` |
| `username` | `SURREAL_USERNAME` | `streamer` |
| `password` | `SURREAL_PASSWORD` | — |

The password is never logged. See the root README ("Secrets") for providing it
through 1Password, a `.env` file or another secret manager.

Other `[surrealdb]` settings: `batch_size` (updates per database transaction,
default 100), `flush_timeout_secs` (default 1) and `idle_timeout_secs`
(default 60). Ledger and Keycloak settings live in named profiles; see
`config/config.toml.example`.

## Commands

### sync

Continuously syncs ledger updates to SurrealDB with automatic reconnection.

```bash
# Normal sync (resumes from the last offset in the database)
cargo run --release -p syncer -- sync --use-keycloak

# Fresh start (deletes the synced data, loads current ACS, streams from ledger end)
cargo run --release -p syncer -- sync --use-keycloak --fresh

# Fresh start from ~7 days ago (resolves the offset via the Ledger API, implies --fresh)
cargo run --release -p syncer -- sync --use-keycloak --from-days-ago 7
```

Options:
- `--config-file <path>`: Path to config.toml (default: `./config/config.toml`, then `./syncer/config/config.toml`)
- `--profile <name>`: Profile to use (overrides `active_profile`)
- `--use-keycloak`: Use Keycloak for JWT token management
- `--access-token <token>`: Provide a static JWT token
- `--fresh`: Delete the synced data and start from the current ledger end
- `--from-days-ago <N>`: Fresh start from the offset current ~N days ago. Implies `--fresh`.

`--fresh` deletes the records in the syncer's tables (with `DELETE`); the
tables and indexes stay defined, and nothing else in the database is touched.

### benchmark

Measures Canton stream throughput without writing to the database.

```bash
cargo run --release -p syncer -- benchmark --use-keycloak --count 10000
```

### print-surql

Prints the SurrealQL writes generated for each update (no database needed).

```bash
cargo run --release -p syncer -- print-surql \
  --access-token <token> \
  --party <party-id> \
  --url <ledger-url> \
  --begin-exclusive <offset>
```

## Using the justfile

```bash
just sync-run            # sync with Keycloak
just sync-fresh          # fresh start
just sync-sandbox        # sync against a local sandbox (fake JWT)
just sync-stop           # stop the sync process
```

## Querying the graph

```sql
-- Latest transactions requested by a party
SELECT ->requested->transaction.* AS txs FROM party:⟨alice::1220...⟩;

-- Exercises on a contract
SELECT choice_name, acting_parties, consuming
FROM exercised WHERE target_contract_id = '00...';

-- The event tree of a transaction
SELECT ->action->exercised->consequence->? AS children FROM transaction:12345;

-- Where a party is hosted now, and its hosting history
SELECT ->hosted_on[WHERE active]->participant.name AS hosted_on,
       <~topology_event.{ kind, permission, offset } AS history
FROM party:⟨alice::1220...⟩;

-- Active contracts per template (not consumed)
SELECT template_name, count() AS active FROM created
WHERE count(<-consumes) = 0 GROUP BY template_name;
```

## Tests

```bash
cargo test -p syncer
```

The integration tests in `tests/surreal_integration.rs` start a throwaway
in-memory server (`surreal start ... memory`) on a free port for each test, so
they never touch real data. They need the `surreal` program on the `PATH` (or
`SURREAL_BIN` pointing at it) and are skipped with a message otherwise.

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌──────────────┐
│  Canton Ledger  │───▶│   syncer     │───▶│  SurrealDB   │◀── graph viewer,
│  (gRPC Stream)  │    │              │ ws │  server      │    SurrealDB Studio
└─────────────────┘    └──────────────┘    └──────────────┘
        │                     │
        │                     ▼
        │              ┌──────────────┐
        └─────────────▶│  Keycloak    │
           (auth)      │  (OAuth2)    │
                       └──────────────┘
```

1. **Stream updates** from Canton's gRPC update stream
2. **Convert** each transaction into rows and edges (`model.rs`)
3. **Batch write** up to `batch_size` updates per database transaction (`store.rs`)
4. **Reconnect** on ledger or database errors, with backoff
5. **Refresh tokens** in the background before they expire
