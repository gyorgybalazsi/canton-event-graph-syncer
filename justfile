# Load a local, gitignored `.env` if present (values, or SECRETS_RUNNER/OP_ACCOUNT).
set dotenv-load

# Command prefix that injects secrets into recipes that need them.
# Default: 1Password, resolving the op:// references in secrets.env.1password.
# Set OP_ACCOUNT to pin a 1Password account (e.g. my.1password.com) when
# several are signed in. Set SECRETS_RUNNER to use something else, or to an
# empty value when the variables are already in the environment or `.env`.
op_account_flag := if env("OP_ACCOUNT", "") == "" { "" } else { "--account " + env("OP_ACCOUNT", "") + " " }
secrets_runner := env("SECRETS_RUNNER", "op run " + op_account_flag + "--env-file=secrets.env.1password --")

# List available recipes
default:
    @just --list

# Run the sync with Keycloak authentication (release mode)
sync-run:
    {{secrets_runner}} \
      cargo run --release -p syncer -- sync --use-keycloak 2>&1 | tee sync.log

# Fresh start: delete the synced data in SurrealDB, load current ACS, and stream from ledger end
sync-fresh:
    {{secrets_runner}} \
      cargo run --release -p syncer -- sync --use-keycloak --fresh 2>&1 | tee sync.log

# Run the sync against local sandbox (no Keycloak, fake JWT)
sync-sandbox:
    {{secrets_runner}} \
      cargo run --release -p syncer -- sync --profile local 2>&1 | tee sync.log

# Fresh start against local sandbox (no Keycloak)
sync-sandbox-fresh:
    {{secrets_runner}} \
      cargo run --release -p syncer -- sync --profile local --fresh 2>&1 | tee sync.log

# Stop the sync
sync-stop:
    pkill -x syncer || true

# Open the optional desktop window to start and stop the sync (builds both programs)
sync-ui:
    cargo build --release -p syncer -p syncer-ui
    {{secrets_runner}} target/release/syncer-ui

# Look up a single update (transaction) by its ID
get-update update_id profile="mainnet":
    {{secrets_runner}} \
      cargo run -p client -- get-update-by-id --profile {{profile}} --update-id {{update_id}}

# --- Integration tests ---

# Build every example DAR in _daml/ (the sandbox tests need them)
build-dars:
    cd _daml && dpm build --all

# Run the sandbox integration tests (test, client, submit). They share the
# sandbox ports 6865-6869, so they run one at a time.
test-sandbox: build-dars
    cargo test --no-fail-fast -p daml-model-rep-examples -p client -p submit -- --test-threads=1

# Run the nested-test integration test (starts sandbox, creates contract, exercises choice)
test-nested:
    cd _daml/daml-nested-test && dpm build --all
    cargo test -p daml-model-rep-examples nested_test -- --nocapture

# --- Sandbox init recipes ---
# Each builds the DAML packages and runs sandbox-init

sandbox-init-asset:
    cd _daml/daml-asset && dpm build --all
    cargo run --bin sandbox-init -- \
        --dar _daml/daml-asset/main/.daml/dist/daml-asset-0.0.1.dar \
        --init-dar _daml/daml-asset/test/.daml/dist/daml-asset-test-0.0.1.dar \
        --init-script-name "Test:setup"

sandbox-init-interface:
    cd _daml/daml-interface-example && dpm build --all
    cargo run --bin sandbox-init -- \
        --dar _daml/daml-interface-example/main/.daml/dist/daml-interface-example-main-1.0.0.dar \
        --init-dar _daml/daml-interface-example/test/.daml/dist/daml-interface-example-test-1.0.0.dar \
        --init-script-name "Test:test"

sandbox-init-optional:
    cd _daml/daml-optional && dpm build --all
    cargo run --bin sandbox-init -- \
        --dar _daml/daml-optional/main/.daml/dist/daml-optional-0.0.1.dar \
        --init-dar _daml/daml-optional/test/.daml/dist/daml-optional-test-0.0.1.dar \
        --init-script-name "Test:setup"

sandbox-init-ticketoffer:
    cd _daml/daml-ticketoffer && dpm build --all
    cargo run --bin sandbox-init -- \
        --dar _daml/daml-ticketoffer/main/.daml/dist/daml-ticketoffer-0.0.1.dar \
        --init-dar _daml/daml-ticketoffer/test/.daml/dist/daml-ticketoffer-test-0.0.1.dar \
        --init-script-name "Test:setup"

sandbox-init-ticketoffer-explicit-disclosure:
    cd _daml/daml-ticketoffer-explicit-disclosure && dpm build --all
    cargo run --bin sandbox-init -- \
        --dar _daml/daml-ticketoffer-explicit-disclosure/main/.daml/dist/daml-ticketoffer-explicit-disclosure-0.0.1.dar \
        --init-dar _daml/daml-ticketoffer-explicit-disclosure/test/.daml/dist/daml-ticketoffer-explicit-disclosure-test-0.0.1.dar \
        --init-script-name "Setup:setup"

sandbox-init-full:
    cd _daml/full && dpm build --all
    cargo run --bin sandbox-init -- \
        --dar _daml/full/main/.daml/dist/full-0.0.1.dar \
        --init-dar _daml/full/test/.daml/dist/full-test-0.0.1.dar \
        --init-script-name "Test:setup"

# --- Analytics (canton-analytics daemon, see analytics/README.md) ---

# Build the example analytics plugins (WebAssembly) and copy them to plugins/
analytics-plugins:
    rustup target add wasm32-wasip2
    cd analytics/plugins/contract-lifetimes && cargo build --release --target wasm32-wasip2 --target-dir target
    cd analytics/plugins/party-activity && cargo build --release --target wasm32-wasip2 --target-dir target
    mkdir -p plugins
    cp analytics/plugins/*/target/wasm32-wasip2/release/*.wasm plugins/

# Write demo Delta tables from the built-in Canton sample (no ledger or server needed)
analytics-demo tables="demo-tables": analytics-plugins
    rm -rf {{tables}}
    cargo run -p canton-analytics --example demo_table -- {{tables}} plugins/*.wasm
    @echo "Dashboard location: $(cd {{tables}} && pwd)"

# Open the explorer's dashboard on a tables folder, with no database server (clones the explorer, or give your own checkout's path)
analytics-dashboard tables="demo-tables" explorer="" branch="main":
    #!/usr/bin/env bash
    set -euo pipefail
    tables="$(cd "{{tables}}" && pwd)"
    dir="{{explorer}}"
    if [ -z "$dir" ]; then
        dir=target/surrealdb-explorer
        if [ -d "$dir/.git" ]; then
            git -C "$dir" fetch --depth 1 origin "{{branch}}"
            git -C "$dir" checkout -q -B "{{branch}}" FETCH_HEAD
        else
            git clone --depth 1 --branch "{{branch}}" https://github.com/gyorgybalazsi/surrealdb-explorer "$dir"
        fi
    fi
    (cd "$dir" && cargo build --release --features dashboard)
    explorer="$(cd "$dir" && pwd)/target/release/surrealdb-explorer"
    # Its own folder and settings: no database server (the dashboard reads the
    # tables directly), no MCP server, and the tables already filled in.
    run=target/analytics-dashboard
    mkdir -p "$run"
    cat > "$run/config.toml" <<EOF
    [database]
    server = ""
    username = ""

    [mcp]
    enabled = false

    [analytics]
    location = "$tables"
    EOF
    cd "$run" && exec "$explorer"

# Run the daemon over the syncer's SurrealDB (SURREAL_* settings from the environment or .env)
analytics-run tables="tables": analytics-plugins
    {{secrets_runner}} \
      cargo run --release -p canton-analytics -- run --plugins-dir plugins --tables {{tables}}
