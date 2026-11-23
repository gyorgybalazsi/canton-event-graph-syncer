//! Loading and calling an analytics plugin: a WebAssembly component that
//! exports the `canton:analytics/plugin` interface (see `wit/analytics.wit`).
//!
//! The plugin runs in a sandbox: no preopened folders, no environment
//! variables, no network, a memory limit, and a fuel budget per call so an
//! endless loop stops instead of hanging the runner.

use std::path::Path;

use analytics_reader::manifest::{self, Manifest};
use anyhow::{Result, anyhow};
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::arrow::record_batch::RecordBatch;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::convert::{from_ipc, to_ipc};

wasmtime::component::bindgen!({
    world: "analytics-plugin",
    path: "../wit",
});

use exports::canton::analytics::plugin::{
    ColumnFormat, Panel, PanelKind, PluginInfo, QuerySource, ValueFormat, WriteMode,
};

/// Most memory a plugin may use.
const MEMORY_LIMIT: usize = 1 << 30;
/// Fuel for one call, roughly one unit per WebAssembly instruction.
const FUEL_PER_CALL: u64 = 50_000_000_000;

/// The engine plugins are compiled with. One per runner, shared by all its
/// plugins.
pub fn engine() -> Result<Engine> {
    let mut config = Config::new();
    config.consume_fuel(true);
    // Compiled code is cached on disk, so a restart doesn't recompile.
    match wasmtime::Cache::from_file(None) {
        Ok(cache) => {
            config.cache(Some(cache));
        }
        Err(e) => tracing::debug!(error = %e, "no wasmtime compilation cache"),
    }
    Ok(Engine::new(&config)?)
}

struct Host {
    wasi: WasiCtx,
    table: ResourceTable,
    limits: StoreLimits,
}

impl WasiView for Host {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// A loaded plugin, ready to call. It holds its state in its own memory.
pub struct Plugin {
    engine: Engine,
    component: Component,
    linker: Linker<Host>,
    store: Store<Host>,
    bindings: AnalyticsPlugin,
    pub info: PluginInfo,
    pub query: String,
    pub high_water: String,
    /// The columns read from each statement's rows.
    pub inputs: Vec<SchemaRef>,
    pub schema: SchemaRef,
    pub manifest: Manifest,
}

impl Plugin {
    pub fn load(engine: &Engine, path: &Path) -> Result<Plugin> {
        let component = Component::from_file(engine, path).map_err(|e| {
            anyhow::Error::from(e).context(format!("could not load the plugin {}", path.display()))
        })?;

        let mut linker = Linker::new(engine);
        // The standard library of a wasm32-wasip2 program imports WASI; the
        // context below grants it nothing beyond stderr, clocks and random
        // numbers.
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        let (mut store, bindings) = instantiate(engine, &component, &linker)?;

        let api = bindings.canton_analytics_plugin();
        let info = api.call_info(&mut store)?;
        let QuerySource {
            query,
            inputs,
            high_water,
        } = api.call_source(&mut store)?;
        let inputs = inputs
            .iter()
            .map(|i| Ok(from_ipc(i)?.0))
            .collect::<Result<Vec<_>>>()?;
        let schema = from_ipc(&api.call_schema(&mut store)?)?.0;
        let mode = match api.call_mode(&mut store)? {
            WriteMode::Append => manifest::Mode::Append,
            WriteMode::Merge(keys) => {
                for k in &keys {
                    if schema.field_with_name(k).is_err() {
                        return Err(anyhow!(
                            "the plugin {} merges on {k}, which isn't one of its columns",
                            info.name
                        ));
                    }
                }
                if keys.is_empty() {
                    return Err(anyhow!("the plugin {} merges on no columns", info.name));
                }
                manifest::Mode::Merge(keys)
            }
        };
        let panels = api
            .call_dashboard(&mut store)?
            .into_iter()
            .map(panel)
            .collect();
        let manifest = Manifest {
            name: info.name.clone(),
            version: info.version.clone(),
            description: info.description.clone(),
            mode,
            panels,
        };
        Ok(Plugin {
            engine: engine.clone(),
            component,
            linker,
            store,
            bindings,
            info,
            query,
            high_water,
            inputs,
            schema,
            manifest,
        })
    }

    /// Throws the instance away and starts a new one, with no state: after
    /// a failed call, whose half-done work may be in the old one's memory.
    pub fn reset(&mut self) -> Result<()> {
        let (store, bindings) = instantiate(&self.engine, &self.component, &self.linker)?;
        self.store = store;
        self.bindings = bindings;
        Ok(())
    }

    pub fn load_state(&mut self, state: &[u8]) -> Result<()> {
        self.store.set_fuel(FUEL_PER_CALL)?;
        let api = self.bindings.canton_analytics_plugin();
        api.call_load_state(&mut self.store, state)?.map_err(|e| {
            anyhow!(
                "the plugin {} could not load its state: {e}",
                self.info.name
            )
        })
    }

    pub fn save_state(&mut self) -> Result<Vec<u8>> {
        self.store.set_fuel(FUEL_PER_CALL)?;
        let api = self.bindings.canton_analytics_plugin();
        Ok(api.call_save_state(&mut self.store)?)
    }

    /// Calls the plugin's `transform` with one Arrow batch per statement,
    /// and returns its rows.
    pub fn transform(&mut self, results: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
        let results = results.iter().map(to_ipc).collect::<Result<Vec<_>>>()?;
        self.store.set_fuel(FUEL_PER_CALL)?;
        let api = self.bindings.canton_analytics_plugin();
        let rows = api
            .call_transform(&mut self.store, &results)?
            .map_err(|e| anyhow!("the plugin {} failed: {e}", self.info.name))?;
        let (schema, batches) = from_ipc(&rows)?;
        if schema.fields() != self.schema.fields() {
            return Err(anyhow!(
                "the plugin {} returned rows that don't match its schema:\n  got {schema:?}\n  expected {:?}",
                self.info.name,
                self.schema
            ));
        }
        Ok(batches)
    }
}

fn instantiate(
    engine: &Engine,
    component: &Component,
    linker: &Linker<Host>,
) -> Result<(Store<Host>, AnalyticsPlugin)> {
    let host = Host {
        wasi: WasiCtx::builder().inherit_stderr().build(),
        table: ResourceTable::new(),
        limits: StoreLimitsBuilder::new().memory_size(MEMORY_LIMIT).build(),
    };
    let mut store = Store::new(engine, host);
    store.limiter(|h| &mut h.limits);
    store.set_fuel(FUEL_PER_CALL)?;
    let bindings = AnalyticsPlugin::instantiate(&mut store, component, linker)?;
    Ok((store, bindings))
}

fn panel(p: Panel) -> manifest::Panel {
    manifest::Panel {
        title: p.title,
        kind: match p.kind {
            PanelKind::Stats => manifest::PanelKind::Stats,
            PanelKind::Timeseries => manifest::PanelKind::Timeseries,
            PanelKind::Bar => manifest::PanelKind::Bar,
            PanelKind::Table => manifest::PanelKind::Table,
        },
        sql: p.sql,
        time_column: p.time_column,
        formats: p
            .formats
            .into_iter()
            .map(|ColumnFormat { column, format }| {
                let format = match format {
                    ValueFormat::Auto => manifest::Format::Auto,
                    ValueFormat::Number => manifest::Format::Number,
                    ValueFormat::DurationSeconds => manifest::Format::DurationSeconds,
                    ValueFormat::Timestamp => manifest::Format::Timestamp,
                    ValueFormat::Id => manifest::Format::Id,
                };
                (column, format)
            })
            .collect(),
    }
}
