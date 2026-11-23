//! Runs WASM analytics plugins over a SurrealDB database and writes their
//! output to Delta Lake tables, which DataFusion (or DuckDB, Spark…) reads.
//!
//! - [`plugin`] loads a plugin component and calls it, in a sandbox.
//! - [`source`] connects to SurrealDB and runs the plugin's queries;
//!   [`fetch`] shares those reads between plugins.
//! - [`convert`] turns query results into Arrow, with the plugin's columns.
//! - [`table`] writes to the Delta table: commits, saved states, merges,
//!   compaction and vacuum.
//! - [`runner`] ties these together for one plugin.
//!
//! Reading the tables is in the `canton-analytics-reader` crate, so the
//! explorer can use it without wasmtime.

pub mod convert;
pub mod fetch;
pub mod plugin;
pub mod runner;
pub mod source;
pub mod table;
