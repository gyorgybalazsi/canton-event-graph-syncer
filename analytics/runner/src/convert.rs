//! SurrealDB query results to Arrow, with the columns a plugin declared.
//!
//! This runs in the host, natively, straight from SurrealDB's values: the
//! plugin gets columnar data it can read without parsing anything.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use deltalake::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder,
    StringBuilder, TimestampMicrosecondBuilder, make_builder,
};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{DataType, Field, SchemaRef};
use deltalake::arrow::ipc::reader::StreamReader;
use deltalake::arrow::ipc::writer::StreamWriter;
use deltalake::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use surrealdb::types::{Number, Value};

/// The rows of one statement: a list of objects, or a single object. Any
/// other result (NONE from `LET`, a number…) has no rows.
pub fn rows(value: &Value) -> Vec<&Value> {
    match value {
        Value::Array(rows) => rows.iter().collect(),
        Value::Object(_) => vec![value],
        _ => Vec::new(),
    }
}

/// Builds one batch with `schema`'s columns from `rows`, reading each field
/// by name. A missing or NONE field is null; a value of the wrong kind is
/// converted where that is lossless (an integer for a float column, a
/// datetime string for a timestamp column) and is an error otherwise.
pub fn to_batch(schema: &SchemaRef, rows: &[&Value]) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(column(field, rows).map_err(|e| anyhow!("column {}: {e}", field.name()))?);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &options,
    )?)
}

fn field<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    match row {
        Value::Object(o) => match o.get(name)? {
            Value::None | Value::Null => None,
            v => Some(v),
        },
        _ => None,
    }
}

fn column(field: &Field, rows: &[&Value]) -> Result<ArrayRef> {
    let name = field.name();
    let values = rows.iter().map(|r| self::field(r, name));
    let array: ArrayRef = match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for v in values {
                b.append_option(v.map(text));
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for v in values {
                b.append_option(v.map(boolean).transpose()?);
            }
            Arc::new(b.finish())
        }
        t if t.is_integer() => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for v in values {
                b.append_option(v.map(integer).transpose()?);
            }
            Arc::new(b.finish())
        }
        t if t.is_floating() => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for v in values {
                b.append_option(v.map(float).transpose()?);
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(_, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len())
                .with_timezone_opt(tz.clone());
            for v in values {
                b.append_option(v.map(micros).transpose()?);
            }
            Arc::new(b.finish())
        }
        DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {
            let mut b = ListBuilder::new(StringBuilder::new()).with_field(item.clone());
            for v in values {
                match v {
                    Some(Value::Array(items)) => {
                        for item in items.iter() {
                            match item {
                                Value::None | Value::Null => b.values().append_null(),
                                item => b.values().append_value(text(item)),
                            }
                        }
                        b.append(true);
                    }
                    Some(other) => {
                        b.values().append_value(text(other));
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
        other => {
            // Anything else: as text (JSON for objects and lists), then cast.
            let mut b = make_builder(&DataType::Utf8, rows.len());
            let s = b
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("a string builder");
            for v in values {
                s.append_option(v.map(text));
            }
            cast(&b.finish(), other).map_err(|e| anyhow!("can't read {other} values: {e}"))?
        }
    };
    // Narrower declared types (Int32, Float32, Timestamp in seconds…) are
    // built wide and cast down.
    if array.data_type() != field.data_type() {
        return Ok(cast(&array, field.data_type())?);
    }
    Ok(array)
}

/// A value as text: strings as they are, record ids as `table:key`, other
/// values as SurrealDB writes them in JSON.
fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => match other.clone().into_json_value() {
            serde_json::Value::String(s) => s,
            json => json.to_string(),
        },
    }
}

fn boolean(v: &Value) -> Result<bool> {
    match v {
        Value::Bool(b) => Ok(*b),
        other => Err(anyhow!("expected a boolean, got {}", text(other))),
    }
}

fn integer(v: &Value) -> Result<i64> {
    match v {
        Value::Number(Number::Float(f)) if f.fract() == 0.0 => Ok(*f as i64),
        Value::Number(n) => n
            .to_int()
            .ok_or_else(|| anyhow!("{} is not a whole number", text(v))),
        Value::String(s) => s
            .parse()
            .map_err(|_| anyhow!("expected an integer, got {s:?}")),
        other => Err(anyhow!("expected an integer, got {}", text(other))),
    }
}

fn float(v: &Value) -> Result<f64> {
    match v {
        Value::Number(n) => n
            .to_f64()
            .ok_or_else(|| anyhow!("{} is not a number", text(v))),
        Value::String(s) => s
            .parse()
            .map_err(|_| anyhow!("expected a number, got {s:?}")),
        other => Err(anyhow!("expected a number, got {}", text(other))),
    }
}

/// Microseconds since the epoch, from a datetime or an RFC 3339 string.
fn micros(v: &Value) -> Result<i64> {
    match v {
        Value::Datetime(d) => Ok(d.timestamp_micros()),
        Value::String(s) => {
            let s = s
                .strip_prefix("d'")
                .and_then(|s| s.strip_suffix('\''))
                .unwrap_or(s);
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|t| t.timestamp_micros())
                .map_err(|e| anyhow!("{s:?} is not a datetime: {e}"))
        }
        Value::Number(n) => n
            .to_int()
            .ok_or_else(|| anyhow!("{} is not a timestamp", text(v))),
        other => Err(anyhow!("expected a datetime, got {}", text(other))),
    }
}

pub fn to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(out)
}

pub fn from_ipc(bytes: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader =
        StreamReader::try_new(bytes, None).map_err(|e| anyhow!("bad Arrow IPC data: {e}"))?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use deltalake::arrow::array::{Array, AsArray};
    use deltalake::arrow::datatypes::{Int64Type, Schema, TimeUnit, TimestampMicrosecondType};

    use super::*;

    fn value(json: serde_json::Value) -> Value {
        Value::from_t(json)
    }

    #[test]
    fn reads_fields_by_name_with_conversions() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("offset", DataType::Int64, true),
            Field::new("at", ts, true),
            Field::new(
                "parties",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("amount", DataType::Float64, true),
        ]));
        let rows = value(serde_json::json!([
            { "id": "a", "offset": 3, "at": "2026-11-02T09:00:00Z", "parties": ["p", "q"], "amount": 2 },
            { "id": "b", "extra": true },
        ]));
        let batch = to_batch(&schema, &self::rows(&rows)).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 3);
        assert!(batch.column(1).is_null(1));
        assert_eq!(
            batch
                .column(2)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            1_793_610_000_000_000
        );
        assert_eq!(batch.column(3).as_list::<i32>().value(0).len(), 2);
        let (_, back) = from_ipc(&to_ipc(&batch).unwrap()).unwrap();
        assert_eq!(back[0], batch);
    }

    #[test]
    fn a_result_without_rows_is_empty() {
        let schema = Arc::new(Schema::empty());
        let batch = to_batch(&schema, &rows(&Value::None)).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
