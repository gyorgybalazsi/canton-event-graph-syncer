//! Query results as plain Rust values, so drawing them never touches Arrow.

use anyhow::{Result, anyhow};
use deltalake::arrow::array::{Array, ArrayRef, AsArray};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{
    DataType, Float64Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use deltalake::arrow::record_batch::RecordBatch;

use crate::manifest::Format;

#[derive(Debug, Clone, PartialEq)]
pub enum Values {
    Number(Vec<Option<f64>>),
    /// Microseconds since the epoch, UTC.
    Time(Vec<Option<i64>>),
    Text(Vec<Option<String>>),
}

impl Values {
    pub fn len(&self) -> usize {
        match self {
            Values::Number(v) => v.len(),
            Values::Time(v) => v.len(),
            Values::Text(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A number for charts: numbers as they are, times as seconds.
    pub fn number(&self, row: usize) -> Option<f64> {
        match self {
            Values::Number(v) => v[row],
            Values::Time(v) => v[row].map(|us| us as f64 / 1e6),
            Values::Text(v) => v[row].as_deref().and_then(|s| s.parse().ok()),
        }
    }

    pub fn text(&self, row: usize) -> Option<String> {
        match self {
            Values::Text(v) => v[row].clone(),
            Values::Number(v) => v[row].map(|n| n.to_string()),
            Values::Time(v) => v[row].map(|us| us.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub format: Format,
    pub values: Values,
}

impl Column {
    /// The format to draw with: the declared one, or one from the type.
    pub fn effective_format(&self) -> Format {
        match (self.format, &self.values) {
            (Format::Auto, Values::Time(_)) => Format::Timestamp,
            (Format::Auto, Values::Number(_)) => Format::Number,
            (f, _) => f,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Frame {
    pub columns: Vec<Column>,
}

impl Frame {
    pub fn rows(&self) -> usize {
        self.columns.first().map(|c| c.values.len()).unwrap_or(0)
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Converts query results. `format` gives each column's declared format.
    pub fn from_batches(batches: &[RecordBatch], format: impl Fn(&str) -> Format) -> Result<Frame> {
        let Some(first) = batches.first() else {
            return Ok(Frame::default());
        };
        let schema = first.schema();
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            let arrays: Vec<&ArrayRef> = batches.iter().map(|b| b.column(i)).collect();
            columns.push(Column {
                name: field.name().clone(),
                format: format(field.name()),
                values: values(&arrays)?,
            });
        }
        Ok(Frame { columns })
    }
}

fn values(arrays: &[&ArrayRef]) -> Result<Values> {
    let data_type = arrays
        .first()
        .map(|a| a.data_type().clone())
        .unwrap_or(DataType::Null);
    Ok(match data_type {
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64 => {
            // Dates have no zone; they are read as UTC midnight.
            let target = DataType::Timestamp(TimeUnit::Microsecond, tz_of(&data_type));
            let mut out = Vec::new();
            for a in arrays {
                let a = cast(a, &target)?;
                let a = a.as_primitive::<TimestampMicrosecondType>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i))));
            }
            Values::Time(out)
        }
        t if t.is_integer() => {
            let mut out = Vec::new();
            for a in arrays {
                let a = cast(a, &DataType::Int64)?;
                let a = a.as_primitive::<Int64Type>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i) as f64)));
            }
            Values::Number(out)
        }
        t if t.is_floating()
            || matches!(t, DataType::Decimal128(..) | DataType::Decimal256(..)) =>
        {
            let mut out = Vec::new();
            for a in arrays {
                let a = cast(a, &DataType::Float64)?;
                let a = a.as_primitive::<Float64Type>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i))));
            }
            Values::Number(out)
        }
        _ => {
            let mut out = Vec::new();
            for a in arrays {
                let a = cast(a, &DataType::Utf8)
                    .map_err(|e| anyhow!("a column can't be shown as text: {e}"))?;
                let a = a.as_string::<i32>();
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i).to_string())));
            }
            Values::Text(out)
        }
    })
}

fn tz_of(t: &DataType) -> Option<std::sync::Arc<str>> {
    match t {
        DataType::Timestamp(_, tz) => tz.clone(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use deltalake::arrow::array::{
        Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
    };
    use deltalake::arrow::datatypes::{Field, Schema};

    use super::*;

    #[test]
    fn converts_numbers_times_and_text() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int32, true),
            Field::new("f", DataType::Float64, true),
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(Float64Array::from(vec![2.5, 3.0])),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(5), None]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let frame = Frame::from_batches(&[batch.clone(), batch], |_| Format::Auto).unwrap();
        assert_eq!(frame.rows(), 4);
        assert_eq!(
            frame.columns[0].values,
            Values::Number(vec![Some(1.0), None, Some(1.0), None])
        );
        assert_eq!(
            frame.columns[2].values,
            Values::Time(vec![Some(5), None, Some(5), None])
        );
        assert_eq!(frame.columns[2].effective_format(), Format::Timestamp);
        assert_eq!(frame.columns[3].values.text(0).as_deref(), Some("a"));
    }
}
