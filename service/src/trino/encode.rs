//! Arrow → Trino: column types (`type` + `typeSignature`) and row values in the text formats
//! the Trino client expects (trino-api-plan.md §2.3). Rows are written straight into the
//! response buffer, one record batch at a time.

use std::io::Write;

use arrow::array::temporal_conversions::{as_date, as_datetime, as_datetime_with_timezone};
use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::*;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};

pub struct TrinoColumn {
    /// The name sent to the client (may differ from the Arrow field: `_colN`).
    pub name: String,
    pub type_name: String,
    pub signature: Value,
    kind: Kind,
}

impl TrinoColumn {
    pub fn to_json(&self) -> Value {
        json!({ "name": self.name, "type": self.type_name, "typeSignature": self.signature })
    }
}

/// How a column's values are written. Types without a Trino equivalent here are sent as
/// varchar text (Arrow's display format).
#[derive(Clone)]
enum Kind {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal128 { precision: u8, scale: i8 },
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp { unit: TimeUnit, tz: Option<Tz>, tz_name: String },
    /// Dictionary-encoded: decoded to its value type first.
    Dictionary(Box<Kind>, DataType),
    Text,
}

pub fn columns(schema: &Schema) -> Vec<TrinoColumn> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let (type_name, signature, kind) = trino_type(f.data_type());
            TrinoColumn { name: f.name().clone(), type_name, signature, kind }
        })
        .collect()
}

/// The Trino type name of an Arrow type (what metadata tables report).
pub fn type_name(dt: &DataType) -> String {
    trino_type(dt).0
}

fn sig(raw: &str, args: &[i64]) -> Value {
    let arguments: Vec<Value> = args.iter().map(|v| json!({ "kind": "LONG", "value": v })).collect();
    json!({ "rawType": raw, "arguments": arguments })
}

fn simple(name: &str, kind: Kind) -> (String, Value, Kind) {
    (name.to_string(), sig(name, &[]), kind)
}

fn varchar(kind: Kind) -> (String, Value, Kind) {
    ("varchar".to_string(), sig("varchar", &[i32::MAX as i64]), kind)
}

fn precision(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

fn trino_type(dt: &DataType) -> (String, Value, Kind) {
    match dt {
        DataType::Boolean => simple("boolean", Kind::Bool),
        DataType::Int8 => simple("tinyint", Kind::Int8),
        DataType::Int16 => simple("smallint", Kind::Int16),
        DataType::Int32 => simple("integer", Kind::Int32),
        DataType::Int64 => simple("bigint", Kind::Int64),
        DataType::UInt8 => simple("smallint", Kind::UInt8),
        DataType::UInt16 => simple("integer", Kind::UInt16),
        DataType::UInt32 => simple("bigint", Kind::UInt32),
        DataType::UInt64 => ("decimal(20,0)".into(), sig("decimal", &[20, 0]), Kind::UInt64),
        DataType::Float32 => simple("real", Kind::Float32),
        DataType::Float64 => simple("double", Kind::Float64),
        DataType::Decimal128(p, s) => (
            format!("decimal({p},{s})"),
            sig("decimal", &[*p as i64, *s as i64]),
            Kind::Decimal128 { precision: *p, scale: *s },
        ),
        DataType::Utf8 => varchar(Kind::Utf8),
        DataType::LargeUtf8 => varchar(Kind::LargeUtf8),
        DataType::Utf8View => varchar(Kind::Utf8View),
        DataType::Binary => simple("varbinary", Kind::Binary),
        DataType::LargeBinary => simple("varbinary", Kind::LargeBinary),
        DataType::Date32 => simple("date", Kind::Date32),
        DataType::Date64 => simple("date", Kind::Date64),
        DataType::Timestamp(unit, tz) => {
            let p = precision(unit);
            match tz {
                None => (format!("timestamp({p})"), sig("timestamp", &[p]), Kind::Timestamp { unit: *unit, tz: None, tz_name: String::new() }),
                Some(name) => {
                    let parsed = name.parse::<Tz>().ok();
                    let tz_name = if matches!(name.as_ref(), "+00:00" | "Z" | "UTC") { "UTC".to_string() } else { name.to_string() };
                    (
                        format!("timestamp({p}) with time zone"),
                        sig("timestamp with time zone", &[p]),
                        Kind::Timestamp { unit: *unit, tz: parsed, tz_name },
                    )
                }
            }
        }
        DataType::Dictionary(_, value) => {
            let (name, signature, kind) = trino_type(value);
            (name, signature, Kind::Dictionary(Box::new(kind), (**value).clone()))
        }
        _ => varchar(Kind::Text),
    }
}

/// Writes one batch's rows as `[v1, v2, ...]` arrays.
pub struct BatchWriter<'a> {
    columns: Vec<(ArrayRef, &'a Kind)>,
    /// Pre-rendered values of `Kind::Text` columns.
    text: Vec<Option<Vec<String>>>,
}

impl<'a> BatchWriter<'a> {
    pub fn new(batch: &RecordBatch, cols: &'a [TrinoColumn]) -> Result<BatchWriter<'a>, arrow::error::ArrowError> {
        let mut columns = Vec::with_capacity(cols.len());
        let mut text = Vec::with_capacity(cols.len());
        for (array, col) in batch.columns().iter().zip(cols) {
            let (array, kind) = match &col.kind {
                Kind::Dictionary(inner, value_type) => (arrow::compute::cast(array, value_type)?, inner.as_ref()),
                kind => (array.clone(), kind),
            };
            text.push(match kind {
                Kind::Text => {
                    let f = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())?;
                    Some((0..array.len()).map(|i| f.value(i).to_string()).collect())
                }
                _ => None,
            });
            columns.push((array, kind));
        }
        Ok(BatchWriter { columns, text })
    }

    pub fn write_row(&self, row: usize, out: &mut Vec<u8>) {
        out.push(b'[');
        for (i, (array, kind)) in self.columns.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            if array.is_null(row) {
                out.extend_from_slice(b"null");
                continue;
            }
            match &self.text[i] {
                Some(values) => string(out, &values[row]),
                None => write_value(array, kind, row, out),
            }
        }
        out.push(b']');
    }
}

fn string(out: &mut Vec<u8>, s: &str) {
    let _ = serde_json::to_writer(&mut *out, s);
}

/// The Trino client decodes double/real columns as JSON floats: `561376` is rejected, so
/// values are written in Rust's Debug form (`561376.0`, `1e-7`), which always marks them.
fn float<F: Into<f64> + std::fmt::Debug + Copy>(out: &mut Vec<u8>, v: F) {
    let wide: f64 = v.into();
    if wide.is_nan() {
        out.extend_from_slice(b"\"NaN\"");
    } else if wide.is_infinite() {
        out.extend_from_slice(if wide > 0.0 { b"\"Infinity\"" } else { b"\"-Infinity\"" });
    } else {
        let _ = write!(out, "{v:?}");
    }
}

fn timestamp_text(v: i64, unit: &TimeUnit, tz: &Option<Tz>, tz_name: &str) -> Option<String> {
    let p = precision(unit) as usize;
    let format = |naive: chrono::NaiveDateTime| match p {
        0 => naive.format("%Y-%m-%d %H:%M:%S").to_string(),
        3 => naive.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        6 => naive.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        _ => naive.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
    };
    let local = match tz {
        // Timestamps with a zone are instants in UTC: shown in their zone, with its name.
        Some(tz) => match unit {
            TimeUnit::Second => as_datetime_with_timezone::<TimestampSecondType>(v, *tz),
            TimeUnit::Millisecond => as_datetime_with_timezone::<TimestampMillisecondType>(v, *tz),
            TimeUnit::Microsecond => as_datetime_with_timezone::<TimestampMicrosecondType>(v, *tz),
            TimeUnit::Nanosecond => as_datetime_with_timezone::<TimestampNanosecondType>(v, *tz),
        }
        .map(|d| d.naive_local()),
        None => match unit {
            TimeUnit::Second => as_datetime::<TimestampSecondType>(v),
            TimeUnit::Millisecond => as_datetime::<TimestampMillisecondType>(v),
            TimeUnit::Microsecond => as_datetime::<TimestampMicrosecondType>(v),
            TimeUnit::Nanosecond => as_datetime::<TimestampNanosecondType>(v),
        },
    }?;
    Some(if tz.is_some() || !tz_name.is_empty() { format!("{} {tz_name}", format(local)) } else { format(local) })
}

fn write_value(array: &ArrayRef, kind: &Kind, row: usize, out: &mut Vec<u8>) {
    match kind {
        Kind::Bool => out.extend_from_slice(if array.as_boolean().value(row) { b"true" } else { b"false" }),
        Kind::Int8 => { let _ = write!(out, "{}", array.as_primitive::<Int8Type>().value(row)); }
        Kind::Int16 => { let _ = write!(out, "{}", array.as_primitive::<Int16Type>().value(row)); }
        Kind::Int32 => { let _ = write!(out, "{}", array.as_primitive::<Int32Type>().value(row)); }
        Kind::Int64 => { let _ = write!(out, "{}", array.as_primitive::<Int64Type>().value(row)); }
        Kind::UInt8 => { let _ = write!(out, "{}", array.as_primitive::<UInt8Type>().value(row)); }
        Kind::UInt16 => { let _ = write!(out, "{}", array.as_primitive::<UInt16Type>().value(row)); }
        Kind::UInt32 => { let _ = write!(out, "{}", array.as_primitive::<UInt32Type>().value(row)); }
        // decimal(20,0): sent as a string, like every Trino decimal.
        Kind::UInt64 => { let _ = write!(out, "\"{}\"", array.as_primitive::<UInt64Type>().value(row)); }
        Kind::Float32 => float(out, array.as_primitive::<Float32Type>().value(row)),
        Kind::Float64 => float(out, array.as_primitive::<Float64Type>().value(row)),
        Kind::Decimal128 { precision, scale } => {
            let v = array.as_primitive::<Decimal128Type>().value(row);
            string(out, &Decimal128Type::format_decimal(v, *precision, *scale));
        }
        Kind::Utf8 => string(out, array.as_string::<i32>().value(row)),
        Kind::LargeUtf8 => string(out, array.as_string::<i64>().value(row)),
        Kind::Utf8View => string(out, array.as_string_view().value(row)),
        Kind::Binary => string(out, &BASE64.encode(array.as_binary::<i32>().value(row))),
        Kind::LargeBinary => string(out, &BASE64.encode(array.as_binary::<i64>().value(row))),
        Kind::Date32 => match as_date::<Date32Type>(array.as_primitive::<Date32Type>().value(row) as i64) {
            Some(d) => string(out, &d.format("%Y-%m-%d").to_string()),
            None => out.extend_from_slice(b"null"),
        },
        Kind::Date64 => match as_date::<Date64Type>(array.as_primitive::<Date64Type>().value(row)) {
            Some(d) => string(out, &d.format("%Y-%m-%d").to_string()),
            None => out.extend_from_slice(b"null"),
        },
        Kind::Timestamp { unit, tz, tz_name } => {
            let v = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            };
            match timestamp_text(v, unit, tz, tz_name) {
                Some(s) => string(out, &s),
                None => out.extend_from_slice(b"null"),
            }
        }
        // Decoded / pre-rendered in BatchWriter::new.
        Kind::Dictionary(..) | Kind::Text => out.extend_from_slice(b"null"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use std::sync::Arc;

    #[test]
    fn types_and_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("price", DataType::Decimal128(10, 2), false),
            Field::new("ok", DataType::Boolean, false),
            Field::new("sold_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("local_at", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("P\"E"), None])),
                Arc::new(Float64Array::from(vec![Some(561376.0), Some(f64::NAN)])),
                Arc::new(Decimal128Array::from(vec![12345, -5]).with_precision_and_scale(10, 2).unwrap()),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_790_812_800_000_010, 0]).with_timezone("UTC")),
                Arc::new(arrow::array::TimestampMillisecondArray::from(vec![0, 1_500])),
            ],
        )
        .unwrap();
        let cols = columns(&schema);
        let types: Vec<&str> = cols.iter().map(|c| c.type_name.as_str()).collect();
        assert_eq!(
            types,
            ["bigint", "varchar", "double", "decimal(10,2)", "boolean", "timestamp(6) with time zone", "timestamp(3)"]
        );
        assert_eq!(cols[5].signature, json!({"rawType": "timestamp with time zone", "arguments": [{"kind": "LONG", "value": 6}]}));

        let writer = BatchWriter::new(&batch, &cols).unwrap();
        let mut out = Vec::new();
        writer.write_row(0, &mut out);
        out.push(b',');
        writer.write_row(1, &mut out);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            r#"[1,"P\"E",561376.0,"123.45",true,"2026-10-01 00:00:00.000010 UTC","1970-01-01 00:00:00.000"],[2,null,"NaN","-0.05",false,"1970-01-01 00:00:00.000000 UTC","1970-01-01 00:00:01.500"]"#
        );
    }
}
