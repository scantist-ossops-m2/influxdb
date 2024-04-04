//! The in memory buffer of a table that can be quickly added to and queried

use crate::write_buffer::{FieldData, Row};
use arrow::array::{ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, GenericByteBuilder, GenericByteDictionaryBuilder, Int64Builder, StringArray, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder, UInt64Builder};
use arrow::datatypes::{GenericStringType, Int32Type};
use arrow::record_batch::RecordBatch;
use data_types::{PartitionKey, TimestampMinMax};
use observability_deps::tracing::debug;
use schema::Schema;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::vec;
use arrow::util::pretty::pretty_format_batches;

pub struct TableBuffer {
    pub segment_key: PartitionKey,
    timestamp_min: i64,
    timestamp_max: i64,
    pub(crate) data: BTreeMap<String, Builder>,
    row_count: usize,
    tag_index: HashMap<Arc<str>, Vec<usize>>,
}

impl TableBuffer {
    pub fn new(segment_key: PartitionKey) -> Self {
        Self {
            segment_key,
            timestamp_min: i64::MAX,
            timestamp_max: i64::MIN,
            data: Default::default(),
            row_count: 0,
            tag_index: Default::default(),
        }
    }

    pub fn add_rows(&mut self, rows: Vec<Row>) {
        let new_row_count = rows.len();

        for (row_index, r) in rows.into_iter().enumerate() {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in r.fields {
                value_added.insert(f.name.clone());

                match f.value {
                    FieldData::Timestamp(v) => {
                        self.timestamp_min = self.timestamp_min.min(v);
                        self.timestamp_max = self.timestamp_max.max(v);

                        let b = self.data.entry(f.name).or_insert_with(|| {
                            debug!("Creating new timestamp builder");
                            let mut time_builder = TimestampNanosecondBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                debug!("Appending null for timestamp");
                                time_builder.append_null();
                            }
                            Builder::Time(time_builder)
                        });
                        if let Builder::Time(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Tag(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut tag_builder = StringDictionaryBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                tag_builder.append_null();
                            }
                            Builder::Tag(tag_builder)
                        });
                        if let Builder::Tag(b) = b {
                            let val = v.as_str().into();
                            let rows_with_tag = self.tag_index.entry(val).or_insert_with(Vec::new);
                            rows_with_tag.push(b.len());
                            b.append(v).unwrap(); // we won't overflow the 32-bit integer for this dictionary
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::String(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut string_builder = StringBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                string_builder.append_null();
                            }
                            Builder::String(string_builder)
                        });
                        if let Builder::String(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Integer(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut int_builder = Int64Builder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                int_builder.append_null();
                            }
                            Builder::I64(int_builder)
                        });
                        if let Builder::I64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::UInteger(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut uint_builder = UInt64Builder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                uint_builder.append_null();
                            }
                            Builder::U64(uint_builder)
                        });
                        if let Builder::U64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Float(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut float_builder = Float64Builder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                float_builder.append_null();
                            }
                            Builder::F64(float_builder)
                        });
                        if let Builder::F64(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Boolean(v) => {
                        let b = self.data.entry(f.name).or_insert_with(|| {
                            let mut bool_builder = BooleanBuilder::new();
                            // append nulls for all previous rows
                            for _ in 0..(row_index + self.row_count) {
                                bool_builder.append_null();
                            }
                            Builder::Bool(bool_builder)
                        });
                        if let Builder::Bool(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                }
            }

            // add nulls for any columns not present
            for (name, builder) in &mut self.data {
                if !value_added.contains(name) {
                    debug!("Adding null for column {}", name);
                    match builder {
                        Builder::Bool(b) => b.append_null(),
                        Builder::F64(b) => b.append_null(),
                        Builder::I64(b) => b.append_null(),
                        Builder::U64(b) => b.append_null(),
                        Builder::String(b) => b.append_null(),
                        Builder::Tag(b) => b.append_null(),
                        Builder::Time(b) => b.append_null(),
                    }
                }
            }
        }

        self.row_count += new_row_count;
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.timestamp_min,
            max: self.timestamp_max,
        }
    }

    pub fn record_batches(&self, schema: &Schema) -> Vec<RecordBatch> {
        // ensure the order of the columns matches their order in the Arrow schema definition
        let mut cols = Vec::with_capacity(self.data.len());
        let schema = schema.as_arrow();
        for f in &schema.fields {
            cols.push(
                self.data
                    .get(f.name())
                    .unwrap_or_else(|| panic!("missing field in table buffer: {}", f.name()))
                    .as_arrow(),
            );
        }

        vec![RecordBatch::try_new(schema, cols).unwrap()]
    }

    pub fn record_batch_for_series(&self, schema: &Schema, projection: Option<&Vec<usize>>, series: &str) -> Option<RecordBatch> {
        let series_id_rows = self.tag_index.get(series)?;

        let schema = schema.as_arrow();

        let column_names: Vec<_> = match projection {
            Some(p) => p.iter().map(|i| schema.field(*i).name()).collect(),
            None => schema.fields().iter().map(|f| f.name()).collect(),
        };
        let mut cols = Vec::with_capacity(column_names.len());

        for name in column_names {
            let builder = self.data.get(name).unwrap();
            cols.push(builder.get_rows(series_id_rows));
        }

        match projection {
            Some(p) => {
                let schema = schema.project(p).expect("projection bug");
                Some(RecordBatch::try_new(Arc::new(schema), cols).unwrap())
            },
            None => {
                Some(RecordBatch::try_new(schema, cols).unwrap())
            }
        }
    }
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for TableBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableBuffer")
            .field("segment_key", &self.segment_key)
            .field("timestamp_min", &self.timestamp_min)
            .field("timestamp_max", &self.timestamp_max)
            .field("row_count", &self.row_count)
            .finish()
    }
}

pub enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    Time(TimestampNanosecondBuilder),
}

impl Builder {
    fn as_arrow(&self) -> ArrayRef {
        match self {
            Self::Bool(b) => Arc::new(b.finish_cloned()),
            Self::I64(b) => Arc::new(b.finish_cloned()),
            Self::F64(b) => Arc::new(b.finish_cloned()),
            Self::U64(b) => Arc::new(b.finish_cloned()),
            Self::String(b) => Arc::new(b.finish_cloned()),
            Self::Tag(b) => Arc::new(b.finish_cloned()),
            Self::Time(b) => Arc::new(b.finish_cloned()),
        }
    }

    fn get_rows(&self, rows: &[usize]) -> ArrayRef {
        match self {
            Self::Bool(b) => {
                let b = b.finish_cloned();
                let mut builder = BooleanBuilder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            },
            Self::I64(b) => {
                let b = b.finish_cloned();
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            },
            Self::F64(b) => {
                let b = b.finish_cloned();
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            },
            Self::U64(b) => {
                let b = b.finish_cloned();
                let mut builder = UInt64Builder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            },
            Self::String(b) => {
                let b = b.finish_cloned();
                let mut builder = StringBuilder::new();
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            },
            Self::Tag(b) => {
                let b = b.finish_cloned();
                let bv = b.values();
                let bva: &StringArray = bv.as_any().downcast_ref::<StringArray>().unwrap();

                let mut builder: GenericByteDictionaryBuilder<Int32Type, GenericStringType<i32>> = StringDictionaryBuilder::new();
                for row in rows {
                    let val = b.key(*row).unwrap();
                    let tag_val = bva.value(val);
                    builder.append(tag_val);
                }
                Arc::new(builder.finish())

            },
            Self::Time(b) => {
                let b = b.finish_cloned();
                let mut builder = TimestampNanosecondBuilder::with_capacity(rows.len());
                for row in rows {
                    builder.append_value(b.value(*row));
                }
                Arc::new(builder.finish())
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};
    use arrow_util::assert_batches_eq;
    use schema::{InfluxFieldType, SchemaBuilder};
    use crate::write_buffer::Field;
    use super::*;

    #[test]
    fn tag_row_index() {
        let mut table_buffer = TableBuffer::new(PartitionKey::from("table"));
        let schema = SchemaBuilder::with_capacity(3)
            .tag("tag")
            .influx_field("value", InfluxFieldType::Integer)
            .timestamp()
            .build()
            .unwrap();


        let rows = vec![
            Row {
                time: 1,
                fields: vec![
                    Field{name: "tag".to_string(), value: FieldData::Tag("a".to_string())},
                    Field{name: "value".to_string(), value: FieldData::Integer(1)},
                    Field{name: "time".to_string(), value: FieldData::Timestamp(1)},
                ],
            },
            Row {
                time: 2,
                fields: vec![
                    Field{name: "tag".to_string(), value: FieldData::Tag("b".to_string())},
                    Field{name: "value".to_string(), value: FieldData::Integer(2)},
                    Field{name: "time".to_string(), value: FieldData::Timestamp(2)},
                ],
            },
            Row {
                time: 3,
                fields: vec![
                    Field{name: "tag".to_string(), value: FieldData::Tag("a".to_string())},
                    Field{name: "value".to_string(), value: FieldData::Integer(3)},
                    Field{name: "time".to_string(), value: FieldData::Timestamp(3)},
                ],
            },
        ];

        table_buffer.add_rows(rows);
        let a_rows = table_buffer.tag_index.get("a").unwrap();
        assert_eq!(a_rows, &[0, 2]);

        let a = table_buffer.record_batch_for_series(&schema, None, "a").unwrap();
        let expected_a =  vec![
            "+-----+-------+--------------------------------+",
            "| tag | value | time                           |",
            "+-----+-------+--------------------------------+",
            "| a   | 1     | 1970-01-01T00:00:00.000000001Z |",
            "| a   | 3     | 1970-01-01T00:00:00.000000003Z |",
            "+-----+-------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_a, &[a]);

        let b_rows = table_buffer.tag_index.get("b").unwrap();
        assert_eq!(b_rows, &[1]);

        let b = table_buffer.record_batch_for_series(&schema, None, "b").unwrap();
        let expected_b =  vec![
            "+-----+-------+--------------------------------+",
            "| tag | value | time                           |",
            "+-----+-------+--------------------------------+",
            "| b   | 2     | 1970-01-01T00:00:00.000000002Z |",
            "+-----+-------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_b, &[b]);

        let b = table_buffer.record_batch_for_series(&schema, Some(&vec![1, 2]), "b").unwrap();
        let expected_b =  vec![
            "+-------+--------------------------------+",
            "| value | time                           |",
            "+-------+--------------------------------+",
            "| 2     | 1970-01-01T00:00:00.000000002Z |",
            "+-------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_b, &[b]);
    }
}