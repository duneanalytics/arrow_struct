#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BinaryArray, GenericListBuilder, Int32Builder, LargeBinaryArray, LargeStringArray,
        LargeStringBuilder, RecordBatch, StructArray,
    };
    use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
    use arrow_struct::Deserialize;
    use arrow_struct::FromArrayRef;
    use serde_arrow::_impl::arrow::_raw::buffer::NullBufferBuilder;
    use serde_arrow::_impl::arrow::array::StringArray;
    use serde_arrow::schema::{SchemaLike, TracingOptions};
    use std::sync::Arc;

    #[allow(dead_code)]
    #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
    struct AllPrimitiveTypes<'a> {
        i8: Option<i8>,
        i16: Option<i16>,
        i32: Option<i32>,
        i64: Option<i64>,
        u8: Option<u8>,
        u16: Option<u16>,
        u32: Option<u32>,
        u64: Option<u64>,
        f32: Option<f32>,
        f64: Option<f64>,
        str: Option<&'a str>,
        byte_slice: Option<&'a [u8]>,
    }

    #[test]
    fn all_primitive_types() {
        println!("here");
        let some_string = "0123456789";
        let data = (0u8..10)
            .map(|i| AllPrimitiveTypes {
                i8: Some(1 + i as i8),
                i16: Some(2 + i as i16),
                i32: Some(3 + i as i32),
                i64: Some(4 + i as i64),
                u8: Some(5 + i),
                u16: Some(6 + i as u16),
                u32: Some(7 + i as u32),
                u64: Some(8 + i as u64),
                f32: Some(9.0 + i as f32),
                f64: Some(10.0 + i as f64),
                str: Some(&some_string[..i as usize]),
                byte_slice: Some(some_string[..i as usize].as_bytes()),
            })
            .collect::<Vec<_>>();
        let fields =
            Vec::<FieldRef>::from_type::<AllPrimitiveTypes>(TracingOptions::default()).unwrap();
        let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
        let struct_array: StructArray = batch.into();
        let array = Arc::new(struct_array) as _;
        assert_eq!(
            data,
            AllPrimitiveTypes::from_array_ref(&array).collect::<Vec<_>>()
        );
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
    struct Nested1 {
        element: Option<i32>,
        nested_2: Option<Vec<Nested2>>,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
    struct Nested2 {
        nested_3: Option<Vec<Nested3>>,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
    struct Nested3 {
        nested_4a: Option<Vec<Nested4>>,
        nested_4b: Option<Vec<Nested4>>,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
    struct Nested4 {
        element: Option<i32>,
    }

    #[test]
    fn deeply_nested() {
        let data = (0..10)
            .map(|i| Nested1 {
                element: Some(i),
                nested_2: Some(vec![Nested2 {
                    nested_3: Some(vec![Nested3 {
                        nested_4a: Some(vec![Nested4 { element: Some(i) }]),
                        nested_4b: Some(vec![Nested4 { element: Some(i) }]),
                    }]),
                }]),
            })
            .collect::<Vec<_>>();
        let fields = Vec::<FieldRef>::from_type::<Nested1>(TracingOptions::default()).unwrap();
        let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
        let struct_array: StructArray = batch.into();
        let array = Arc::new(struct_array) as _;
        assert_eq!(data, Nested1::from_array_ref(&array).collect::<Vec<_>>());
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
    struct Outer {
        inner: Inner,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
    struct Inner {
        i64: Option<i64>,
    }

    #[test]
    fn outer_inner() {
        let data = (0..10)
            .map(|i| Outer {
                inner: Inner { i64: Some(i) },
            })
            .collect::<Vec<_>>();
        let fields = Vec::<FieldRef>::from_type::<Outer>(TracingOptions::default()).unwrap();
        let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
        let struct_array: StructArray = batch.into();
        let array = Arc::new(struct_array) as _;
        assert_eq!(data, Outer::from_array_ref(&array).collect::<Vec<_>>());
    }

    #[allow(dead_code)]
    #[derive(Deserialize, Debug)]
    struct Struct<'a> {
        id: Option<i32>,
        id2: Option<i64>,
        id3: Nested,
        list: Option<Vec<Option<i32>>>,
        list_nested: Option<Vec<Nested>>,
        bytes: Option<&'a [u8]>,
    }

    #[allow(dead_code)]
    #[derive(Deserialize, Debug)]
    struct Nested {
        id: Option<i32>,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct SmallAndLargeArrays<'a> {
        string: Option<&'a str>,
        large_string: Option<&'a str>,
        binary: Option<&'a [u8]>,
        large_binary: Option<&'a [u8]>,
        list: Option<Vec<i32>>,
        large_list: Option<Vec<i32>>,
    }

    #[test]
    fn small_and_large_arrays() {
        let string_array: Arc<dyn Array> = Arc::new(StringArray::from(vec!["1", "2"]));
        let large_string_array: Arc<dyn Array> = Arc::new(LargeStringArray::from(vec!["1", "2"]));
        let data: Vec<Option<&[u8]>> = vec![Some(b"1"), Some(b"2")];
        let binary_array: Arc<dyn Array> = Arc::new(BinaryArray::from(data.clone()));
        let large_binary_array: Arc<dyn Array> = Arc::new(LargeBinaryArray::from(data));
        let mut builder: GenericListBuilder<i32, Int32Builder> =
            GenericListBuilder::new(Int32Builder::new());
        for i in 1..=2 {
            builder.values().append_value(i);
            builder.append(true);
        }
        let list_array: Arc<dyn Array> = Arc::new(builder.finish());
        let mut builder: GenericListBuilder<i64, Int32Builder> =
            GenericListBuilder::new(Int32Builder::new());
        for i in 1..=2 {
            builder.values().append_value(i);
            builder.append(true);
        }
        let large_list_array: Arc<dyn Array> = Arc::new(builder.finish());

        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let schema = Schema::new(vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("large_string", DataType::LargeUtf8, false),
            Field::new("binary", DataType::Binary, false),
            Field::new("large_binary", DataType::LargeBinary, false),
            Field::new("list", DataType::List(list_field.clone()), true),
            Field::new("large_list", DataType::LargeList(list_field), false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                string_array,
                large_string_array,
                binary_array,
                large_binary_array,
                list_array,
                large_list_array,
            ],
        )
        .unwrap();

        let struct_array: StructArray = batch.into();
        let array = Arc::new(struct_array) as _;
        let expected = vec![
            SmallAndLargeArrays {
                string: Some("1"),
                large_string: Some("1"),
                binary: Some(b"1"),
                large_binary: Some(b"1"),
                list: Some(vec![1]),
                large_list: Some(vec![1]),
            },
            SmallAndLargeArrays {
                string: Some("2"),
                large_string: Some("2"),
                binary: Some(b"2"),
                large_binary: Some(b"2"),
                list: Some(vec![2]),
                large_list: Some(vec![2]),
            },
        ];
        assert_eq!(
            expected,
            SmallAndLargeArrays::from_array_ref(&array).collect::<Vec<_>>()
        );
    }

    #[test]
    fn null_object() {
        #[allow(dead_code)]
        #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
        #[arrow_struct(rename_all = "camelCase")]
        struct NullOuter {
            inner1: Option<NullInner>,
        }
        #[allow(dead_code)]
        #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
        struct NullInner {
            string1: String,
        }

        let data = [
            NullOuter { inner1: None },
            NullOuter {
                inner1: Some(NullInner {
                    string1: "hello".to_string(),
                }),
            },
            NullOuter { inner1: None },
            NullOuter {
                inner1: Some(NullInner {
                    string1: "world".to_string(),
                }),
            },
        ];

        let mut string_array_builder = LargeStringBuilder::new();
        string_array_builder.append_null();
        string_array_builder.append_value("hello");
        string_array_builder.append_null();
        string_array_builder.append_value("world");

        let mut null_buffer_builder = NullBufferBuilder::new(10);
        null_buffer_builder.append_null();
        null_buffer_builder.append(true);
        null_buffer_builder.append_null();
        null_buffer_builder.append(true);

        let fields_inner = Vec::<FieldRef>::from_type::<NullInner>(
            TracingOptions::default().allow_null_fields(true),
        )
        .unwrap();
        let struct_array_inner = StructArray::new(
            Fields::from(fields_inner),
            vec![Arc::new(string_array_builder.finish())],
            null_buffer_builder.finish(),
        );

        let fields = Vec::<FieldRef>::from_type::<NullOuter>(
            TracingOptions::default().allow_null_fields(true),
        )
        .unwrap();
        let struct_array = StructArray::new(
            Fields::from(fields),
            vec![Arc::new(struct_array_inner)],
            None,
        );
        let batch = RecordBatch::from(struct_array);
        let i = 2;
        let length = 2;
        let struct_array: StructArray = batch.clone().into();
        let struct_array = struct_array.slice(i, length);

        let array = Arc::new(struct_array) as _;
        assert_eq!(
            &data[i..i + length],
            NullOuter::from_array_ref(&array).collect::<Vec<_>>()
        );
    }

    #[test]
    fn null_object_vec() {
        #[allow(dead_code)]
        #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
        struct Struct {
            string: Vec<String>,
        }
        let data = (1..=10)
            .map(|x| Struct {
                string: (1..=x).map(|y| y.to_string()).collect(),
            })
            .collect::<Vec<_>>();
        let fields = Vec::<FieldRef>::from_type::<Struct>(TracingOptions::default()).unwrap();
        let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
        let batch = batch.slice(5, 5);

        let struct_array: StructArray = batch.clone().into();
        let array = Arc::new(struct_array) as _;
        let actual = Struct::from_array_ref(&array).collect::<Vec<_>>();
        assert_eq!(data[5..10], actual);
    }

    #[test]
    fn camel_case() {
        #[allow(dead_code)]
        #[derive(serde::Deserialize, serde::Serialize, Deserialize, Debug, PartialEq)]
        #[serde(rename_all = "camelCase")]
        #[arrow_struct(rename_all = "camelCase")]
        struct Struct {
            camel_case: i64,
        }
        let data = vec![Struct { camel_case: 1 }];
        let fields = Vec::<FieldRef>::from_type::<Struct>(TracingOptions::default()).unwrap();
        let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();

        let struct_array: StructArray = batch.clone().into();
        let array = Arc::new(struct_array) as _;
        println!("{:?}", Struct::from_array_ref(&array).collect::<Vec<_>>());
    }
}
