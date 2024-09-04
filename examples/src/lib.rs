#[cfg(test)]
mod tests {
    use arrow_struct::Deserialize;

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

    use arrow::array::{
        Array, BinaryArray, GenericListBuilder, Int32Array, Int32Builder, Int64Array, RecordBatch,
        StructArray, StructBuilder,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_struct::FromArrayRef;
    use std::sync::Arc;

    #[test]
    fn my_test() {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let id2_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let bin_data: Vec<&[u8]> = vec![b"1", b"2", b"3", b"4", b"5"];
        let binary_array = BinaryArray::from(bin_data);
        let schema_nested = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let list_nested_field = Arc::new(Field::new_list_field(
            DataType::Struct(schema_nested.clone().fields),
            true,
        ));
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id2", DataType::Int64, false),
            Field::new("id3", DataType::Struct(schema_nested.clone().fields), false),
            Field::new("list", DataType::List(list_field), false),
            Field::new("listNested", DataType::List(list_nested_field), true),
            Field::new("bytes", DataType::Binary, false),
        ]);

        println!("{:?}", schema);

        let values_builder = Int32Builder::new();
        let mut builder: GenericListBuilder<i32, Int32Builder> =
            GenericListBuilder::new(values_builder);

        for i in 1..=5 {
            builder.values().append_value(i);
            builder.append(true);
        }
        let list = builder.finish();

        let values_builder = StructBuilder::from_fields(schema_nested.clone().fields, 5);
        let mut builder: GenericListBuilder<i32, StructBuilder> =
            GenericListBuilder::new(values_builder);
        for i in 1..=5 {
            for j in 1..=i {
                let field_builder: &mut Int32Builder = builder.values().field_builder(0).unwrap();
                field_builder.append_value(j);
                builder.values().append(true);
            }

            if i % 2 == 0 {
                builder.append(true);
            } else {
                builder.append_null();
            }
        }
        let list_nested = builder.finish();

        let ids = Arc::new(id_array);
        let ids2 = Arc::new(id2_array);
        let batch_nested =
            RecordBatch::try_new(Arc::new(schema_nested), vec![ids.clone()]).unwrap();
        let array_nested: StructArray = batch_nested.into();
        let ids3 = Arc::new(array_nested);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                ids.clone(),
                ids2.clone(),
                ids3.clone(),
                Arc::new(list),
                Arc::new(list_nested),
                Arc::new(binary_array),
            ],
        )
        .unwrap();
        let st: StructArray = batch.into();

        let st: Arc<dyn Array> = Arc::new(st);
        let s = Struct::from_array_ref(&st);
        println!("{:#?}", s.collect::<Vec<_>>())
    }
}
