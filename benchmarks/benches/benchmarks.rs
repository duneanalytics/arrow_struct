use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::sync::Arc;

fn serde_arrow_convert<T: DeserializeOwned>(batch: &RecordBatch) -> Vec<T> {
    serde_arrow::from_record_batch(&batch).unwrap()
}

fn arrow_struct_convert<'a, T: arrow_struct::FromArrayRef<'a>>(batch: &'a ArrayRef) -> Vec<T> {
    T::from_array_ref(batch).collect()
}

fn setup_record_batch<T: DeserializeOwned + Serialize + From<usize>>(size: usize) -> RecordBatch {
    let batch = (0..size).map(|i| T::from(i)).collect::<Vec<_>>();
    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default()).unwrap();
    serde_arrow::to_record_batch(&fields, &batch).unwrap()
}

fn benchmark<
    T: DeserializeOwned + Serialize + From<usize> + for<'a> arrow_struct::FromArrayRef<'a>,
>(
    c: &mut Criterion,
    size: usize,
) {
    let batch = setup_record_batch::<T>(size);
    let struct_array: StructArray = batch.clone().into();
    let array: ArrayRef = Arc::new(struct_array);
    c.bench_function(&format!("serde_arrow {} {}", std::any::type_name::<T>(), size), |b| {
        b.iter_with_large_drop(|| serde_arrow_convert::<T>(black_box(&batch)))
    });
    c.bench_function(&format!("arrow_struct {} {}", std::any::type_name::<T>(), size), |b| {
        b.iter_with_large_drop(|| arrow_struct_convert::<T>(black_box(&array)))
    });
}

#[derive(Deserialize, Serialize, arrow_struct::Deserialize)]
struct Small {
    i64: i64,
}

impl From<usize> for Small {
    fn from(value: usize) -> Self {
        Self { i64: value as i64 }
    }
}

fn benchmark_small(c: &mut Criterion) {
    benchmark::<Small>(c, 1024)
}

#[derive(Deserialize, Serialize, arrow_struct::Deserialize)]
struct Large {
    vec: Option<Vec<i64>>,
}

impl From<usize> for Large {
    fn from(value: usize) -> Self {
        Self {
            vec: Some(vec![value as i64; value]),
        }
    }
}

fn benchmark_large(c: &mut Criterion) {
    benchmark::<Large>(c, 1024)
}

criterion_group!(benches, benchmark_small, benchmark_large);
criterion_main!(benches);
