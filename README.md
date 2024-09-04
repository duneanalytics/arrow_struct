* Tests
* Benchmark
    * serde_arrow
    * arrow2-construct
* Configurable column cases with attributes
* Pick a better name

# Usage

## RecordBatch vs. StructArray

## Option vs non-Option
Unless you have a lot of trust in your data, prefer to use `Option` for all struct fields (i.e., `struct Struct { field: Option<i32> }` over `struct Struct { field: i32 }`),
except for nested structs. Arrow does not enforce not-null constraints in RecordBatches. That is, the schema can claim that it's not-null, while in fact the data is null.

We will panic if we encounter a null field for a not-Option column.

# Performance tips for deserialization

## Zero-copy

If you can, you should prefer to use references for non-primitive types (i.e., `&str` instead of `String`, `&[u8]` instead of `Bytes`).
This avoids clones.

## Avoid Arrow lists

If you can, you should prefer to avoid using Arrow lists.
Even if we are careful when deserializing lists, we create a vector for every row with a non-null list.