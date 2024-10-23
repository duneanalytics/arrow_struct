pub use arrow::array::Array;
pub use arrow::array::ArrayRef;
pub use arrow::array::AsArray;
use arrow::array::{GenericListArray, OffsetSizeTrait};
pub use arrow::datatypes::Int32Type;
pub use arrow::datatypes::Int64Type;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
pub use arrow::record_batch::RecordBatch;
pub use bytes::Bytes;
use std::fmt::Debug;
pub use std::option::Option;

pub use arrow_struct_derive::Deserialize;

pub trait FromArrayRef<'a>: Sized {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self>;
}

pub trait FromArrayRefOpt<'a>: Sized {
    type Item;
    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>>;
}

impl<'a, T: FromArrayRefOpt<'a, Item = T>> FromArrayRef<'a> for Option<T> {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        T::from_array_ref_opt(array)
    }
}

impl<'a, T: FromArrayRefOpt<'a, Item = T>> FromArrayRefOpt<'a> for Option<T> {
    type Item = T;

    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        T::from_array_ref_opt(array)
    }
}

// Effectively a marker trait, since stable Rust does not have specialization or negative trait bounds
pub trait NullConversion: Sized {
    type Item;
    fn convert(item: Option<Self::Item>) -> Self;
}

impl<T> NullConversion for Option<T> {
    type Item = T;

    fn convert(item: Option<Self::Item>) -> Self {
        item
    }
}

impl<T> NullConversion for Vec<T> {
    type Item = Vec<T>;

    fn convert(item: Option<Self::Item>) -> Self {
        item.unwrap()
    }
}

macro_rules! impl_from_array_ref_primitive {
    ($native_ty:ty, $data_ty:ty) => {
        impl<'a> FromArrayRefOpt<'a> for $native_ty {
            type Item = $native_ty;
            fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
                let array = array
                    .as_primitive_opt::<$data_ty>()
                    .expect(&format!(concat!(stringify!(Expected #data_ty), ", was {:?}"), array.data_type()));
                array.iter()
            }
        }

        /// Will panic on null
        impl<'a> FromArrayRef<'a> for $native_ty {
            fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
                let array = array
                    .as_primitive_opt::<$data_ty>()
                    .expect(&format!(concat!(stringify!(Expected #data_ty), ", was {:?}"), array.data_type()));
                array.iter().map(Option::unwrap)
            }
        }

        impl_null_conversion_simple_type!($native_ty);
    };
}

macro_rules! impl_null_conversion_simple_type {
    ($native_ty:ty) => {
        impl NullConversion for $native_ty {
            type Item = $native_ty;

            fn convert(item: Option<Self::Item>) -> Self {
                item.unwrap()
            }
        }

        impl NotNull for $native_ty {}
    };
}

impl_from_array_ref_primitive!(i8, Int8Type);
impl_from_array_ref_primitive!(i16, Int16Type);
impl_from_array_ref_primitive!(i32, Int32Type);
impl_from_array_ref_primitive!(i64, Int64Type);
impl_from_array_ref_primitive!(u8, UInt8Type);
impl_from_array_ref_primitive!(u16, UInt16Type);
impl_from_array_ref_primitive!(u32, UInt32Type);
impl_from_array_ref_primitive!(u64, UInt64Type);
impl_from_array_ref_primitive!(f32, Float32Type);
impl_from_array_ref_primitive!(f64, Float64Type);

impl_null_conversion_simple_type!(String);
impl_null_conversion_simple_type!(Bytes);
impl_null_conversion_simple_type!(bool);

impl<'a> FromArrayRefOpt<'a> for bool {
    type Item = bool;

    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        let array = array.as_boolean();
        let nulls = array.nulls();
        let mut iter = array.iter();
        let mut position = 0;
        std::iter::from_fn(move || {
            if let Some(next) = iter.next() {
                position += 1;
                if nulls
                    .map(|nulls| nulls.is_null(position))
                    .unwrap_or_default()
                {
                    Some(None)
                } else {
                    Some(next)
                }
            } else {
                None
            }
        })
    }
}

impl<'a> FromArrayRefOpt<'a> for String {
    type Item = String;
    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        let res: Box<dyn Iterator<Item = Option<Self::Item>>> = match array.data_type() {
            DataType::Utf8 => {
                let array = array.as_string::<i32>();
                Box::new(array.iter().map(|s| s.map(|s| s.to_string())))
            }
            DataType::LargeUtf8 => {
                let array = array.as_string::<i64>();
                Box::new(array.iter().map(|s| s.map(|s| s.to_string())))
            }
            _ => {
                panic!("Expected String, was {:?}", array.data_type())
            }
        };
        res
    }
}

impl<'a> FromArrayRef<'a> for String {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        Option::<String>::from_array_ref(array).map(|x| x.expect("unwrap String"))
    }
}

impl<'a> FromArrayRefOpt<'a> for &'a str {
    type Item = Self;

    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        let res: Box<dyn Iterator<Item = Option<Self::Item>>> = match array.data_type() {
            DataType::Utf8 => {
                let array = array.as_string::<i32>();
                Box::new(array.iter())
            }
            DataType::LargeUtf8 => {
                let array = array.as_string::<i64>();
                Box::new(array.iter())
            }
            _ => {
                panic!("Expected String, was {:?}", array.data_type())
            }
        };
        res
    }
}

impl<'a> FromArrayRef<'a> for &'a str {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        Option::<&'a str>::from_array_ref(array).map(|x| x.expect("unwrap str"))
    }
}

impl<'a> FromArrayRefOpt<'a> for Bytes {
    type Item = Bytes;

    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        let res: Box<dyn Iterator<Item = Option<Self::Item>>> = match array.data_type() {
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                Box::new(
                    array
                        .iter()
                        .map(|bytes| bytes.map(|bytes| Bytes::from(bytes.to_vec()))),
                )
            }
            DataType::LargeBinary => {
                let array = array.as_binary::<i64>();
                Box::new(
                    array
                        .iter()
                        .map(|bytes| bytes.map(|bytes| Bytes::from(bytes.to_vec()))),
                )
            }
            _ => {
                panic!("Expected Binary, was {:?}", array.data_type())
            }
        };
        res
    }
}

impl<'a> FromArrayRef<'a> for Bytes {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        Option::<Bytes>::from_array_ref(array).map(|x| x.expect("unwrap bytes"))
    }
}

impl<'a, 'c> FromArrayRefOpt<'a> for &'c [u8]
where
    'a: 'c,
{
    type Item = &'c [u8];
    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        let res: Box<dyn Iterator<Item = Option<Self::Item>>> = match array.data_type() {
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                Box::new(array.iter())
            }
            DataType::LargeBinary => {
                let array = array.as_binary::<i64>();
                Box::new(array.iter())
            }
            _ => {
                panic!("Expected Binary, was {:?}", array.data_type())
            }
        };
        res
    }
}

impl<'a, T: FromArrayRefOpt<'a> + Debug + 'a> FromArrayRefOpt<'a> for Vec<Option<T>> {
    type Item = Vec<Option<T::Item>>;

    // TODO: Needs extensive testing.
    // This is a bit verbose, but the naive implementation below is too slow:
    // array.iter()
    //      .map(|element|
    //           element.as_ref().map(|element| T::from_array_ref(element).collect::<Vec<_>>()))
    // We must use array.values() directly and handle the offsets, as we cannot call
    // T::from_array_ref in any kind of loop.
    // Could be room for more optimization by not using iterators?
    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        fn helper<'a, O: OffsetSizeTrait + Into<i64>, T: FromArrayRefOpt<'a> + Debug + 'a>(
            array: &'a GenericListArray<O>,
        ) -> impl Iterator<Item = Option<Vec<Option<<T as FromArrayRefOpt<'a>>::Item>>>> + 'a
        {
            let nulls = array.logical_nulls();
            let offsets = array.offsets();
            let mut inner = T::from_array_ref_opt(array.values());
            let mut current_position = 0;

            if let Some(first_offset) = offsets.first() {
                for _ in 0..first_offset.as_usize() {
                    let _ = inner.next();
                }
            }

            std::iter::from_fn(move || {
                if current_position >= array.len() {
                    return None;
                }

                let len = array.value_length(current_position).into();
                let is_null = nulls
                    .as_ref()
                    .map(|buffer| buffer.is_null(current_position))
                    .unwrap_or_default();
                let res = if is_null {
                    for _ in 0..len {
                        // This can happen if record batch has values which are nulled. It's weird to construct RecordBatches this way, but it's possible
                        let _ = inner.next().unwrap();
                    }
                    None
                } else {
                    let mut out = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        out.push(inner.next().unwrap());
                    }
                    Some(out)
                };
                current_position += 1;
                Some(res)
            })
        }

        let res: Box<dyn Iterator<Item = Option<Self::Item>>> = match array.data_type() {
            DataType::List(_) => {
                let array = array.as_list::<i32>();
                Box::new(helper::<_, T>(array))
            }
            DataType::LargeList(_) => {
                let array = array.as_list::<i64>();
                Box::new(helper::<_, T>(array))
            }
            _ => {
                panic!("Expected List, was {:?}", array.data_type())
            }
        };
        res
    }
}

pub trait NotNull {}

impl<'a, T: FromArrayRefOpt<'a> + Debug + NotNull + 'a> FromArrayRefOpt<'a> for Vec<T> {
    type Item = Vec<T::Item>;

    // TODO: Needs extensive testing.
    // This is a bit verbose, but the naive implementation below is too slow:
    // array.iter()
    //      .map(|element|
    //           element.as_ref().map(|element| T::from_array_ref(element).collect::<Vec<_>>()))
    // We must use array.values() directly and handle the offsets, as we cannot call
    // T::from_array_ref in any kind of loop.
    // Could be room for more optimization by not using iterators?
    fn from_array_ref_opt(array: &'a ArrayRef) -> impl Iterator<Item = Option<Self::Item>> {
        fn helper<'a, O: OffsetSizeTrait + Into<i64>, T: FromArrayRefOpt<'a> + Debug + 'a>(
            array: &'a GenericListArray<O>,
        ) -> impl Iterator<Item = Option<Vec<<T as FromArrayRefOpt<'a>>::Item>>> + 'a {
            let nulls = array.logical_nulls();
            let offsets = array.offsets();
            let mut inner = T::from_array_ref_opt(array.values());
            let mut current_position = 0;

            if let Some(first_offset) = offsets.first() {
                for _ in 0..first_offset.as_usize() {
                    let _ = inner.next();
                }
            }

            std::iter::from_fn(move || {
                if current_position >= array.len() {
                    return None;
                }

                let len = array.value_length(current_position).into();
                let is_null = nulls
                    .as_ref()
                    .map(|buffer| buffer.is_null(current_position))
                    .unwrap_or_default();
                let res = if is_null {
                    for _ in 0..len {
                        // This can happen if record batch has values which are nulled. It's weird to construct RecordBatches this way, but it's possible
                        let _ = inner.next().unwrap();
                    }
                    None
                } else {
                    let mut out = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        out.push(inner.next().unwrap().expect("unwrap in vec"));
                    }
                    Some(out)
                };
                current_position += 1;
                Some(res)
            })
        }

        let res: Box<dyn Iterator<Item = Option<Self::Item>>> = match array.data_type() {
            DataType::List(_) => {
                let array = array.as_list::<i32>();
                Box::new(helper::<_, T>(array))
            }
            DataType::LargeList(_) => {
                let array = array.as_list::<i64>();
                Box::new(helper::<_, T>(array))
            }
            _ => {
                panic!("Expected List, was {:?}", array.data_type())
            }
        };
        res
    }
}
