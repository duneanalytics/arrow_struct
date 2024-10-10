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

pub use arrow_struct_derive::Deserialize;

pub trait FromArrayRef<'a>: Sized {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self>;
}

macro_rules! impl_from_array_ref_primitive {
    ($native_ty:ty, $data_ty:ty) => {
        impl<'a> FromArrayRef<'a> for Option<$native_ty> {
            fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
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

impl<'a> FromArrayRef<'a> for Option<bool> {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        let array = array.as_boolean();
        array.iter()
    }
}

impl<'a> FromArrayRef<'a> for Option<String> {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        let res: Box<dyn Iterator<Item = Self>> = match array.data_type() {
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

impl<'a> FromArrayRef<'a> for Option<&'a str> {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        let res: Box<dyn Iterator<Item = Self>> = match array.data_type() {
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

impl<'a> FromArrayRef<'a> for Option<Bytes> {
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        let res: Box<dyn Iterator<Item = Self>> = match array.data_type() {
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                Box::new(array.iter()
                    .map(|bytes| bytes.map(|bytes| Bytes::from(bytes.to_vec()))))
            }
            DataType::LargeBinary => {
                let array = array.as_binary::<i64>();
                Box::new(array.iter()
                    .map(|bytes| bytes.map(|bytes| Bytes::from(bytes.to_vec()))))
            }
            _ => {
                panic!("Expected String, was {:?}", array.data_type())
            }
        };
        res
    }
}

impl<'a, 'c> FromArrayRef<'a> for Option<&'c [u8]>
where
    'a: 'c,
{
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Option<&'c [u8]>> {
        let res: Box<dyn Iterator<Item = Self>> = match array.data_type() {
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

impl<'a, T: FromArrayRef<'a> + Debug + 'a> FromArrayRef<'a> for Option<Vec<T>> {
    // TODO: Needs extensive testing.
    // This is a bit verbose, but the naive implementation below is too slow:
    // array.iter()
    //      .map(|element|
    //           element.as_ref().map(|element| T::from_array_ref(element).collect::<Vec<_>>()))
    // We must use array.values() directly and handle the offsets, as we cannot call
    // T::from_array_ref in any kind of loop.
    // Could be room for more optimization by not using iterators?
    fn from_array_ref(array: &'a ArrayRef) -> impl Iterator<Item = Self> {
        fn helper<'a, O: OffsetSizeTrait + Into<i64>, T: FromArrayRef<'a> + Debug + 'a>(
            array: &'a GenericListArray<O>,
        ) -> impl Iterator<Item = Option<Vec<T>>> + 'a {
            let nulls = array.logical_nulls();
            let mut inner = T::from_array_ref(array.values());
            let mut current_position = 0;

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

        let res: Box<dyn Iterator<Item = Self>> = match array.data_type() {
            DataType::List(_) => {
                let array = array.as_list::<i32>();
                Box::new(helper(array))
            }
            DataType::LargeList(_) => {
                let array = array.as_list::<i64>();
                Box::new(helper(array))
            }
            _ => {
                panic!("Expected Binary, was {:?}", array.data_type())
            }
        };
        res
    }
}
