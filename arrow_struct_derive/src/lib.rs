extern crate proc_macro;

use convert_case::{Case, Casing};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use std::str::FromStr;
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DeriveInput, Fields, GenericParam, LifetimeParam};

#[derive(deluxe::ExtractAttributes, Debug, Default)]
#[deluxe(attributes(arrow_struct))]
struct Attributes {
    #[deluxe(default = String::from("none"))]
    rename_all: String,
}

#[derive(Default, Debug)]
enum RenameAll {
    #[default]
    None,
    SnakeCase,
    CamelCase,
}

impl FromStr for RenameAll {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "snake_case" => Ok(Self::SnakeCase),
            "none" => Ok(Self::None),
            "camelCase" => Ok(Self::CamelCase),
            _ => Err(format!("Unknown case: {}", s)),
        }
    }
}

impl From<RenameAll> for Option<Case> {
    fn from(value: RenameAll) -> Self {
        match value {
            RenameAll::None => None,
            RenameAll::SnakeCase => Some(Case::Snake),
            RenameAll::CamelCase => Some(Case::Camel),
        }
    }
}

#[proc_macro_derive(Deserialize, attributes(arrow_struct))]
pub fn derive_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let mut attrs = input.attrs;
    let res: Attributes = deluxe::extract_attributes(&mut attrs).unwrap();

    let rename_all: RenameAll = RenameAll::from_str(&res.rename_all).unwrap();
    let case = rename_all.into();

    let name = input.ident;
    let (plain_impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // We add our reserved lifetime parameter 'ar (like 'de of serde::Deserialize) and add all existing lifetimes as bounds
    let lt = syn::Lifetime::new("'ar", Span::call_site());
    let mut ltp = LifetimeParam::new(lt);
    for lifetime in input.generics.lifetimes() {
        ltp.bounds.push(lifetime.lifetime.clone())
    }
    let mut new_generics = input.generics.clone();
    new_generics.params.push(GenericParam::Lifetime(ltp));
    let (impl_generics, _, _) = new_generics.split_for_impl();

    let inner = inner_implementation(&input.data, case);

    let expanded = quote! {
        impl #impl_generics arrow_struct::FromArrayRefOpt<'ar> for #name #ty_generics #where_clause {
            type Item=Self;
            fn from_array_ref_opt(array: &'ar arrow_struct::ArrayRef) -> impl Iterator<Item=Option<Self>>{
                let array = arrow_struct::AsArray::as_struct(array);

                #inner
            }
        }

        impl #impl_generics arrow_struct::FromArrayRef<'ar> for #name #ty_generics #where_clause {
            fn from_array_ref(array: &'ar arrow_struct::ArrayRef) -> impl Iterator<Item=Self> {
                <#name #ty_generics as arrow_struct::FromArrayRefOpt<'ar>>::from_array_ref_opt(array)
                .map(|x| arrow_struct::Option::expect(x, stringify!(unwrap on #name)))
            }
        }

        impl #plain_impl_generics arrow_struct::NullConversion for #name #ty_generics #where_clause {
            type Item=Self;
            fn convert(item: Option<Self::Item>) -> Self {
                item.unwrap()
            }
        }

        impl #plain_impl_generics arrow_struct::NotNull for #name #ty_generics #where_clause {

        }
    };

    proc_macro::TokenStream::from(expanded)
}

fn inner_implementation(data: &Data, case: Option<Case>) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let idents = fields.named.iter().map(|f| f.ident.clone());
                let idents_clone = idents.clone();

                let iterators = fields.named.iter().map(|field| {
                    let ident = field.ident.clone();
                    let name = field.ident.as_ref().unwrap().to_string();
                    let field_type = field.ty.clone();
                    let column_name = if let Some(case) = case { name.clone().to_case(case) } else { name.clone() };
                    let iterator_name = format_ident!("__arrow_struct_derive_{}", name);

                    let iterator_declaration = quote_spanned! {field.span()=>
                        let mut #iterator_name = {
                            let array = array.column_by_name(#column_name)
                                             .expect(stringify!(no column named #column_name));
                            <#field_type as arrow_struct::FromArrayRefOpt>::from_array_ref_opt(array)
                        };
                    };
                    let conversion = quote_spanned! {field.span()=>
                        let #ident = <#field_type as arrow_struct::NullConversion>::convert(#ident);
                    };
                    (iterator_name, iterator_declaration, conversion)
                });

                let iterator_declarations =
                    iterators.clone().map(|(_, declaration, _)| declaration);
                let iterator_next = iterators
                    .clone()
                    .map(|(name, _, _)| quote! { #name.next() });
                let conversions = iterators.clone().map(|(_, _, conversion)| conversion);

                quote! {
                    // TODO: See if nulls can be used instead
                    let is_null = arrow_struct::Array::logical_nulls(array);

                    #(#iterator_declarations)*

                    let mut pos = 0;
                    std::iter::from_fn(move || {
                        let res = if let (#(Some(#idents)),*) = (#(#iterator_next),*) {
                            let is_null = is_null.as_ref().map(|x| x.is_null(pos)).unwrap_or(false);
                            if !is_null {
                                #(#conversions)*
                                Some(Some(Self { #(#idents_clone),* }))
                            } else {
                                Some(None)
                            }
                        } else {
                            None
                        };
                        pos += 1;
                        res
                    })
                }
            }
            Fields::Unnamed(_) => {
                unimplemented!()
            }
            Fields::Unit => {
                unimplemented!()
            }
        },
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    }
}
