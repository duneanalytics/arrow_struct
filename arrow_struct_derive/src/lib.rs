extern crate proc_macro;

use convert_case::{Case, Casing};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DeriveInput, Fields, GenericParam, LifetimeParam};

#[proc_macro_derive(Deserialize)]
pub fn derive_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;
    let (_, ty_generics, where_clause) = input.generics.split_for_impl();

    // We add our reserved lifetime parameter 'ar (like 'de of serde::Deserialize) and add all existing lifetimes as bounds
    let lt = syn::Lifetime::new("'ar", Span::call_site());
    let mut ltp = LifetimeParam::new(lt);
    for lifetime in input.generics.lifetimes() {
        ltp.bounds.push(lifetime.lifetime.clone())
    }
    let mut new_generics = input.generics.clone();
    new_generics.params.push(GenericParam::Lifetime(ltp));
    let (impl_generics, _, _) = new_generics.split_for_impl();

    let inner = inner_implementation(&input.data);

    let expanded = quote! {
        impl #impl_generics arrow_struct::FromArrayRef<'ar> for #name #ty_generics #where_clause {
            fn from_array_ref(array: &'ar arrow_struct::ArrayRef) -> impl Iterator<Item=Self> {
                let array = arrow_struct::AsArray::as_struct(array);

                #inner
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

fn inner_implementation(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let idents = fields.named.iter().map(|f| f.ident.clone());
                let idents_clone = idents.clone();

                let iterators = fields.named.iter().map(|field| {
                    let name = field.ident.as_ref().unwrap().to_string();
                    let field_type = field.ty.clone();
                    let column_name = name.clone(); //.to_case(Case::Camel);
                    let iterator_name = format_ident!("__arrow_struct_derive_{}", name);

                    let iterator_declaration = quote_spanned! {field.span()=>
                        let mut #iterator_name = {
                            let array = array.column_by_name(#column_name)
                                             .expect(stringify!(no column named #column_name));
                            <#field_type as arrow_struct::FromArrayRef>::from_array_ref(array)
                        };
                    };
                    (iterator_name, iterator_declaration)
                });

                let iterator_declarations = iterators.clone().map(|(_, declaration)| declaration);
                let iterator_next = iterators.clone().map(|(name, _)| quote! { #name.next() });

                quote! {
                    #(#iterator_declarations)*

                    std::iter::from_fn(move || {
                        if let (#(Some(#idents)),*) = (#(#iterator_next),*) {
                            Some(Self { #(#idents_clone),* })
                        } else {
                            None
                        }
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
