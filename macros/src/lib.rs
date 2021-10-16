extern crate proc_macro;
use proc_macro::{TokenStream};
use quote::{quote};
#[proc_macro_derive(AnyFlowNode, attributes())]
pub fn AnyFlowNode(_metadata: TokenStream) -> TokenStream {
    println!("_metadata {:?}", _metadata);
    TokenStream::from(quote! {
        impl AsyncNode for ANode {
        fn deserialize(params_ptr: &Box<RawValue>) -> AnyParams {
            serde_json::from_str(params_ptr.get()).unwrap()
        }
    }
    })
}