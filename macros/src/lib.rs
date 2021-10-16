extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, DeriveInput, Ident, Result};

#[proc_macro_attribute]
pub fn AnyFlowNode(params: TokenStream, code: TokenStream) -> TokenStream {
    let pp = code.clone();
    let input = parse_macro_input!(pp as DeriveInput);
    let struct_name = &input.ident.to_string();
    println!("xxohiohhoihiox: {:?}", struct_name);

    let x = format!(
        r#"
    {code}
    #[async_trait]
    impl AsyncNode for ANode {{
        // type Params = {params};
    // fn deserialize(self, params_ptr: &Box<RawValue>) -> AnyParams {{
    //     serde_json::from_str(params_ptr.get()).unwrap()
    // }}

    async fn handle<'a, E: Send + Sync>(
        self,
        graph_args: &'a Arc<E>,
        input: Arc<NodeResult>,
        // params: Arc<AnyParams>,
    ) -> NodeResult {{

        return self.handle_wrapper(graph_args, input).await;
    }}

    fn name() -> &'static str {{
        return "{struct_name}"
    }}
}}
"#,
        params = params.to_string(),
        code = code.to_string(),
        struct_name = struct_name,
    );

    x.parse().expect("Generated invalid tokens")
}
