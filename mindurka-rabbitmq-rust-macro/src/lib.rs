use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitInt, LitStr, parse_macro_input};

#[derive(Debug, FromMeta)]
#[darling(derive_syn_parse)]
struct NetworkEventArgs {
    pub queue: String,
    #[darling(default = || 60)]
    pub ttl: u64,
}

#[proc_macro_attribute]
pub fn network_event(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse::<NetworkEventArgs>(attr) {
        Ok(val) => val,
        Err(err) => return err.to_compile_error().into(),
    };

    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let queue_lit = LitStr::new(&args.queue, name.span());

    let ttl_lit = LitInt::new(&args.ttl.to_string(), name.span());

    let expanded = quote! {
        #input

        impl #impl_generics NetworkEvent for #name #ty_generics #where_clause {
            const QUEUE: &str = #queue_lit;
            const TTL: u64 = #ttl_lit;
            const CONSUMER_SERVICE: &str = env!("CARGO_PKG_NAME");
        }
    };

    TokenStream::from(expanded)
}
