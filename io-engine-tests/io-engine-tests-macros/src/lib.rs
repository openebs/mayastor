use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// All tests marked with this attribute will run in the same single, specially
/// designated thread. This allows one test to initialize SPDK and other tests
/// to use SPDK, regardless of which system thread `cargo test` will assign
/// to them.
/// The test marked with thid attribute expands as a tokio async test.
#[proc_macro_attribute]
pub fn spdk_test(_args: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemFn);
    let orig = item.clone();
    let fn_ident = item.sig.ident;

    quote! {
        #[tokio::test]
        async fn #fn_ident () {
            #orig

            io_engine_tests::test_task::run_single_thread_test_task(|| {
                #fn_ident();
            }).await;
        }
    }
    .into()
}
