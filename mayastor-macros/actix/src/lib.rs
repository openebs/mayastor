use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, ItemFn};

macro_rules! doc_comment {
    ($x:expr; $($tt:tt)*) => {
        #[doc = $x]
        $($tt)*
    };
}

impl Method {
    // removes the URI from the attributes and collects the rest
    // so they can be used with the paperclip::actix::api_v2_operation
    fn paperclip_attributes(attr: TokenStream) -> TokenStream {
        let mut attr = parse_macro_input!(attr as syn::AttributeArgs);
        if attr.len() < 2 {
            TokenStream::new()
        } else {
            // remove the URI path
            attr.remove(0);
            let mut paperclip_attr = "".to_string();
            for i in attr {
                paperclip_attr.push_str(&format!(
                    "{},",
                    i.into_token_stream().to_string()
                ));
            }
            paperclip_attr.parse().unwrap()
        }
    }
    fn handler_uri(attr: TokenStream) -> TokenStream {
        let attr = parse_macro_input!(attr as syn::AttributeArgs);
        attr.first().into_token_stream().into()
    }
    fn handler_name(item: TokenStream) -> syn::Result<syn::Ident> {
        let handler: ItemFn = syn::parse(item)?;
        Ok(handler.sig.ident)
    }
    fn generate(
        &self,
        attr: TokenStream,
        item: TokenStream,
    ) -> syn::Result<TokenStream2> {
        let uri: TokenStream2 = Self::handler_uri(attr.clone()).into();
        let handler_name = Self::handler_name(item.clone())?;
        let handler_fn: TokenStream2 = item.into();
        let method: TokenStream2 = self.method().parse()?;
        let variant: TokenStream2 = self.variant().parse()?;
        let handler_name_str = handler_name.to_string();
        let attr: TokenStream2 = Self::paperclip_attributes(attr).into();

        Ok(quote! {
            #[allow(non_camel_case_types, missing_docs)]
            pub struct #handler_name;

            impl #handler_name {
                fn resource() -> paperclip::actix::web::Resource {
                    #[paperclip::actix::api_v2_operation(#attr)]
                    #handler_fn
                    paperclip::actix::web::Resource::new(#uri)
                        .name(#handler_name_str)
                        .guard(actix_web::guard::#variant())
                        .route(paperclip::actix::web::#method().to(#handler_name))
                }
            }

            impl actix_web::dev::HttpServiceFactory for #handler_name {
                fn register(self, config: &mut actix_web::dev::AppService) {
                    Self::resource().register(config);
                }
            }

            impl paperclip::actix::Mountable for #handler_name {
                fn path(&self) -> &str {
                    #uri
                }

                fn operations(
                    &mut self,
                ) -> std::collections::BTreeMap<
                    paperclip::v2::models::HttpMethod,
                    paperclip::v2::models::DefaultOperationRaw,
                > {
                    Self::resource().operations()
                }

                fn definitions(
                    &mut self,
                ) -> std::collections::BTreeMap<
                    String,
                    paperclip::v2::models::DefaultSchemaRaw,
                > {
                    Self::resource().definitions()
                }

                fn security_definitions(
                    &mut self,
                ) -> std::collections::BTreeMap<String, paperclip::v2::models::SecurityScheme>
                {
                    Self::resource().security_definitions()
                }
            }
        })
    }
}

macro_rules! rest_methods {
    (
        $($variant:ident, $method:ident, )+
    ) => {
        /// All available Rest methods
        #[derive(Debug, PartialEq, Eq, Hash)]
        enum Method {
            $(
                $variant,
            )+
        }

        impl Method {
            fn method(&self) -> &'static str {
                match self {
                    $(Self::$variant => stringify!($method),)+
                }
            }
            fn variant(&self) -> &'static str {
                match self {
                    $(Self::$variant => stringify!($variant),)+
                }
            }
        }

        $(doc_comment! {
            concat!("
Creates route handler with `paperclip::actix::web::Resource", "`.
In order to control the output type and status codes the return value/response must implement the
trait actix_web::Responder.

# Syntax
```text
#[", stringify!($method), r#"("path"[, attributes])]
```

# Attributes
- `"path"` - Raw literal string with path for which to register handler.
- any paperclip api_v2_operation attributes

# Example

```rust
# use actix_web::Json;
# use mayastor_macros::"#, stringify!($method), ";
#[", stringify!($method), r#"("/")]
async fn example() -> Json<()> {
    Json(())
}
```
"#);
            #[proc_macro_attribute]
            pub fn $method(attr: TokenStream, item: TokenStream) -> TokenStream {
                match Method::$variant.generate(attr, item) {
                    Ok(v) => v.into(),
                    Err(e) => e.to_compile_error().into(),
                }
            }
        })+
    };
}

rest_methods! {
    Get,    get,
    Post,   post,
    Put,    put,
    Delete, delete,
}
