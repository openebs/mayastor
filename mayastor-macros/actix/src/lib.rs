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
        if attr.len() < 3 {
            TokenStream::new()
        } else {
            // remove the base URI path
            attr.remove(0);
            // remove the relative URI path
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
    /// URI with the full path used to register the handler
    fn handler_uri(attr: TokenStream) -> TokenStream {
        let mut attr = parse_macro_input!(attr as syn::AttributeArgs);
        let base = attr.first().to_token_stream().to_string();
        attr.remove(0);
        let uri = attr.first().to_token_stream().to_string();
        let base_unquoted = base.trim_matches('"');
        let uri_unquoted = uri.trim_matches('"');
        let handler_uri = format!("{}{}", base_unquoted, uri_unquoted);
        let handler_uri_token = quote! {
            #handler_uri
        };
        handler_uri_token.into()
    }
    /// relative URI (full URI minus the openapi base path)
    fn openapi_uri(attr: TokenStream) -> TokenStream {
        let mut attr = parse_macro_input!(attr as syn::AttributeArgs);
        // remove the Base Path
        attr.remove(0);
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
        let full_uri: TokenStream2 = Self::handler_uri(attr.clone()).into();
        let relative_uri: TokenStream2 = Self::openapi_uri(attr.clone()).into();
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
                    paperclip::actix::web::Resource::new(#full_uri)
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
                    #relative_uri
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
- `"base"` - Raw literal string with the handler base path used by the openapi `paths`.
- `"path"` - Raw literal string representing the uri path for which to register the handler
 when combined with the base path.
- any paperclip api_v2_operation attributes.

# Example

```rust
# use actix_web::Json;
# use mayastor_macros::"#, stringify!($method), ";
#[", stringify!($method), r#"("", "/")]
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
