use actix_web::{dev::Factory, web, Error, HttpResponse};
use futures::future::{ok as fut_ok, Ready};
use tinytemplate::TinyTemplate;

pub(super) fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource(&format!("{}/swagger-ui", super::version())).route(
            web::get().to(GetSwaggerUi(get_swagger_html(&super::spec_uri()))),
        ),
    );
}

static TEMPLATE: &str = include_str!("./resources/swagger-ui.html");
fn get_swagger_html(spec_uri: &str) -> Result<String, String> {
    let context = serde_json::json!({ "api_spec_uri": spec_uri });
    let mut template = TinyTemplate::new();
    template
        .add_template("swagger-ui", TEMPLATE)
        .map_err(|e| e.to_string())?;
    template
        .render("swagger-ui", &context)
        .map_err(|e| e.to_string())
}

#[derive(Clone)]
struct GetSwaggerUi(Result<String, String>);

impl
    Factory<(), Ready<Result<HttpResponse, Error>>, Result<HttpResponse, Error>>
    for GetSwaggerUi
{
    fn call(&self, _: ()) -> Ready<Result<HttpResponse, Error>> {
        match &self.0 {
            Ok(html) => {
                fut_ok(HttpResponse::Ok().content_type("text/html").body(html))
            }
            Err(error) => fut_ok(
                HttpResponse::NotFound()
                    .content_type("application/json")
                    .body(serde_json::json!({ "error_message": error })),
            ),
        }
    }
}
