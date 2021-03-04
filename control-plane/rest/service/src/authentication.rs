use actix_web::{Error, HttpRequest};
use jsonwebtoken::{crypto, Algorithm, DecodingKey};
use std::str::FromStr;

use http::HeaderValue;
use std::fs::File;

/// Initialise JWK with the contents of the file at 'jwk_path'.
/// If jwk_path is 'None', authentication is disabled.
pub fn init(jwk_path: Option<String>) -> JsonWebKey {
    match jwk_path {
        Some(path) => {
            let jwk_file = File::open(path).expect("Failed to open JWK file");
            let jwk = serde_json::from_reader(jwk_file)
                .expect("Failed to deserialise JWK");
            JsonWebKey {
                jwk,
            }
        }
        None => JsonWebKey {
            ..Default::default()
        },
    }
}

#[derive(Default, Debug)]
pub struct JsonWebKey {
    jwk: serde_json::Value,
}

impl JsonWebKey {
    // Returns true if REST calls should be authenticated.
    fn auth_enabled(&self) -> bool {
        !self.jwk.is_null()
    }

    // Return the algorithm.
    fn algorithm(&self) -> Algorithm {
        Algorithm::from_str(self.jwk["alg"].as_str().unwrap()).unwrap()
    }

    // Return the modulus.
    fn modulus(&self) -> &str {
        self.jwk["n"].as_str().unwrap()
    }

    // Return the exponent.
    fn exponent(&self) -> &str {
        self.jwk["e"].as_str().unwrap()
    }

    // Return the decoding key
    fn decoding_key(&self) -> DecodingKey {
        DecodingKey::from_rsa_components(self.modulus(), self.exponent())
    }
}

/// Authenticate the HTTP request by checking the authorisation token to ensure
/// the sender is who they claim to be.
pub fn authenticate(req: &HttpRequest) -> Result<(), Error> {
    let jwk: &JsonWebKey = req.app_data().unwrap();

    // If authentication is disabled there is nothing to do.
    if !jwk.auth_enabled() {
        return Ok(());
    }

    match req.headers().get(http::header::AUTHORIZATION) {
        Some(token) => validate(&format_token(token), jwk),
        None => {
            tracing::error!("Missing bearer token in HTTP request.");
            Err(Error::from(actix_web::HttpResponse::Unauthorized()))
        }
    }
}

// Ensure the token is formatted correctly by removing the "Bearer" prefix if
// present.
fn format_token(token: &HeaderValue) -> String {
    let token = token
        .to_str()
        .expect("Failed to convert token to string")
        .replace("Bearer", "");
    token.trim().into()
}

/// Validate a bearer token.
pub fn validate(token: &str, jwk: &JsonWebKey) -> Result<(), Error> {
    let (message, signature) = split_token(&token);
    return match crypto::verify(
        &signature,
        &message,
        &jwk.decoding_key(),
        jwk.algorithm(),
    ) {
        Ok(true) => Ok(()),
        Ok(false) => {
            tracing::error!("Signature verification failed.");
            Err(Error::from(actix_web::HttpResponse::Unauthorized()))
        }
        Err(e) => {
            tracing::error!(
                "Failed to complete signature verification with error {}",
                e
            );
            Err(Error::from(actix_web::HttpResponse::Unauthorized()))
        }
    };
}

// Split the JSON Web Token (JWT) into 2 parts, message and signature.
// The message comprises the header and payload.
//
// JWT format:
//      <header>.<payload>.<signature>
//      \______  ________/
//             \/
//           message
fn split_token(token: &str) -> (String, String) {
    let elems = token.split('.').collect::<Vec<&str>>();
    let message = format!("{}.{}", elems[0], elems[1]);
    let signature = elems[2];
    (message, signature.into())
}

#[test]
fn validate_test() {
    let token_file = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("authentication")
        .join("token");
    let mut token = std::fs::read_to_string(token_file)
        .expect("Failed to get bearer token");
    let jwk_file = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("authentication")
        .join("jwk");
    let jwk = init(Some(jwk_file.to_str().unwrap().into()));

    validate(&token, &jwk).expect("Validation should pass");
    // create invalid token
    token.push_str("invalid");
    validate(&token, &jwk)
        .expect_err("Validation should fail with an invalid token");
}
