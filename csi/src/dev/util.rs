use regex::Regex;
use uuid::{parser::ParseError, Uuid};

pub(super) fn extract_uuid(value: &str) -> Result<Uuid, ParseError> {
    lazy_static! {
        static ref PATTERN: Regex = Regex::new(r"^[[:xdigit:]]$").unwrap();
    }

    let mut components: Vec<&str> = value.split('-').collect();

    if !components.is_empty() && !PATTERN.is_match(components[0]) {
        components.remove(0);
    }

    Uuid::parse_str(&components.join("-"))
}
