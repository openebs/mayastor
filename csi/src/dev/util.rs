use regex::Regex;
use uuid::{Error, Uuid};

pub(super) fn extract_uuid(value: &str) -> Result<Uuid, Error> {
    lazy_static! {
        static ref PATTERN: Regex = Regex::new(r"^[[:xdigit:]]+-").unwrap();
    }

    if PATTERN.is_match(value) {
        if let Ok(uuid) = Uuid::parse_str(value) {
            return Ok(uuid);
        }
    }

    let mut components: Vec<&str> = value.split('-').collect();

    if !components.is_empty() {
        components.remove(0);
    }

    Uuid::parse_str(&components.join("-"))
}
