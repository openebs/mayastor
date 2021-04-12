//! Simple utility functions to help with parsing URIs.

use std::str::ParseBoolError;

use url::Url;

pub(crate) fn segments(url: &Url) -> Vec<&str> {
    if let Some(iter) = url.path_segments() {
        let mut segments: Vec<&str> = iter.collect();

        if segments.len() == 1 && segments[0].is_empty() {
            segments.remove(0);
        }

        return segments;
    }

    Vec::new()
}

/// Parse a value that represents a boolean
/// Acceptable values are: true, false, yes, no, on, off
/// Also accept an (unsigned) integer, where 0 represents false
/// and any other value represents true
pub(crate) fn boolean(
    value: &str,
    empty: bool,
) -> Result<bool, ParseBoolError> {
    if value.is_empty() {
        return Ok(empty);
    }
    if value == "yes" || value == "on" {
        return Ok(true);
    }
    if value == "no" || value == "off" {
        return Ok(false);
    }
    if let Ok(number) = value.parse::<u32>() {
        return Ok(number > 0);
    }
    value.parse::<bool>()
}

pub(crate) fn uuid(
    value: Option<String>,
) -> Result<Option<uuid::Uuid>, uuid::Error> {
    value.map(|uuid| uuid::Uuid::parse_str(&uuid)).transpose()
}
