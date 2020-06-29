//! Simple utility functions to help with parsing URIs.

use std::{collections::HashMap, str::ParseBoolError};

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

/// Generate a comma separated list of all keys present in a HashMap as a String
pub(crate) fn keys(map: HashMap<String, String>) -> Option<String> {
    if map.is_empty() {
        None
    } else {
        Some(
            map.keys()
                .map(|key| key.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
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
