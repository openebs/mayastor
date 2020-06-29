//! Definitions of objects that may be stored on the "MayaMeta" partition.
//! Note that the definitions provided here are purely for demonstration
//! (and testing) purposes at present.
//! The intent is that these structures will define precisely what
//! content is to be stored on the "MayaMeta" partition.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone)]
pub struct NexusConfigVersion1 {
    pub name: String,
    pub tags: Vec<String>,
    pub revision: u32,
    pub checksum: u32,
    pub data: String,
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone)]
pub struct NexusConfigVersion2 {
    pub name: String,
    pub tags: Vec<String>,
    pub revision: u32,
    pub checksum: u32,
    pub data: String,
    pub count: u16,
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone)]
pub struct NexusConfigVersion3 {
    pub name: String,
    pub revision: u32,
    pub checksum: u32,
    pub data: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize, Clone)]
pub enum NexusConfig {
    Version1(NexusConfigVersion1),
    Version2(NexusConfigVersion2),
    Version3(NexusConfigVersion3),
    Version4(HashMap<String, String>),
}
