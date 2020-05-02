use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone)]
pub struct NexusConfigVersion1 {
    pub name: String,
    pub labels: Vec<String>,
    pub revision: u32,
    pub checksum: u32,
    pub data: String,
}

#[derive(Debug, Deserialize, PartialEq, Default, Serialize, Clone)]
pub struct NexusConfigVersion2 {
    pub name: String,
    pub labels: Vec<String>,
    pub revision: u32,
    pub checksum: u32,
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
