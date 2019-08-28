///! Utility functions for reading and modifying the state of sysfs
/// objects.
use std::path::Path;
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader, Error, ErrorKind, Result},
    str::FromStr,
    string,
};

/// Read and parse value from a file
pub fn parse_value<T>(dir: &Path, file: &str) -> Result<T>
where
    T: FromStr,
{
    let path = dir.join(file);
    let s = fs::read_to_string(&path)?;
    let s = s.trim();
    match s.parse() {
        Ok(v) => Ok(v),
        Err(_) => Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "Failed to parse {}: {}",
                path.as_path().to_str().unwrap(),
                s
            ),
        )),
    }
}

/// Write string to a file
pub fn write_value<T>(dir: &Path, file: &str, content: T) -> Result<()>
where
    T: string::ToString,
{
    let path = dir.join(file);
    fs::write(path, content.to_string())
}

/// Read dictionary format from a file. Example:
///  KEY1=val1
///  KEY2=val2
///  ...
pub fn parse_dict(dir: &Path, file: &str) -> Result<HashMap<String, String>> {
    let path = dir.join(file);
    let mut dict = HashMap::new();
    let f = fs::File::open(&path)?;
    let file = BufReader::new(&f);

    for line in file.lines() {
        let line = line.unwrap();
        let parts: Vec<&str> = line.split('=').collect();
        if parts.len() == 2 {
            dict.insert(parts[0].to_string(), parts[1].to_string());
        }
    }
    Ok(dict)
}
