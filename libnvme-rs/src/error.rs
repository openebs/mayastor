use snafu::Snafu;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
#[snafu(visibility = "pub(crate)")]
pub enum NvmeError {
    #[snafu(display("IO error:"))]
    IoError { source: std::io::Error },
    #[snafu(display("Lookup host failed: {}", rc))]
    LookupHostError { rc: i32 },
    #[snafu(display("Create controller failed: {}", rc))]
    CreateCtrlrError { rc: i32 },
    #[snafu(display("No controller found: {}", rc))]
    AddCtrlrError { rc: i32 },
    #[snafu(display("File IO error: {}", rc))]
    FileIoError { rc: i32 },
    #[snafu(display("NVMe URL invalid: {}", source))]
    UrlError { source: url::ParseError },
}

impl From<std::io::Error> for NvmeError {
    fn from(source: std::io::Error) -> NvmeError {
        NvmeError::IoError {
            source,
        }
    }
}
