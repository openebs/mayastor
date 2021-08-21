use snafu::Snafu;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
#[snafu(visibility = "pub(crate)")]
pub enum NvmeError {
    #[snafu(display("IO error:"))]
    IoError { source: std::io::Error },
    #[snafu(display("Failed to parse {}: {}, {}", path, contents, error))]
    ValueParseError {
        path: String,
        contents: String,
        error: String,
    },
    #[snafu(display("Failed to parse value"))]
    ParseError {},
    #[snafu(display("File IO error: {}, {}", filename, source))]
    FileIoError {
        filename: String,
        source: std::io::Error,
    },
    #[snafu(display("nqn: {} not found", text))]
    NqnNotFound { text: String },
    #[snafu(display("No nvmf subsystems found"))]
    NoSubsystems,
    #[snafu(display("Connect in progress"))]
    ConnectInProgress,
    #[snafu(display("NVMe connect failed: {}, {}", filename, source))]
    ConnectError {
        source: std::io::Error,
        filename: String,
    },
    #[snafu(display("IO error during NVMe discovery"))]
    DiscoveryError { source: nix::Error },
    #[snafu(display("Controller with nqn: {} not found", text))]
    CtlNotFound { text: String },
    #[snafu(display("Invalid path {}: {}", path, source))]
    InvalidPath {
        source: std::path::StripPrefixError,
        path: String,
    },
    #[snafu(display("NVMe subsystems error: {}, {}", path_prefix, source))]
    SubSysError {
        source: glob::PatternError,
        path_prefix: String,
    },
    #[snafu(display("NVMe URI invalid: {}", source))]
    UrlError { source: url::ParseError },
    #[snafu(display("Transport type {} not supported", trtype))]
    TransportError { trtype: String },
    #[snafu(display("Invalid parameter: {}", text))]
    InvalidParam { text: String },
}

impl From<std::io::Error> for NvmeError {
    fn from(source: std::io::Error) -> NvmeError {
        NvmeError::IoError { source }
    }
}
