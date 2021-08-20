use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Failed to parse {}", err))]
    FailedParsing { err: String },
    #[snafu(display("Failed to execute command {}", err))]
    FailedExec { err: String },
    #[snafu(display("I/O error: {}", err))]
    Io { err: std::io::Error },
    #[snafu(display("Invalid PoolType {}", value))]
    InvalidPoolType { value: i32 },
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::FailedExec {
            err: e.to_string(),
        }
    }
}
