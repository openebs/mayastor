//! Definition of DeviceError used by the attach and detach code.
use nvmeadm::nvmf_discovery;
use std::string::FromUtf8Error;

pub struct DeviceError {
    pub message: String,
}

impl DeviceError {
    pub fn new(message: &str) -> DeviceError {
        DeviceError {
            message: String::from(message),
        }
    }
}

impl std::fmt::Debug for DeviceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::fmt::Display for DeviceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for DeviceError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl From<std::io::Error> for DeviceError {
    fn from(error: std::io::Error) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}

impl From<failure::Error> for DeviceError {
    fn from(error: failure::Error) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}

impl From<std::num::ParseIntError> for DeviceError {
    fn from(error: std::num::ParseIntError) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}

impl From<uuid::Error> for DeviceError {
    fn from(error: uuid::Error) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}

impl From<nvmf_discovery::ConnectArgsBuilderError> for DeviceError {
    fn from(error: nvmf_discovery::ConnectArgsBuilderError) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}

impl From<String> for DeviceError {
    fn from(message: String) -> DeviceError {
        DeviceError { message }
    }
}

impl From<serde_json::error::Error> for DeviceError {
    fn from(error: serde_json::error::Error) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}

impl From<FromUtf8Error> for DeviceError {
    fn from(error: FromUtf8Error) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}

impl From<nvmeadm::error::NvmeError> for DeviceError {
    fn from(error: nvmeadm::error::NvmeError) -> DeviceError {
        DeviceError {
            message: format!("{}", error),
        }
    }
}
