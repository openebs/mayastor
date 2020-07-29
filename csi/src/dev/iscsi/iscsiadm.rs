//! Utility functions that provide a simple interface to the iscsiadm command.
//! Contains (public) functions for performing each of the various iSCSI
//! commands that we require.

use std::{env, process::Command};

use regex::Regex;

use super::DeviceError;

pub(super) struct IscsiAdmin;

impl IscsiAdmin {
    pub(super) fn find_session(
        portal: &str,
        iqn: &str,
    ) -> Result<bool, DeviceError> {
        const ARGS: [&str; 2] = ["--mode", "session"];

        let iscsiadm = IscsiAdmin::get_binary()?;

        trace!("iscsiadm {:?}", &ARGS);

        let output = Command::new(iscsiadm).args(&ARGS).output()?;

        if output.status.success() {
            return Ok(IscsiAdmin::find_target(portal, iqn, output.stdout));
        }

        if output.status.code() == Some(21) {
            // An exit code of 21 corresponds to ISCSI_ERR_NO_OBJS_FOUND.
            // In this case that means that no iSCSI sessions were found,
            // and this SHOULD NOT be considered an error.
            return Ok(false);
        }

        Err(DeviceError::from(String::from_utf8(output.stderr).unwrap()))
    }

    pub(super) fn discover(portal: &str, iqn: &str) -> Result<(), DeviceError> {
        let iscsiadm = IscsiAdmin::get_binary()?;

        let args = [
            "--mode",
            "discovery",
            "--type",
            "sendtargets",
            "--portal",
            portal,
            "--interface",
            "default",
        ];

        trace!("iscsiadm {:?}", &args);

        let output = Command::new(iscsiadm).args(&args).output()?;

        if output.status.success() {
            if IscsiAdmin::find_target(portal, iqn, output.stdout) {
                return Ok(());
            }

            return Err(DeviceError::from(format!(
                "no record for target {}",
                iqn
            )));
        }

        Err(DeviceError::from(String::from_utf8(output.stderr).unwrap()))
    }

    pub(super) fn login(portal: &str, iqn: &str) -> Result<(), DeviceError> {
        let args = [
            "--mode",
            "node",
            "--targetname",
            iqn,
            "--portal",
            portal,
            "--interface",
            "default",
            "--login",
        ];
        IscsiAdmin::execute(&args)
    }

    pub(super) fn logout(portal: &str, iqn: &str) -> Result<(), DeviceError> {
        let args = [
            "--mode",
            "node",
            "--targetname",
            iqn,
            "--portal",
            portal,
            "--logout",
        ];
        IscsiAdmin::execute(&args)
    }

    pub(super) fn delete(portal: &str, iqn: &str) -> Result<(), DeviceError> {
        let args = [
            "--mode",
            "node",
            "--targetname",
            iqn,
            "--portal",
            portal,
            "--interface",
            "default",
            "--op",
            "delete",
        ];
        IscsiAdmin::execute(&args)
    }

    fn execute(args: &[&str]) -> Result<(), DeviceError> {
        let iscsiadm = IscsiAdmin::get_binary()?;

        trace!("iscsiadm {:?}", args);

        let output = Command::new(iscsiadm).args(args).output()?;

        if output.status.success() {
            return Ok(());
        }

        Err(DeviceError::from(String::from_utf8(output.stderr).unwrap()))
    }

    fn find_target(portal: &str, iqn: &str, data: Vec<u8>) -> bool {
        lazy_static! {
            static ref PATTERN: Regex = Regex::new(r"(?P<portal>[[:digit:]]+(\.[[:digit:]]+){3}:[[:digit:]]+),[[:digit:]]+ +(?P<target>iqn\.[^ ]+)").unwrap();
        }

        for line in String::from_utf8(data).unwrap().split('\n') {
            if let Some(captures) = PATTERN.captures(line) {
                if captures.name("portal").unwrap().as_str() == portal
                    && captures.name("target").unwrap().as_str() == iqn
                {
                    return true;
                }
            }
        }

        false
    }

    fn get_binary() -> Result<&'static str, DeviceError> {
        const MAYASTOR_ISCSIADM: &str = "/bin/mayastor-iscsiadm";

        lazy_static! {
            static ref ISCSIADM: String = match env::var("ISCSIADM") {
                Ok(path) => {
                    debug!("using environment: ISCSIADM={}", &path);
                    path
                }
                _ => match which::which(MAYASTOR_ISCSIADM) {
                    Ok(_) => {
                        debug!(
                            "using hardcoded default: {}",
                            MAYASTOR_ISCSIADM
                        );
                        String::from(MAYASTOR_ISCSIADM)
                    }
                    _ => match which::which("iscsiadm") {
                        Ok(path) => {
                            debug!("using PATH: {:?}", path);
                            String::from("iscsiadm")
                        }
                        _ => {
                            debug!("iscsiadm binary not found");
                            String::from("")
                        }
                    },
                },
            };
        }

        if ISCSIADM.is_empty() {
            return Err(DeviceError::new("iscsiadm binary not found"));
        }

        Ok(&ISCSIADM)
    }
}
