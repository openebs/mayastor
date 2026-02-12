/// Possible device states are SUSPENDED, ACTIVE, and READ-ONLY.
/// The dmsetup suspend command sets a device state to SUSPENDED.
/// When a device is suspended, all I/O operations to that device stop.
/// The dmsetup resume command restores a device state to ACTIVE.
#[derive(Eq, PartialEq)]
pub enum DmState {
    Suspended,
    Active,
    ReadOnly,
    Unknown(String),
}
impl From<String> for DmState {
    fn from(value: String) -> Self {
        let state = value.to_uppercase();
        match state.as_str() {
            "SUSPENDED" => Self::Suspended,
            "ACTIVE" => Self::Active,
            "READ-ONLY" => Self::ReadOnly,
            _ => Self::Unknown(state),
        }
    }
}
impl std::fmt::Display for DmState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            DmState::Suspended => "Suspended",
            DmState::Active => "Active",
            DmState::ReadOnly => "Read-Only",
            DmState::Unknown(state) => state.as_str(),
        };
        write!(f, "{state}")
    }
}

/// A device-mapper setup table.
#[derive(Debug)]
pub struct DmTable(pub String);

impl std::fmt::Display for DmTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Helper methods for running dmsetup commands.
pub struct DmSetup;

impl DmSetup {
    /// Suspends the device.
    ///
    /// # NOTES
    ///
    /// Any I/O that has already been mapped by the device but has not yet completed will be flushed. \
    /// Any further I/O to that device will be postponed for as long as the device is suspended. \
    /// If there's a filesystem on the device which supports the operation, an attempt will be made
    /// to sync it first unless --nolockfs is specified. \
    /// Some targets such as recent (October 2006) versions of multipath may support the --noflush option.
    /// This lets outstanding I/O that has not yet reached the device to remain unflushed.
    pub async fn suspend(path: &str) -> Result<(), super::Error> {
        super::cli::LvmCmd::dm_setup()
            .arg("suspend")
            .arg(path)
            .run()
            .await?;
        Ok(())
    }
    /// Resumes/Un-suspends the device.
    ///
    /// # NOTES
    ///
    /// If an inactive table has been loaded, it becomes live. \
    /// Postponed I/O then gets re-queued for processing.
    pub async fn resume(path: &str) -> Result<(), super::Error> {
        super::cli::LvmCmd::dm_setup()
            .arg("resume")
            .arg(path)
            .run()
            .await?;
        Ok(())
    }

    /// Retrieve the currently applied [`DmTable`] of the device.
    ///
    /// Outputs the current table for the device in a format that can be fed back in using the create or load commands. \
    /// With --target, only information relating to the specified target type is displayed. \
    /// Real encryption keys are suppressed in the table output for crypt and integrity targets unless the --showkeys parameter is supplied. \
    /// Kernel key references prefixed with : are not affected  by  the parameter  and get displayed always (crypt target only).
    /// With --concise, the output is presented concisely on a single line. \
    /// Commas then separate the name, uuid, minor device number, flags ('ro' or 'rw') and the table (if present).
    /// Semi-colons separate devices.  \
    /// Backslashes escape any commas, semi-colons or backslashes.
    pub async fn table(path: &str) -> Result<DmTable, super::Error> {
        let output = super::cli::LvmCmd::dm_setup()
            .arg("table")
            .arg(path)
            .output()
            .await?;
        let output_str = String::from_utf8_lossy(&output.stdout).to_string();
        let table_str = output_str.trim_end();
        Ok(DmTable(table_str.to_string()))
    }

    /// Loads a [`DmTable`].
    pub async fn load(path: &str, table: DmTable) -> Result<(), super::Error> {
        let _output = super::cli::LvmCmd::dm_setup()
            .arg("load")
            .arg(path)
            .input(table.to_string())
            .output()
            .await?;
        Ok(())
    }

    /// Removes a device-mapper device.
    ///
    /// # NOTES
    ///
    /// It will no longer be visible to dmsetup.
    ///
    /// Open devices cannot be removed, but:
    /// - adding --force will replace the table with one that fails all I/O.
    /// - --deferred will enable deferred removal of open devices - the device will be removed when
    ///   the last user closes it.
    ///
    /// As is, this method will remove stale devices.
    pub async fn remove(path: &str) -> Result<(), super::Error> {
        let _output = super::cli::LvmCmd::dm_setup()
            .arg("remove")
            .arg(path)
            .output()
            .await?;
        Ok(())
    }

    /// Get the [`DmState`] of the given device-mapper device path.
    pub async fn state(path: &str) -> Result<DmState, super::Error> {
        let output = super::cli::LvmCmd::dm_setup()
            .arg("info")
            .arg("-C")
            .arg("-osuspended")
            .arg("--noheadings")
            .arg(path)
            .output()
            .await?;
        let state = String::from_utf8_lossy(&output.stdout).to_string();
        Ok(DmState::from(state.trim_end().to_string()))
    }
}
