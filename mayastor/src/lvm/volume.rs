use crate::lvm::error::Error;
use serde::de::{Deserialize, Deserializer};
use tokio::process::Command;
use rpc::mayastor::{CreateReplicaRequest};
use crate::lvm::pool::{
    MAYASTOR_TAG,
    MAYASTOR_LABEL,
    deserialize_number_from_string,
};


const LVCREATE_COMMAND: &str = "lvcreate";
const LVS_COMMAND: &str = "lvs";
const LVREMOVE_COMMAND: &str = "lvremove";
const LVCHANGE_COMMAND: &str = "lvchange";

fn deserialize_vec_from_string_sequence<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
    where
        D: Deserializer<'de>,
{
    let sequence = String::deserialize(deserializer)?;
    Ok(
        sequence
        .split(",")
        .map(|item| item.to_owned())
        .collect()
    )
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalVolume {
    lv_uuid: String,
    lv_name: String,
    vg_name: String,
    lv_path: String,
    #[serde(rename = "lv_size")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    size: u64,
    #[serde(rename = "lv_tags")]
    #[serde(deserialize_with = "deserialize_vec_from_string_sequence")]
    tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogicalVolumeList {
    lv: Vec<LogicalVolume>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogicalVolsReport {
    report: Vec<LogicalVolumeList>,
}

impl LogicalVolume {

    pub async fn create(req: CreateReplicaRequest) -> Result<LogicalVolume, Error> {
        let lv_name =  req.uuid.as_str();
        let vg_name = req.pool.as_str();
        let mut size = req.size.to_string();
        // need to append the units as bytes
        size.push_str("b");

        // add the necessary tags for the lvm volume
        let mut add_tag_command: Vec<&str> = Vec::new();
        let share_tag = format!("--addtag={}", req.share);
        let mayastor_tag = format!("--addtag={}", MAYASTOR_TAG);
        add_tag_command.push(mayastor_tag.as_str());
        add_tag_command.push(share_tag.as_str());

        let output = Command::new(LVCREATE_COMMAND)
            .args(&["-L", size.as_str()])
            .args(&["-n", lv_name])
            .args(add_tag_command)
            .arg(vg_name)
            .output()
            .await?;

        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!("failed to parse stderr for lvcreate: {}", e.to_string())
                },
                |s| s,
            );
            return Err(Error::FailedExec{
                err: msg,
            });
        }

        let lv_path = format!("/dev/{}/{}", vg_name, lv_name);

        info!("lvm volume {} created", lv_path);

        Ok(
            LogicalVolume {
                lv_path,
                lv_name: req.uuid,
                vg_name: req.pool,
                size: req.size,
                lv_uuid: String::new(),
                tags: vec![format!("{}", req.share), MAYASTOR_TAG.to_string()],
            }
        )
    }

    pub async fn lookup_by_lv_name(lv_name: String) -> Option<Self> {
        Self::list("")
            .await
            .ok()?
            .iter()
            .find(|v| v.lv_name == lv_name)
            .cloned()
    }

    pub async fn list(_vg_name: &str) -> Result<Vec<LogicalVolume>, Error> {
        let args = vec![
            "--reportformat=json",
            "--options=lv_name,vg_name,lv_size,lv_uuid,lv_tags,lv_path",
            "--units=b",
            "--nosuffix",
            MAYASTOR_LABEL
        ];

        /*if !vg_name.is_empty() {
           let select_option = "select vg_name=".to_owned() + vg_name;
            args.push(select_option.as_str());
        }*/

        let output = Command::new(LVS_COMMAND)
            .args(args.as_slice())
            .output()
            .await?;

        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!("failed to parse stderr for lvs: {}", e.to_string())
                },
                |s| s,
            );
            return Err(Error::FailedExec{
                err: msg,
            });
        }

        let json_result: LogicalVolsReport = serde_json::from_slice(output.stdout.as_slice())
            .map_err(|e| Error::FailedParsing {
                err: e.to_string()
            })?;

        let volumes = json_result.report[0].lv.clone();

        Ok(volumes)
    }

    pub async fn remove(self) -> Result<(), Error> {

        let output = Command::new(LVREMOVE_COMMAND)
            .arg(self.lv_path)
            .arg("-y")
            .output()
            .await?;

        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!("failed to parse stderr for lvremove: {}", e.to_string())
                },
                |s| s,
            );
            return Err(Error::FailedExec{
                err: msg,
            });
        }

        info!("lvm volume {} deleted", self.lv_name);

        Ok(())

    }

    pub async fn change_share_tag(self, share_protocol: i32) -> Result<(), Error> {
        // remove the first tag, which corresponds to the share protocol
        Self::delete_tag(&self, self.tags[0].as_str()).await?;
        Self::add_tag(&self, format!("{}", share_protocol).as_str()).await?;
        info!("share tag changed to {} for {}", share_protocol, self.lv_name);
        Ok(())
    }

    async fn add_tag(&self, tag: &str) -> Result<(), Error> {
        let output = Command::new(LVCHANGE_COMMAND)
            .args(&["--addtag", tag])
            .arg(&self.lv_path)
            .output()
            .await?;
        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!(
                        "failed to parse stderr for lvchange addtag: {}",
                        e.to_string()
                    )
                },
                |s| s,
            );
            return Err(Error::FailedExec {
                err: msg,
            });
        }
        Ok(())
    }

    async fn delete_tag(&self, tag: &str) -> Result<(), Error> {
        let output = Command::new(LVCHANGE_COMMAND)
            .args(&["--deltag", tag])
            .arg(&self.lv_path)
            .output()
            .await?;
        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!(
                        "failed to parse stderr for lvchange deltag: {}",
                        e.to_string()
                    )
                },
                |s| s,
            );
            return Err(Error::FailedExec {
                err: msg,
            });
        }
        Ok(())
    }

    pub fn uuid(&self) -> &str {
        &self.lv_uuid
    }
    pub fn name(&self) -> &str {
        &self.lv_name
    }
    pub fn vg_name(&self) -> &str {
        &self.vg_name
    }
    pub fn lv_path(&self) -> &str {
        &self.lv_path
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn tags(&self) -> &Vec<String> {
        &self.tags
    }
    pub fn share(&self) -> i32 {
        self.tags()[0].parse::<i32>().unwrap()
    }
}