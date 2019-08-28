//! Utility functions for working with nbd devices
use rpc::mayastor::*;

use crate::{
    csi::{NodeStageVolumeRequest, NodeStageVolumeResponse},
    device,
    format::probed_format,
    mount::{match_mount, mount_fs, Fs},
};
use enclose::enclose;
use futures::{
    future::{err, ok, Either},
    Future,
};
use glob::glob;
use jsonrpc;
use rpc::jsonrpc as jsondata;
use std::fmt;
use sysfs;
use tower_grpc::{Code, Response, Status};

use std::{path::PathBuf, sync::Mutex};

lazy_static! {
    static ref ARRAY: Mutex<Vec<u32>> =
        Mutex::new(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15]);
}

#[derive(Clone, Copy)]
pub struct NbdDevInfo {
    instance: u32,
    major: u64,
    minor: u64,
}

impl fmt::Display for NbdDevInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "/dev/nbd{}", self.instance)
    }
}

impl fmt::Debug for NbdDevInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "nbd{} ({}:{})", self.instance, self.major, self.minor)
    }
}

pub fn nbd_stage_volume(
    socket: String,
    msg: &NodeStageVolumeRequest,
    filesystem: Fs,
    mnt_opts: Vec<String>,
) -> Box<
    dyn Future<Item = Response<NodeStageVolumeResponse>, Error = Status> + Send,
> {
    //let msg = request.into_inner();

    let uuid = msg.volume_id.clone();
    let target_path = msg.staging_target_path.to_string();
    let mount_fail = msg.publish_context.contains_key("mount");

    let f = get_nbd_instance(&socket.clone(), &uuid)
        .and_then(move |nbd_disk| {
            if nbd_disk.is_none() {
                // if we dont have a nbd device with a corresponding bdev,
                // its an error ass it should
                error!("No device instance found for {}, likely a bug", &uuid);
                return err(Status::new(
                    Code::Internal,
                    "no such bdev exists".to_string(),
                ));
            }

            let nbd_disk = nbd_disk.unwrap();

            if let Some(mount) = match_mount(
                Some(&nbd_disk.nbd_device),
                Some(&target_path),
                false,
            ) {
                if mount.source == nbd_disk.nbd_device
                    && mount.dest == target_path
                {
                    // the device is already mounted we should return OK
                    return ok((true, nbd_disk, target_path, uuid));
                } else {
                    // something is there already return error
                    return err(Status::new(
                        Code::AlreadyExists,
                        "Some different BDEV on that path already".to_string(),
                    ));
                }
            }
            ok((false, nbd_disk, target_path, uuid))
        })
        .and_then(move |mounted| {
            if !mounted.0 {
                Either::A(
                    probed_format(&mounted.1.nbd_device, &filesystem.name)
                        .then(move |format_result| {
                            let mnt_result =
                                if mount_fail || format_result.is_err() {
                                    if !mount_fail {
                                        Err(format_result.unwrap_err())
                                    } else {
                                        debug!("Simulating mount failure");
                                        Err("simulated".to_owned())
                                    }
                                } else {
                                    mount_fs(
                                        &mounted.1.nbd_device,
                                        &mounted.2,
                                        false,
                                        &filesystem.name,
                                        &mnt_opts,
                                    )
                                };

                            if let Err(reason) = mnt_result {
                                Box::new(err(Status::new(
                                    Code::Internal,
                                    reason,
                                )))
                            } else {
                                info!(
                                    "staged {} on {}",
                                    &mounted.3, &mounted.2
                                );
                                Box::new(ok(Response::new(
                                    NodeStageVolumeResponse {},
                                )))
                            }
                        }),
                )
            } else {
                Either::B(Box::new(ok(Response::new(
                    NodeStageVolumeResponse {},
                ))))
            }
        });

    Box::new(f)
}

pub fn create_blkdev(
    socket: String,
    msg: &CreateBlkdevRequest,
) -> Box<dyn Future<Item = Response<CreateBlkdevReply>, Error = Status> + Send>
{
    trace!("{:?}", msg);

    debug!("Creating NBD device for {} ...", msg.uuid);

    let nbd_dev_info = NbdDevInfo::new();
    let uuid = msg.uuid.clone();

    // what ever instance we got assigned, it was in use, and is now removed
    // from the device list
    if nbd_dev_info.is_none() {
        return Box::new(err(Status::new(
            Code::Internal,
            String::from("EAGAIN"),
        )));
    }

    let nbd_dev_info = nbd_dev_info.unwrap();

    let f = get_nbd_instance(&socket, &uuid)
        // TODO: Avoid this step in future chain by returning eexist from
        // start-nbd-disk json-rpc method.
        .and_then(enclose! { (uuid) move |bdev| {
            if let Some(bdev)  = bdev {
                return err(Status::new(
                    Code::AlreadyExists,
                    format!(
                        "Bbdev {} already published at {}",
                        uuid,
                        bdev.nbd_device
                    ),
                ));
            }
            ok(())
        }})
        .map_err(|e| jsonrpc::error::Error::GenericError(e.to_string()))
        .and_then(enclose! { (uuid) move |_| {
            jsonrpc::call::<jsondata::StartNbdDiskArgs, String>(
                &socket,
                "start_nbd_disk",
                Some(jsondata::StartNbdDiskArgs {
                    bdev_name: uuid,
                    nbd_device: format!("{}", nbd_dev_info),
                }),
            )
        }})
        .and_then(move |nbd_device| {
            trace!("NBD device {} created", &nbd_device);
            device::await_size(&nbd_device).map_err(jsonrpc::error::Error::from)
        })
        .and_then(move |size| {
            info!("Device {} reported size: {}", nbd_dev_info, size);
            let reply = CreateBlkdevReply {
                blk_dev: format!("{}", nbd_dev_info),
            };

            ok(Response::new(reply))
        })
        .map_err(move |err| {
            error!(
                "Putting back nbd device {} due to error: {}",
                nbd_dev_info,
                err.to_string()
            );
            nbd_dev_info.put_back();
            err.into_status()
        });

    Box::new(f)
}

pub fn destroy_blkdev(
    socket: String,
    msg: &DestroyBlkdevRequest,
) -> Box<dyn Future<Item = Response<Null>, Error = Status> + Send> {
    trace!("{:?}", msg);

    let uuid = msg.uuid.clone();

    debug!("Deleting NBD device for {} ...", uuid);

    let f = get_nbd_instance(&socket, &uuid)
        // TODO: Avoid this step by returning enoent from stop-nbd-disk
        // json-rpc method.
        .and_then(move |nbd_disk| {
            if nbd_disk.is_none() {
                trace!("bdev {} not found", uuid);
                return err(Status::new(
                    Code::Internal,
                    format!("no such bdev {}", uuid),
                ));
            }

            let nbd_disk = nbd_disk.unwrap();
            ok(nbd_disk)
        })
        .and_then(move |nbd_disk| {
            trace!("Stopping NBD device {}", nbd_disk.nbd_device);
            jsonrpc::call::<jsondata::StopNbdDiskArgs, bool>(
                &socket,
                "stop_nbd_disk",
                Some(jsondata::StopNbdDiskArgs {
                    nbd_device: nbd_disk.nbd_device.clone(),
                }),
            )
            .map_err(|err| err.into_status())
            .and_then(|done| {
                if done {
                    info!(
                        "Stopped NBD device {} with bdev {}",
                        nbd_disk.nbd_device, nbd_disk.bdev_name
                    );

                    NbdDevInfo::from(nbd_disk.nbd_device).put_back();
                    Box::new(ok(Response::new(Null {})))
                } else {
                    let msg = format!(
                        "Failed to stop nbd device {} for {}",
                        nbd_disk.nbd_device, nbd_disk.bdev_name
                    );
                    error!("{}", msg);
                    Box::new(err(Status::new(Code::Internal, msg)))
                }
            })
        });

    Box::new(f)
}

pub fn get_nbd_instance(
    sock: &str,
    bdev_name: &str,
) -> Box<dyn Future<Item = Option<jsondata::NbdDisk>, Error = Status> + Send> {
    let bdev_name = bdev_name.to_string();
    let socket = sock.to_string();

    let f = jsonrpc::call::<jsondata::GetBdevsArgs, Vec<jsondata::Bdev>>(
        &socket,
        "get_bdevs",
        Some(jsondata::GetBdevsArgs {
            name: bdev_name.clone(),
        }),
    )
    .map_err(|e| {
        Status::new(Code::NotFound, format!("Failed to list bdevs: {}", e))
    })
    .and_then(move |bdev| {
        jsonrpc::call::<(), Vec<jsondata::NbdDisk>>(
            &socket,
            "get_nbd_disks",
            None,
        )
        .map(move |nbd_disks| {
            nbd_disks
                .into_iter()
                .find(|ent| ent.bdev_name == bdev[0].name)
        })
        .map_err(|err| {
            Status::new(
                Code::NotFound,
                format!("Failed to find nbd disk: {}", err),
            )
        })
    });

    Box::new(f)
}

impl NbdDevInfo {
    /// This will return the next available nbd device
    pub fn new() -> Option<Self> {
        let instance = ARRAY.lock().unwrap().pop()?;
        trace!("Will use nbd slot {}", instance);
        NbdDevInfo::create(instance)
    }

    fn create(instance: u32) -> Option<Self> {
        let mut path =
            PathBuf::from(&format!("/sys/class/block/nbd{}", instance));

        path.push("pid");
        if path.exists() {
            trace!(
                "Dropping nbd instance: {} as it appears to be in use",
                instance
            );
            return None;
        }

        path.pop();

        let e = path
            .strip_prefix("/sys/class/block")
            .unwrap()
            .to_str()
            .unwrap()
            .split_at(3);

        let instance = e.1.parse().unwrap();

        let dev_t: String = sysfs::parse_value(&path, "dev").unwrap();
        let nums: Vec<u64> =
            dev_t.split(':').map(|x| x.parse().unwrap()).collect();

        // Documentation/admin-guide/devices.txt
        if nums[0] != 43 {
            warn!("Invalid major number of nbd dev {}", path.display());
        }

        let nbd = NbdDevInfo {
            instance,
            major: nums[0],
            minor: nums[1],
        };

        assert_eq!(nbd.instance, instance);
        Some(nbd)
    }

    pub fn put_back(&self) {
        ARRAY.lock().unwrap().push(self.instance);
        trace!("instance {} added back to the free list", self.instance);
    }

    pub fn num_devices() -> usize {
        glob("/sys/class/block/nbd*").unwrap().count()
    }
}

impl From<String> for NbdDevInfo {
    fn from(e: String) -> Self {
        let instance: u32 = e.replace("/dev/nbd", "").parse().unwrap();
        NbdDevInfo::create(instance).unwrap()
    }
}
