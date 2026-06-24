use std::path::Path;

use inotify::{EventMask, Inotify, WatchMask};

use crate::core::{runtime, Mthread, Reactors};

const CPUSET_FILE: &str = "/var/lib/kubelet/cpu_manager_state";

pub fn start_monitor() {
    runtime::spawn_blocking(run);
}

fn run() {
    Mthread::unaffinitize();

    loop {
        if !Path::new(CPUSET_FILE).exists() {
            tracing::warn!("{} does not exist, waiting...", CPUSET_FILE);
            std::thread::sleep(std::time::Duration::from_secs(1));
            continue;
        }

        let mut inotify = Inotify::init().expect("failed to initialise inotify");

        inotify
            .watches()
            .add(
                CPUSET_FILE,
                WatchMask::ATTRIB
                    | WatchMask::MODIFY
                    | WatchMask::CLOSE_WRITE
                    | WatchMask::DELETE_SELF,
            )
            .unwrap();

        tracing::info!("watching {}", CPUSET_FILE);

        let mut buffer = [0u8; 4096];

        let mut recreate_watch = false;

        while !recreate_watch {
            match inotify.read_events_blocking(&mut buffer) {
                Ok(events) => {
                    let mut changed = false;

                    for event in events {
                        tracing::debug!("event {:?}", event.mask);

                        if event.mask.contains(EventMask::ATTRIB)
                            || event.mask.contains(EventMask::MODIFY)
                            || event.mask.contains(EventMask::CLOSE_WRITE)
                        {
                            changed = true;
                        }

                        if event.mask.contains(EventMask::DELETE_SELF)
                            || event.mask.contains(EventMask::IGNORED)
                        {
                            recreate_watch = true;
                        }
                    }

                    if changed {
                        reconcile();
                    }
                }
                Err(error) => {
                    tracing::error!("cpu_manager_state monitor failed: {}", error);
                    recreate_watch = true;
                }
            }
        }

        tracing::warn!("watch lost, recreating on new inode");

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn reconcile() {
    tracing::warn!("Detected cpuset update, restoring affinity");

    for reactor in Reactors::iter() {
        reactor.reapply_affinity();
    }

    runtime::reapply_workers_unaffinity();
}
