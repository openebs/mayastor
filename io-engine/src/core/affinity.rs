use std::path::Path;

use inotify::{Inotify, WatchMask};

use crate::core::{runtime, Mthread, Reactors};

const CPUSET_FILE: &str = "/sys/fs/cgroup/cpuset.cpus.effective";

pub fn start_monitor() {
    std::thread::Builder::new()
        .name("cpuset-monitor".to_string())
        .spawn(run)
        .expect("failed to start cpuset monitor");
}

fn run() {
    // Keep monitor off reactor cores.
    Mthread::unaffinitize();

    if !Path::new(CPUSET_FILE).exists() {
        tracing::warn!("{} does not exist, affinity monitor disabled", CPUSET_FILE);
        return;
    }

    let mut inotify = Inotify::init().expect("failed to initialise inotify");

    inotify
        .watches()
        .add(CPUSET_FILE, WatchMask::MODIFY | WatchMask::CLOSE_WRITE)
        .unwrap();

    tracing::info!("watching {} for cpuset changes", CPUSET_FILE);

    let mut buffer = [0u8; 4096];

    loop {
        match inotify.read_events_blocking(&mut buffer) {
            Ok(events) => {
                let mut changed = false;

                for _ in events {
                    changed = true;
                }

                if changed {
                    reconcile();
                }
            }
            Err(error) => {
                tracing::error!("cpuset monitor failed: {}", error);
            }
        }
    }
}

fn reconcile() {
    tracing::warn!("Detected cpuset update, restoring affinity");

    for reactor in Reactors::iter() {
        reactor.reapply_affinity();
    }

    runtime::reapply_tokio_unaffinity();
}
