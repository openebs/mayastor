use std::{
    path::Path,
    time::{Duration, Instant},
};

use inotify::{EventMask, Inotify, WatchMask};

use crate::core::{runtime, Mthread, Reactors};

const CPUSET_FILE: &str = "/var/lib/kubelet/cpu_manager_state";

/// Starts the CPUManager cpuset monitoring thread.
///
/// The monitor watches the kubelet `cpu_manager_state` file for
/// cpuset updates and restores reactor and runtime thread affinity
/// when Kubernetes CPUManager changes the effective cpuset.
pub fn start_monitor() {
    if std::env::var("KUBERNETES_SERVICE_HOST").is_err() {
        return;
    }
    runtime::spawn_blocking(run);
}

fn run() {
    let mut inotify = match Inotify::init() {
        Ok(inotify) => inotify,
        Err(error) => {
            tracing::error!(
                ?error,
                "failed to initialize CPUManager affinity monitor; affinity restoration disabled"
            );
            return;
        }
    };

    loop {
        if !Path::new(CPUSET_FILE).exists() {
            tracing::warn!("{CPUSET_FILE} does not exist, waiting...");
            std::thread::sleep(std::time::Duration::from_secs(1));
            continue;
        }

        if let Err(error) = inotify.watches().add(
            CPUSET_FILE,
            WatchMask::ATTRIB | WatchMask::MODIFY | WatchMask::CLOSE_WRITE | WatchMask::DELETE_SELF,
        ) {
            tracing::warn!(?error, "failed to watch {CPUSET_FILE}, retrying");
            std::thread::sleep(std::time::Duration::from_secs(1));
            continue;
        }

        tracing::trace!("watching {CPUSET_FILE}");

        let mut buffer = [0u8; 4096];
        let mut recreate_watch = false;

        while !recreate_watch {
            match inotify.read_events_blocking(&mut buffer) {
                Ok(events) => {
                    let mut changed = false;

                    for event in events {
                        tracing::trace!("event {:?}", event.mask);

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
                    tracing::warn!(?error, "cpu_manager_state monitor failed, recreating watch");
                    recreate_watch = true;
                }
            }
        }

        tracing::debug!(
            "inotify watch lost (DELETE_SELF/IGNORED), recreating watch on new cpuset file"
        );

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Upper bound on how long [`reconcile`] waits for kubelet to apply the cgroup
/// cpuset after a `cpu_manager_state` event.
///
/// kubelet writes `cpu_manager_state` (which triggers this reconcile) as soon
/// as a Guaranteed pod is admitted/removed, but it applies the container's
/// cgroup cpuset asynchronously, on its CPUManager reconcile period (default
/// 5s). Measured apply latencies on the target cluster were ~5s (grow) and
/// ~7-9s (shrink), so we must keep watching well past that; otherwise we miss
/// the apply and leave the baseline stale, which then mis-triggers the next
/// event. This is only a ceiling -- reconcile breaks the moment the change is
/// observed, so it normally returns in ~5-9s and only approaches this bound if
/// the apply never arrives (in which case we fall back to the current
/// baseline).
const RECONCILE_SETTLE_WINDOW: Duration = Duration::from_secs(20);
/// Interval between cpuset re-checks within [`RECONCILE_SETTLE_WINDOW`].
const RECONCILE_SETTLE_INTERVAL: Duration = Duration::from_millis(500);

fn reconcile() {
    tracing::debug!("Detected cpuset update, restoring affinity");

    // Wait for kubelet to actually apply the cgroup cpuset before restoring
    // affinity. kubelet writes cpu_manager_state (our trigger) up to its
    // CPUManager reconcile period (~5s) *before* it applies the container cgroup
    // cpuset, so poll refresh until it reports the cpuset changed -- then stop
    // early. If nothing changes within the settle window (a same-value
    // re-assert or a spurious event), stop anyway. Either way, fall through.
    let start = Instant::now();
    loop {
        if Mthread::refresh_base_cpuset() {
            break;
        }
        if start.elapsed() >= RECONCILE_SETTLE_WINDOW {
            break;
        }
        std::thread::sleep(RECONCILE_SETTLE_INTERVAL);
    }

    // Applying the cgroup cpuset resets our threads' affinities (the kernel
    // re-pins every task in the cgroup to the new mask), so restore them once,
    // per event, using the current baseline -- the new cpuset if it changed,
    // the old one otherwise.
    for reactor in Reactors::iter() {
        reactor.reapply_affinity();
    }
    runtime::reapply_workers_unaffinity();
}
