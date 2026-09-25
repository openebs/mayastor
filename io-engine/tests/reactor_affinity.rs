use io_engine::core::{mayastor_env_stop, Cores, MayastorCliArgs, MayastorEnvironment};

pub mod common;

/// CPU ids that thread `tid` is allowed to run on.
fn affinity_of(tid: libc::pid_t) -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set),
            0,
            "sched_getaffinity failed for tid {}",
            tid
        );
        (0..libc::CPU_SETSIZE as usize)
            .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
            .collect()
    }
}

/// Thread ids of this process' tokio runtime workers. The kernel truncates
/// `comm` to 15 characters, so "tokio-runtime-worker" arrives as
/// "tokio-runtime-w".
fn tokio_worker_tids() -> Vec<libc::pid_t> {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            let comm = std::fs::read_to_string(path.join("comm")).unwrap_or_default();
            if !comm.trim_end().starts_with("tokio-runtime-w") {
                return None;
            }
            path.file_name()?.to_str()?.parse().ok()
        })
        .collect()
}

/// Tokio's workers must never be allowed to run on a reactor core: a reactor
/// busy-polls, so anything else scheduled onto its core stalls the I/O path.
///
/// Regression test for the ordering bug where the workers are created before
/// the reactors exist -- the `unaffinitize()` in their `on_thread_start` hook
/// then runs with an empty `Cores` list, clears nothing, and leaves them free
/// to run anywhere. The environment must correct this once the reactors are up.
///
/// This test requires the system to have at least 2 cpus.
#[common::spdk_test]
fn reactor_workers_off_reactor_cores() {
    // Force the runtime up *before* the environment starts, reproducing the
    // ordering the io-engine binary has. Without this the runtime is created
    // later, when `Cores` is already populated, and the bug cannot occur.
    io_engine::core::runtime::spawn(async {});

    let args = MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    };

    MayastorEnvironment::new(args)
        .start(|| {
            let reactor_cores: Vec<usize> =
                Cores::count().into_iter().map(|c| c as usize).collect();
            assert!(!reactor_cores.is_empty(), "no reactor cores");

            let workers = tokio_worker_tids();
            assert!(!workers.is_empty(), "no tokio worker threads found");

            for tid in workers {
                let allowed = affinity_of(tid);
                let overlap: Vec<usize> = allowed
                    .iter()
                    .filter(|cpu| reactor_cores.contains(cpu))
                    .copied()
                    .collect();
                assert!(
                    overlap.is_empty(),
                    "tokio worker {} is allowed on reactor core(s) {:?} \
                     (allowed={:?}, reactor cores={:?})",
                    tid,
                    overlap,
                    allowed,
                    reactor_cores
                );
            }

            mayastor_env_stop(0);
        })
        .unwrap();
}
