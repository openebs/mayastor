use std::{io, mem};

/// Core list for the reactors, taken from the CPU affinity mask of the process.
///
/// The kubelet CPU manager enforces its allocation through the container
/// cpuset, which the kernel reflects in the process affinity mask. Reading the
/// mask directly avoids parsing cgroup files and behaves the same on cgroup
/// v1 and v2.
///
/// Every core in the mask is used. The caller is responsible for making sure
/// the pod holds exclusive cores (QoS class Guaranteed with an integer CPU
/// request): a pod that is not Guaranteed inherits the shared pool, and a
/// busy-poll reactor on every core of it would starve the node.
pub fn core_list() -> io::Result<String> {
    cores_to_list(&read_affinity()?)
}

/// The conversion itself, separate from reading the mask so that it can be
/// tested with synthetic masks on any machine.
fn cores_to_list(set: &libc::cpu_set_t) -> io::Result<String> {
    // SAFETY: every index probed stays below CPU_SETSIZE.
    let cores: Vec<String> = (0..libc::CPU_SETSIZE as usize)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, set) })
        .map(|cpu| cpu.to_string())
        .collect();

    if cores.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "the process CPU affinity mask is empty",
        ));
    }
    Ok(cores.join(","))
}

fn read_affinity() -> io::Result<libc::cpu_set_t> {
    // SAFETY: `set` is a valid, zero-initialised cpu_set_t of the size passed.
    unsafe {
        let mut set: libc::cpu_set_t = mem::zeroed();
        if libc::sched_getaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mask(cores: &[usize]) -> libc::cpu_set_t {
        let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
        for core in cores {
            unsafe { libc::CPU_SET(*core, &mut set) };
        }
        set
    }

    #[test]
    fn lists_every_core_of_the_mask_in_order() {
        assert_eq!(cores_to_list(&mask(&[3, 2])).unwrap(), "2,3");
        assert_eq!(cores_to_list(&mask(&[7])).unwrap(), "7");
        assert_eq!(
            cores_to_list(&mask(&[1, 4, 5, 6, 10])).unwrap(),
            "1,4,5,6,10"
        );
    }

    #[test]
    fn uses_the_whole_shared_pool_without_judging_it() {
        // Whether the pod is Guaranteed is checked by the deployment, not here.
        let pool: Vec<usize> = (4..32).collect();
        let list = cores_to_list(&mask(&pool)).unwrap();
        assert_eq!(list.split(',').count(), 28);
        assert!(list.starts_with("4,5,6,"));
    }

    #[test]
    fn rejects_an_empty_mask() {
        let err = cores_to_list(&mask(&[])).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn reads_the_live_affinity_of_the_process() {
        let list = core_list().expect("affinity must be readable");
        for core in list.split(',') {
            core.parse::<usize>()
                .expect("every entry must be a core id");
        }
    }

    /// The affinity is per thread, so a thread pinned to one core, as the
    /// CPU manager does for a container, must see exactly that core.
    #[test]
    fn follows_a_thread_pinned_to_one_core() {
        let allowed: Vec<usize> = core_list()
            .unwrap()
            .split(',')
            .map(|c| c.parse().unwrap())
            .collect();
        let target = *allowed.last().unwrap();

        let seen = std::thread::spawn(move || {
            let pinned = mask(&[target]);
            // SAFETY: `pinned` is a valid cpu_set_t of the size passed.
            let rc =
                unsafe { libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &pinned) };
            assert_eq!(rc, 0, "{}", io::Error::last_os_error());
            core_list().unwrap()
        })
        .join()
        .unwrap();

        assert_eq!(seen, target.to_string());
    }
}
