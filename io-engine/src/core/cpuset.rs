use std::{io, mem};

/// What the pod spec says about the CPUs of this container, in millicores.
///
/// The chart passes both values through the Kubernetes downward API
/// (`resourceFieldRef` with divisor `1m`), so they come from the live pod and
/// include whatever a LimitRange or an admission webhook changed.
#[derive(Debug, Clone, Copy, Default)]
pub struct CpuSpec {
    pub request_millis: Option<u64>,
    pub limit_millis: Option<u64>,
}

/// Core list for the reactors, taken from the CPU affinity mask of the process.
///
/// The kubelet CPU manager enforces its allocation through the container
/// cpuset, which the kernel reflects in the process affinity mask. Reading the
/// mask directly avoids parsing cgroup files and behaves the same on cgroup
/// v1 and v2.
///
/// Every core in the mask is used. A pod that is not QoS class Guaranteed
/// inherits the shared pool, and a busy-poll reactor on every core of it would
/// starve the node. The QoS class cannot be read from inside the container,
/// but its effect can: an exclusive-core pod holds exactly as many cores as it
/// requested. With `spec` set, the mask is accepted only if the request equals
/// the limit, is a whole number of cores, and matches the size of the mask.
/// With `None` nothing is checked.
pub fn core_list(spec: Option<CpuSpec>) -> io::Result<String> {
    let set = read_affinity()?;
    let list = cores_to_list(&set)?;
    if let Some(spec) = spec {
        check_exclusive(list.split(',').count(), spec)?;
    }
    Ok(list)
}

/// Checks that the cpuset has the size the pod asked for. This does not prove
/// the pod is Guaranteed, since only this container is visible, but a shared
/// pool or a Burstable pod fails it.
fn check_exclusive(cores: usize, spec: CpuSpec) -> io::Result<()> {
    let (request, limit) = match (spec.request_millis, spec.limit_millis) {
        (Some(request), Some(limit)) => (request, limit),
        _ => {
            return Err(invalid(
                "the CPU request and limit of the container are not known; \
                 pass them with --cpu-request-millis and --cpu-limit-millis \
                 (the chart does it through the downward API), or allow a \
                 shared cpuset with --allow-shared-cpuset"
                    .to_string(),
            ))
        }
    };
    if request != limit {
        return Err(invalid(format!(
            "the CPU request ({request}m) differs from the limit ({limit}m); \
             the pod must be QoS class Guaranteed to hold exclusive cores"
        )));
    }
    if request == 0 || request % 1000 != 0 {
        return Err(invalid(format!(
            "the CPU request ({request}m) is not a whole number of cores; \
             the CPU manager hands out exclusive cores only for integer requests"
        )));
    }
    let expected = (request / 1000) as usize;
    if cores != expected {
        return Err(invalid(format!(
            "the container cpuset holds {cores} cores but {expected} were \
             requested; the pod is probably not QoS class Guaranteed and \
             inherited the shared pool"
        )));
    }
    Ok(())
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

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
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
        let list = core_list(None).expect("affinity must be readable");
        for core in list.split(',') {
            core.parse::<usize>()
                .expect("every entry must be a core id");
        }
    }

    /// The affinity is per thread, so a thread pinned to one core, as the
    /// CPU manager does for a container, must see exactly that core.
    #[test]
    fn follows_a_thread_pinned_to_one_core() {
        let allowed: Vec<usize> = core_list(None)
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
            core_list(None).unwrap()
        })
        .join()
        .unwrap();

        assert_eq!(seen, target.to_string());
    }

    fn spec(request: Option<u64>, limit: Option<u64>) -> CpuSpec {
        CpuSpec {
            request_millis: request,
            limit_millis: limit,
        }
    }

    #[test]
    fn accepts_a_guaranteed_integer_request_matching_the_mask() {
        assert!(check_exclusive(2, spec(Some(2000), Some(2000))).is_ok());
        assert!(check_exclusive(1, spec(Some(1000), Some(1000))).is_ok());
    }

    #[test]
    fn rejects_unknown_request_or_limit() {
        for case in [
            spec(None, None),
            spec(Some(2000), None),
            spec(None, Some(2000)),
        ] {
            let err = check_exclusive(2, case).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert!(err.to_string().contains("not known"), "{}", err);
        }
    }

    #[test]
    fn rejects_a_request_that_differs_from_the_limit() {
        // A Burstable pod without a limit reports the node's allocatable CPU
        // as its limit through the downward API.
        let err = check_exclusive(28, spec(Some(1000), Some(28000))).unwrap_err();
        assert!(err.to_string().contains("Guaranteed"), "{}", err);
    }

    #[test]
    fn rejects_fractional_and_zero_requests() {
        for millis in [0, 500, 1500, 2100] {
            let err = check_exclusive(2, spec(Some(millis), Some(millis))).unwrap_err();
            assert!(err.to_string().contains("whole number"), "{}", err);
        }
    }

    #[test]
    fn rejects_a_mask_of_another_size_than_requested() {
        let err = check_exclusive(28, spec(Some(2000), Some(2000))).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("28 cores"), "{}", message);
        assert!(message.contains("2 were requested"), "{}", message);
        assert!(check_exclusive(1, spec(Some(2000), Some(2000))).is_err());
    }

    #[test]
    fn checks_the_live_mask_against_the_spec_only_when_given() {
        let cores = core_list(None).unwrap().split(',').count();
        let millis = cores as u64 * 1000;
        assert!(core_list(Some(spec(Some(millis), Some(millis)))).is_ok());
        assert!(core_list(Some(spec(Some(millis + 1000), Some(millis + 1000)))).is_err());
    }
}
