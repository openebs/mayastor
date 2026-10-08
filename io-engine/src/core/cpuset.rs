use std::{collections::BTreeSet, io, mem};

/// Core list for the reactors, taken from the CPU affinity mask of the process.
///
/// The kubelet CPU manager enforces its allocation through the container
/// cpuset, which the kernel reflects in the process affinity mask. Reading the
/// mask directly avoids parsing cgroup files and behaves the same on cgroup
/// v1 and v2.
///
/// `requested` is the core list passed with `-l`, if any. Only its size is
/// used: a pod that holds exclusive cores has exactly as many of them as it
/// requested, while a pod that is not Guaranteed inherits the whole shared
/// pool, and starting a busy-poll reactor on every one of those cores would
/// starve the node.
pub fn core_list(requested: Option<&str>) -> io::Result<String> {
    select_cores(&read_affinity()?, requested)
}

/// The decision itself, separate from reading the mask so that it can be
/// tested with synthetic masks on any machine.
fn select_cores(set: &libc::cpu_set_t, requested: Option<&str>) -> io::Result<String> {
    let cores = cores_in(set)?;

    if let Some(requested) = requested {
        let expected = count_cores(requested)?;
        if cores.len() != expected {
            return Err(invalid(format!(
                "the cpuset holds {} cores ({}) but {} were requested \
                 with -l; the pod must be QoS class Guaranteed with an integer \
                 CPU request equal to the requested core count, otherwise the \
                 cpuset is the shared pool or has a different size",
                cores.len(),
                summarise(&cores),
                expected,
            )));
        }
    }

    Ok(cores
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(","))
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

fn cores_in(set: &libc::cpu_set_t) -> io::Result<Vec<usize>> {
    // SAFETY: every index probed stays below CPU_SETSIZE.
    let cores: Vec<usize> = (0..libc::CPU_SETSIZE as usize)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, set) })
        .collect();

    if cores.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "the process CPU affinity mask is empty",
        ));
    }
    Ok(cores)
}

/// Number of distinct cores in a DPDK style core list such as `1,4-6,10`.
fn count_cores(list: &str) -> io::Result<usize> {
    let limit = libc::CPU_SETSIZE as usize;
    let mut cores = BTreeSet::new();

    for part in list.split(',') {
        let part = part.trim();
        let (first, last) = match part.split_once('-') {
            Some((a, b)) => (parse_core(a, list)?, parse_core(b, list)?),
            None => {
                let core = parse_core(part, list)?;
                (core, core)
            }
        };
        if first > last || last >= limit {
            return Err(invalid(format!("invalid core range '{part}' in '{list}'")));
        }
        cores.extend(first..=last);
    }
    Ok(cores.len())
}

fn parse_core(text: &str, list: &str) -> io::Result<usize> {
    text.trim()
        .parse()
        .map_err(|_| invalid(format!("invalid core id '{text}' in core list '{list}'")))
}

/// At most eight cores, so that an oversized shared pool stays readable.
fn summarise(cores: &[usize]) -> String {
    let shown = cores
        .iter()
        .take(8)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    if cores.len() > 8 {
        format!("{shown},...")
    } else {
        shown
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
    fn counts_plain_lists_and_ranges() {
        assert_eq!(count_cores("2,3").unwrap(), 2);
        assert_eq!(count_cores("2-3").unwrap(), 2);
        assert_eq!(count_cores("1,4-6,10").unwrap(), 5);
        assert_eq!(count_cores(" 2 , 3 ").unwrap(), 2);
        assert_eq!(count_cores("7").unwrap(), 1);
    }

    #[test]
    fn counts_overlapping_entries_once() {
        assert_eq!(count_cores("2,2").unwrap(), 1);
        assert_eq!(count_cores("1-3,2-4").unwrap(), 4);
    }

    #[test]
    fn rejects_malformed_core_lists() {
        for bad in ["", "a", "2,,3", "3-2", "2-", "-3", "0-2000", "1;2"] {
            assert!(count_cores(bad).is_err(), "{:?} should be rejected", bad);
        }
    }

    #[test]
    fn accepts_exclusive_cores_matching_the_request() {
        assert_eq!(select_cores(&mask(&[2, 3]), Some("2,3")).unwrap(), "2,3");
        // The chart's generated list names other cores; only the size counts.
        assert_eq!(select_cores(&mask(&[2, 3]), Some("1,2")).unwrap(), "2,3");
    }

    #[test]
    fn rejects_a_shared_pool_larger_than_the_request() {
        let pool: Vec<usize> = (4..32).collect();
        let err = select_cores(&mask(&pool), Some("1,2")).unwrap_err();
        let message = err.to_string();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(message.contains("28 cores"), "{}", message);
        assert!(message.contains("2 were requested"), "{}", message);
        assert!(message.contains("Guaranteed"), "{}", message);
    }

    #[test]
    fn rejects_fewer_cores_than_requested() {
        let err = select_cores(&mask(&[2, 3]), Some("2,3,4")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn does_not_compare_without_a_request() {
        let pool: Vec<usize> = (4..32).collect();
        let expected = pool
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(select_cores(&mask(&pool), None).unwrap(), expected);
    }

    #[test]
    fn rejects_an_empty_mask() {
        let err = select_cores(&mask(&[]), None).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn summarises_long_core_lists() {
        let pool: Vec<usize> = (4..32).collect();
        assert_eq!(summarise(&pool), "4,5,6,7,8,9,10,11,...");
        assert_eq!(summarise(&[2, 3]), "2,3");
    }

    #[test]
    fn reads_the_live_affinity_of_the_process() {
        let list = core_list(None).expect("affinity must be readable");
        for core in list.split(',') {
            core.parse::<usize>()
                .expect("every entry must be a core id");
        }
    }
}
