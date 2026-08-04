//! Reads the CPU set assigned to this process' cgroup by the container

use std::{fs, path::PathBuf};

/// Returns the effective cgroup CPU set as a DPDK `-l`-compatible core-list
/// string (e.g. `"0-2,7"`), or `None` if it can't be determined.
pub fn cpuset_from_cgroup() -> Option<String> {
    let cgroups = read_self_cgroup(&fs::read_to_string("/proc/self/cgroup").ok()?);

    candidate_cpuset_files(&cgroups).into_iter().find_map(|path| {
        let cpus = fs::read_to_string(&path).ok()?;
        let cpus = cpus.trim();
        if cpus.is_empty() {
            return None;
        }
        debug!("Read cgroup cpuset '{cpus}' from {}", path.display());
        Some(cpus.to_string())
    })
}

/// Parses `/proc/self/cgroup` into `(controllers, path)` pairs.
fn read_self_cgroup(contents: &str) -> Vec<(String, String)> {
    contents
        .lines()
        .filter_map(|line| {
            let mut parts = line.splitn(3, ':');
            let _hierarchy_id = parts.next()?;
            let controllers = parts.next()?.to_string();
            let path = parts.next()?.to_string();
            Some((controllers, path))
        })
        .collect()
}

/// Candidate paths to the cpuset "effective" CPUs file, v2 then v1.
fn candidate_cpuset_files(cgroups: &[(String, String)]) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some((_, path)) = cgroups.iter().find(|(controllers, _)| controllers.is_empty()) {
        candidates.push(PathBuf::from(format!(
            "/sys/fs/cgroup{path}/cpuset.cpus.effective"
        )));
    }

    if let Some((_, path)) = cgroups
        .iter()
        .find(|(controllers, _)| controllers.split(',').any(|c| c == "cpuset"))
    {
        candidates.push(PathBuf::from(format!(
            "/sys/fs/cgroup/cpuset{path}/cpuset.effective_cpus"
        )));
    }

    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroup_v2_unified_line() {
        let cgroups = read_self_cgroup("0::/kubepods.slice/kubepod-pod123.slice\n");
        assert_eq!(
            cgroups,
            vec![("".to_string(), "/kubepods.slice/kubepod-pod123.slice".to_string())]
        );
    }

    #[test]
    fn parses_cgroup_v1_multi_line() {
        let cgroups = read_self_cgroup(
            "12:cpuset:/kubepods/podabc/container\n11:memory:/kubepods/podabc/container\n",
        );
        assert_eq!(
            cgroups,
            vec![
                ("cpuset".to_string(), "/kubepods/podabc/container".to_string()),
                ("memory".to_string(), "/kubepods/podabc/container".to_string()),
            ]
        );
    }

    #[test]
    fn candidate_files_prefer_v2_then_v1() {
        let cgroups = vec![
            ("".to_string(), "/v2path".to_string()),
            ("cpuset,cpu".to_string(), "/v1path".to_string()),
        ];
        let candidates = candidate_cpuset_files(&cgroups);
        assert_eq!(
            candidates,
            vec![
                PathBuf::from("/sys/fs/cgroup/v2path/cpuset.cpus.effective"),
                PathBuf::from("/sys/fs/cgroup/cpuset/v1path/cpuset.effective_cpus"),
            ]
        );
    }

    #[test]
    fn no_cpuset_controller_found() {
        let cgroups = vec![("memory".to_string(), "/path".to_string())];
        assert!(candidate_cpuset_files(&cgroups).is_empty());
    }
}
