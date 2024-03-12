#[macro_export]
macro_rules! gen_rebuild_instances {
    ($T:ty) => {
        /// List of rebuild jobs indexed by the destination's replica uri.
        type RebuildJobInstances =
            std::collections::HashMap<String, std::sync::Arc<$T>>;

        impl $T {
            /// Get the rebuild job instances container, we ensure that this can
            /// only ever be called on a properly allocated thread
            fn get_instances<'a>() -> parking_lot::MutexGuard<'a, RebuildJobInstances> {
                assert!(
                    spdk_rs::Thread::is_spdk_thread(),
                    "not called from SPDK thread"
                );

                static REBUILD_INSTANCES: once_cell::sync::OnceCell<
                    parking_lot::Mutex<RebuildJobInstances>,
                > = once_cell::sync::OnceCell::new();

                REBUILD_INSTANCES
                    .get_or_init(|| parking_lot::Mutex::new(std::collections::HashMap::new()))
                    .lock()
            }

            /// Returns number of all rebuild jobs of type $T on the system.
            pub fn count() -> usize {
                Self::get_instances().len()
            }

            /// Lookup a rebuild job by its name then remove and drop it.
            pub fn remove(
                name: &str,
            ) -> Result<std::sync::Arc<Self>, super::RebuildError> {
                match Self::get_instances().remove(name) {
                    Some(job) => Ok(job),
                    None => Err(RebuildError::JobNotFound {
                        job: name.to_owned(),
                    }),
                }
            }

            /// Stores a rebuild job in the rebuild job list.
            pub fn store(self) -> Result<std::sync::Arc<Self>, super::RebuildError> {
                let mut rebuild_list = Self::get_instances();

                if rebuild_list.contains_key(self.name()) {
                    Err(RebuildError::JobAlreadyExists {
                        job: self.dst_uri().to_string(),
                    })
                } else {
                    let job = std::sync::Arc::new(self);
                    let _ = rebuild_list.insert(
                        job.name().to_owned(),
                        job.clone(),
                    );
                    Ok(job)
                }
            }

            /// Lookup a rebuild job by its name and return it.
            pub fn lookup(
                name: &str,
            ) -> Result<std::sync::Arc<Self>, super::RebuildError> {
                if let Some(job) = Self::get_instances().get(name) {
                    Ok(job.clone())
                } else {
                    Err(RebuildError::JobNotFound {
                        job: name.to_owned(),
                    })
                }
            }

            /// Lookup all rebuilds jobs with `src_uri` as its source uri.
            pub fn lookup_src(src_uri: &str) -> Vec<std::sync::Arc<Self>> {
                Self::get_instances()
                    .iter_mut()
                    .filter_map(|j| {
                        if j.1.src_uri() == src_uri {
                            Some(j.1.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            }

            /// Lookup a rebuild job by its target uri and return it.
            pub fn lookup_dst_uri(
                dst_uri: &str,
            ) -> Result<std::sync::Arc<Self>, super::RebuildError> {
                let not_found = || RebuildError::JobNotFound {
                    job: dst_uri.to_owned(),
                };
                let url = url::Url::parse(dst_uri).map_err(|_| not_found())?;
                let name = url.path().strip_prefix('/').unwrap_or(url.path());
                let job = Self::lookup(name)?;
                if job.dst_uri != dst_uri {
                    return Err(not_found());
                }
                Ok(job)
            }
        }
    };
}
