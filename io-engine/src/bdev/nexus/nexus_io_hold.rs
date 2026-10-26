//! Holds of the nexus I/O pause on behalf of external holders, eg: a group snapshot
//! coordinator which needs the I/O of several nexuses paused at the same time.
//! Each holder owns one pause of the nexus I/O subsystem, which is released either
//! explicitly or automatically when the holder's lease expires.
//! The pause runs in its own task so that a requester can give up on a slow drain without
//! abandoning the in-flight pause: a hold cancelled while pausing is released as soon as the
//! pause completes.

use super::{nexus_lookup_mut, Error, Nexus, NexusOperation, NexusState};
use crate::{
    core::{Protocol, Reactors, Share},
    sleep::mayastor_sleep,
};
use futures::{channel::oneshot, future::Either};
use std::{
    pin::Pin,
    time::{Duration, Instant},
};

/// The longest lease of a hold: the hosts give up on paused I/O well before it expires.
const MAX_IO_HOLD_LEASE: Duration = Duration::from_secs(600);
/// How long to wait before retrying to release a hold whose I/O failed to resume.
const IO_HOLD_RELEASE_RETRY: Duration = Duration::from_secs(1);

/// A hold of the nexus I/O pause.
#[derive(Debug, Clone, Copy)]
pub(super) enum IoHold {
    /// The pause is in progress. A cancelled hold is released as soon as the pause completes.
    Pausing { lease: Duration, cancelled: bool },
    /// The I/O is held paused until the lease expires.
    Held { expires: Instant },
}

impl Nexus<'_> {
    /// Hold the nexus I/O paused on behalf of `holder` until it's released or until its
    /// `lease` expires. Calling this again with the same holder renews its lease; with
    /// `renew_only` it fails if the holder no longer holds the pause rather than pausing again.
    /// If the I/O doesn't drain within `timeout` the hold is cancelled and the request fails.
    /// Note: must be called from the master core.
    pub async fn hold_io_pause(
        self: Pin<&mut Self>,
        holder: &str,
        lease: Duration,
        renew_only: bool,
        timeout: Option<Duration>,
    ) -> Result<(), Error> {
        if holder.is_empty() || lease.is_zero() || lease > MAX_IO_HOLD_LEASE {
            return Err(Error::InvalidArguments {
                name: self.name.clone(),
                args: format!(
                    "a holder and a non-zero lease of at most {MAX_IO_HOLD_LEASE:?} are required"
                ),
            });
        }
        match self.io_holds.lock().get_mut(holder) {
            Some(IoHold::Held { expires }) => {
                *expires = Instant::now() + lease;
                return Ok(());
            }
            Some(IoHold::Pausing { .. }) => {
                return Err(Error::IoHoldInProgress {
                    name: self.name.clone(),
                    holder: holder.to_string(),
                })
            }
            None if renew_only => {
                return Err(Error::IoHoldNotFound {
                    name: self.name.clone(),
                    holder: holder.to_string(),
                })
            }
            None => {}
        }

        self.check_nexus_operation(NexusOperation::NexusIoPause)?;
        if *self.state.lock() != NexusState::Open {
            return Err(Error::OperationNotAllowed {
                reason: "Nexus is not opened".to_string(),
            });
        }
        if !matches!(self.shared(), Some(Protocol::Nvmf)) {
            return Err(Error::NotSharedNvmf {
                name: self.name.clone(),
            });
        }

        self.io_holds.lock().insert(
            holder.to_string(),
            IoHold::Pausing {
                lease,
                cancelled: false,
            },
        );
        let (sender, receiver) = oneshot::channel();
        let (nexus, pause_holder) = (self.name.clone(), holder.to_string());
        Reactors::master().send_future(async move {
            let result = Self::pause_for_holder(&nexus, &pause_holder).await;
            // the requester might have given up on the pause already
            let _ = sender.send(result);
        });

        let result = match timeout {
            None => receiver.await,
            Some(timeout) => match futures::future::select(receiver, mayastor_sleep(timeout)).await
            {
                Either::Left((result, _)) => result,
                Either::Right(_) => {
                    if let Some(IoHold::Pausing { cancelled, .. }) =
                        self.io_holds.lock().get_mut(holder)
                    {
                        *cancelled = true;
                    }
                    return Err(Error::IoHoldTimedOut {
                        name: self.name.clone(),
                        holder: holder.to_string(),
                        timeout,
                    });
                }
            },
        };
        result.unwrap_or_else(|_| {
            Err(Error::IoHoldNotFound {
                name: self.name.clone(),
                holder: holder.to_string(),
            })
        })
    }

    /// Pause the nexus I/O for `holder` whose hold is pausing, then hold it paused or, if the
    /// hold was cancelled meanwhile, release it right away.
    async fn pause_for_holder(nexus: &str, holder: &str) -> Result<(), Error> {
        let not_found = || Error::NexusNotFound {
            name: nexus.to_string(),
        };
        let result = nexus_lookup_mut(nexus).ok_or_else(not_found)?.pause().await;
        // looked up again as it might have been destroyed while pausing
        let nx = nexus_lookup_mut(nexus).ok_or_else(not_found)?;
        let hold = nx.io_holds.lock().get(holder).copied();
        match (result, hold) {
            (
                Ok(()),
                Some(IoHold::Pausing {
                    lease,
                    cancelled: false,
                }),
            ) => {
                nx.io_holds.lock().insert(
                    holder.to_string(),
                    IoHold::Held {
                        expires: Instant::now() + lease,
                    },
                );
                info!("{nx:?}: I/O held paused by '{holder}' for {lease:?}");
                Self::spawn_io_hold_expiry(nexus.to_string(), holder.to_string());
                Ok(())
            }
            (Ok(()), _) => {
                nx.io_holds.lock().remove(holder);
                warn!("{nx:?}: I/O pause of '{holder}' completed after it was cancelled, releasing it");
                Self::resume_released(nx, holder).await?;
                Err(Error::IoHoldNotFound {
                    name: nexus.to_string(),
                    holder: holder.to_string(),
                })
            }
            (Err(error), _) => {
                nx.io_holds.lock().remove(holder);
                Err(error)
            }
        }
    }

    /// Release the I/O pause held by `holder`. Releasing a holder which holds no pause
    /// succeeds; a pause still in progress is released as soon as it completes.
    /// Note: must be called from the master core.
    pub async fn release_io_hold(self: Pin<&mut Self>, holder: &str) -> Result<(), Error> {
        {
            let mut holds = self.io_holds.lock();
            match holds.get_mut(holder) {
                None => return Ok(()),
                Some(IoHold::Pausing { cancelled, .. }) => {
                    *cancelled = true;
                    return Ok(());
                }
                Some(IoHold::Held { .. }) => {
                    holds.remove(holder);
                }
            }
        }
        info!("{self:?}: I/O hold of '{holder}' released");
        Self::resume_released(self, holder).await
    }

    /// Resume the I/O paused by the hold of `holder`, which was just released. Should the I/O
    /// fail to resume, it is still paused: the hold is kept, expired, so that it's released
    /// again, by the holder or by its expiry, rather than leaving the I/O paused for good.
    async fn resume_released(mut nexus: Pin<&mut Self>, holder: &str) -> Result<(), Error> {
        let result = nexus.as_mut().resume().await;
        if let Err(error) = &result {
            error!("{nexus:?}: {error}: failed to resume the I/O released by '{holder}', retrying");
            nexus.io_holds.lock().insert(
                holder.to_string(),
                IoHold::Held {
                    expires: Instant::now() + IO_HOLD_RELEASE_RETRY,
                },
            );
            Self::spawn_io_hold_expiry(nexus.name.clone(), holder.to_string());
        }
        result
    }

    /// Whether any holder holds, or is pausing, the nexus I/O.
    pub(super) fn io_held(&self) -> bool {
        !self.io_holds.lock().is_empty()
    }

    /// Release the hold of `holder` once its lease expires, unless released before.
    /// The nexus is looked up again after every sleep as it might be destroyed meanwhile.
    fn spawn_io_hold_expiry(nexus: String, holder: String) {
        Reactors::master().send_future(async move {
            loop {
                let remaining = {
                    let Some(nexus) = nexus_lookup_mut(&nexus) else {
                        return;
                    };
                    let Some(IoHold::Held { expires }) =
                        nexus.io_holds.lock().get(&holder).copied()
                    else {
                        return;
                    };
                    let remaining = expires.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        warn!("{nexus:?}: I/O hold lease of '{holder}' expired, releasing it");
                        if let Err(error) = nexus.release_io_hold(&holder).await {
                            error!("{error}: failed to release the expired I/O hold of '{holder}'");
                        }
                        return;
                    }
                    remaining
                };
                _ = mayastor_sleep(remaining).await;
            }
        });
    }
}
