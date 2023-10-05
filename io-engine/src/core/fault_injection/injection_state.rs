use rand::{rngs::StdRng, SeedableRng};
use std::time::{Duration, Instant};

/// Injection random number generator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InjectionState {
    /// Instance at which the injection started to be active.
    pub(super) started: Option<Instant>,
    /// Number of injection hits.
    pub(super) hits: u64,
    /// Random number generator for data corruption injection.
    pub(super) rng: StdRng,
}

impl Default for InjectionState {
    fn default() -> Self {
        let seed = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        Self {
            started: None,
            hits: 0,
            rng: StdRng::from_seed(seed),
        }
    }
}

impl InjectionState {
    /// TODO
    #[inline(always)]
    pub(super) fn tick(&mut self) -> bool {
        if self.started.is_none() {
            self.started = Some(Instant::now());
            self.hits = 1;
            true
        } else {
            self.hits += 1;
            false
        }
    }

    /// Returns the current time relative to the injection start.
    #[inline(always)]
    pub(super) fn now(&self) -> Duration {
        self.started.map_or(Duration::MAX, |s| {
            Instant::now().saturating_duration_since(s)
        })
    }
}
