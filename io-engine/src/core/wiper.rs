use crate::core::{CoreError, UntypedBdevHandle};
use snafu::Snafu;
use std::{fmt::Debug, ops::Deref};

/// The Error for the Wiper.
#[derive(Clone, Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum Error {
    #[snafu(display("Too many notifications, try increasing the chunk_size"))]
    TooManyChunks {},
    #[snafu(display("The chunk_size is larger than the bdev"))]
    ChunkTooLarge {},
    #[snafu(display(
        "The chunk_size is not a multiple of the bdev block size"
    ))]
    ChunkBlockSizeInvalid {},
    #[snafu(display("The bdev seems to have no size!"))]
    ZeroBdev {},
    #[snafu(display("The wipe has been aborted"))]
    WipeAborted {},
    #[snafu(display("Error while wiping the bdev (IO Error)"))]
    WipeIoFailed { source: Box<CoreError> },
    #[snafu(display("Wipe Method {method:?} not implemented"))]
    MethodUnimplemented { method: WipeMethod },
    #[snafu(display("Failed to post client notification: {error}"))]
    ChunkNotifyFailed { error: String },
}

impl From<Error> for CoreError {
    fn from(source: Error) -> Self {
        Self::WipeFailed {
            source,
        }
    }
}
impl From<CoreError> for Error {
    fn from(source: CoreError) -> Self {
        Self::WipeIoFailed {
            source: Box::new(source),
        }
    }
}

/// A wiper which can wipe a bdev using a specified wipe method.
pub(crate) struct Wiper {
    /// The bdev which is to be wiped.
    bdev: UntypedBdevHandle,
    /// The method used to wipe the bdev.
    wipe_method: WipeMethod,
}

/// Options for the streamed wiper.
pub(crate) struct StreamWipeOptions {
    /// Wipe in chunks and notify client when every chunk is complete.
    /// We might be able to add a range here if we wanted to..
    pub(crate) chunk_size: u64,
    /// Method used to wipe the bdev.
    pub(crate) wipe_method: WipeMethod,
}

/// A streamed version of `Wiper` which can notify a client as it progresses and
/// can track if the client has disconnected for faster error handling.
/// todo: abstract `N` and `A` into a Stream like trait.
pub(crate) struct StreamedWiper<S: NotifyStream> {
    wiper: Wiper,
    stats: WipeStats,
    stream: S,
}

/// A notification stream which can be used to notify a client everytime a
/// certain size is wiped.
pub(crate) trait NotifyStream {
    /// Notify the client with the given stats.
    fn notify(&self, stats: &WipeStats) -> Result<(), String>;
    /// Check if the stream is closed.
    fn is_closed(&self) -> bool;
}

/// Wipe method, allowing for some flexibility.
#[derive(Default, Debug, Clone, Copy)]
pub enum WipeMethod {
    /// Don't actually wipe, just pretend.
    #[default]
    None,
    /// Wipe by writing zeroes.
    WriteZeroes,
    /// Wipe by sending unmap/trim.
    Unmap,
    /// When using WRITE_PATTERN, wipe using this 32bit write pattern, example:
    /// 0xDEADBEEF.
    WritePattern(u32),
}

/// Final Wipe stats.
#[derive(Debug)]
pub(crate) struct FinalWipeStats {
    start: std::time::Instant,
    end: std::time::Instant,
    stats: WipeStats,
}
impl FinalWipeStats {
    /// Log the stats.
    pub(crate) fn log(&self) {
        let stats = &self.stats;
        let elapsed = self.end - self.start;
        let elapsed_f = elapsed.as_secs_f64();
        let bandwidth = if elapsed_f.is_normal() {
            let bandwidth = (stats.total_bytes as f64 / elapsed_f) as u128;
            byte_unit::Byte::from_bytes(bandwidth)
                .get_appropriate_unit(true)
                .to_string()
        } else {
            "??".to_string()
        };

        tracing::warn!(
            "Wiped {} => {:.3?} => {bandwidth}/s",
            self.stats.uuid,
            elapsed
        );
    }
}

/// Wipe stats which help track the progress.
#[derive(Default, Debug)]
pub(crate) struct WipeStats {
    /// Uuid of the bdev to be wiped.
    pub(crate) uuid: uuid::Uuid,
    /// The stats iterator for the wipe operation.
    pub(crate) stats: WipeIterator,
    /// Track how long it's been since the first wipe IO.
    pub(crate) since: Option<std::time::Duration>,
}
impl Deref for WipeStats {
    type Target = WipeIterator;

    fn deref(&self) -> &Self::Target {
        &self.stats
    }
}
impl WipeStats {
    /// Complete the current chunk.
    fn complete_chunk(&mut self, start: std::time::Instant, size: u64) {
        self.stats.complete_chunk(size);
        self.since = Some(start.elapsed());
    }
}

impl Wiper {
    /// Return a new `Self` which can wipe the given bdev using the provided
    /// wipe method.
    pub fn new(
        bdev: UntypedBdevHandle,
        wipe_method: WipeMethod,
    ) -> Result<Self, Error> {
        Ok(Self {
            bdev,
            wipe_method: Self::supported(wipe_method)?,
        })
    }
    /// Wipe the bdev at the given byte offset and byte size.
    pub async fn wipe(&self, offset: u64, size: u64) -> Result<(), Error> {
        match self.wipe_method {
            WipeMethod::None => Ok(()),
            WipeMethod::WriteZeroes => {
                self.bdev.write_zeroes_at(offset, size).await.map_err(
                    |source| Error::WipeIoFailed {
                        source: Box::new(source),
                    },
                )
            }
            WipeMethod::Unmap | WipeMethod::WritePattern(_) => {
                Err(Error::MethodUnimplemented {
                    method: self.wipe_method,
                })
            }
        }?;
        Ok(())
    }
    /// Check if the given method is supported.
    pub(crate) fn supported(
        wipe_method: WipeMethod,
    ) -> Result<WipeMethod, Error> {
        match wipe_method {
            WipeMethod::None | WipeMethod::WriteZeroes => Ok(wipe_method),
            WipeMethod::Unmap | WipeMethod::WritePattern(_) => {
                Err(Error::MethodUnimplemented {
                    method: wipe_method,
                })
            }
        }
    }
}

impl<S: NotifyStream> StreamedWiper<S> {
    /// Create a new `Self` which wipes a bdev using the given `Wiper` by wiping
    /// in chunk sizes and notifying with stats after every chunk.
    pub fn new(
        wiper: Wiper,
        chunk_size_bytes: u64,
        max_chunks: usize,
        stream: S,
    ) -> Result<Self, Error> {
        let size = wiper.bdev.get_bdev().size_in_bytes();
        let block_len = wiper.bdev.get_bdev().block_len() as u64;
        snafu::ensure!(chunk_size_bytes <= size, ChunkTooLarge {});
        let iterator = WipeIterator::new(0, size, chunk_size_bytes, block_len)?;

        snafu::ensure!(
            iterator.total_chunks < max_chunks as u64,
            TooManyChunks {}
        );

        let stats = WipeStats {
            uuid: wiper.bdev.get_bdev().uuid(),
            stats: iterator,
            since: None,
        };
        Ok(Self {
            wiper,
            stats,
            stream,
        })
    }

    /// Wipe the bdev while notifying after every chunk_size is complete. This
    /// is to allow the client to notice wipe is in progress and not stuck.
    pub async fn wipe(mut self) -> Result<FinalWipeStats, Error> {
        self.notify()?;
        let start = std::time::Instant::now();
        while let Some((offset, size)) = self.stats.next() {
            self.wipe_chunk(start, offset, size).await?;
        }
        Ok(FinalWipeStats {
            start,
            end: std::time::Instant::now(),
            stats: self.stats,
        })
    }

    /// Wipe a "chunk" using a byte offset and byte length.
    async fn wipe_chunk(
        &mut self,
        start: std::time::Instant,
        offset: u64,
        size: u64,
    ) -> Result<(), Error> {
        self.wipe_with_abort(offset, size).await?;

        self.stats.complete_chunk(start, size);

        self.notify()
    }

    /// Wipe the bdev at the given byte offset and byte size.
    /// Uses the abort checker allowing us to stop early if a client disconnects
    /// or if the process is being shutdown.
    async fn wipe_with_abort(
        &self,
        offset: u64,
        size: u64,
    ) -> Result<(), Error> {
        // todo: configurable?
        let max_io_size = 8 * 1024 * 1024;
        if size > max_io_size {
            let block_len = self.wiper.bdev.get_bdev().block_len() as u64;
            let mut iterator =
                WipeIterator::new(offset, size, max_io_size, block_len)?;
            while let Some((offset, size)) = iterator.next() {
                self.wiper.wipe(offset, size).await?;
                iterator.complete_chunk(size);
                self.check_abort()?;
            }
        } else {
            self.wiper.wipe(offset, size).await?;
        }
        Ok(())
    }

    fn check_abort(&self) -> Result<(), Error> {
        if self.stream.is_closed() {
            return Err(Error::WipeAborted {});
        }
        Ok(())
    }

    /// Notify with the latest stats.
    fn notify(&self) -> Result<(), Error> {
        if let Err(error) = self.stream.notify(&self.stats) {
            self.check_abort()?;
            return Err(Error::ChunkNotifyFailed {
                error,
            });
        }
        Ok(())
    }
}

impl Iterator for WipeStats {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.stats.next()
    }
}

/// Iterator to keep track of a wipe.
#[derive(Default, Debug)]
pub(crate) struct WipeIterator {
    /// The starting offset to wipe.
    start_offset: u64,
    /// Total bytes to be wiped.
    pub(crate) total_bytes: u64,
    /// Size of a chunk.
    /// # Note: The last chunk may be of a smaller size if the chunk size is not a multiple of the total size.
    pub(crate) chunk_size_bytes: u64,
    pub(crate) extra_chunk_size_bytes: Option<u64>,

    /// How many chunks we've wiped so far.
    pub(crate) wiped_chunks: u64,
    /// How many byes we've wiped so far.
    pub(crate) wiped_bytes: u64,
    /// Remaining chunks to be wiped.
    pub(crate) remaining_chunks: u64,
    /// Number of chunks to wipe.
    pub(crate) total_chunks: u64,
}
impl WipeIterator {
    fn new(
        start_offset: u64,
        total_bytes: u64,
        chunk_size_bytes: u64,
        block_len: u64,
    ) -> Result<Self, Error> {
        snafu::ensure!(total_bytes > 0, ZeroBdev {});

        let chunk_size_bytes = if chunk_size_bytes == 0 {
            total_bytes
        } else {
            chunk_size_bytes
        };

        snafu::ensure!(chunk_size_bytes <= total_bytes, ChunkTooLarge {});
        snafu::ensure!(
            chunk_size_bytes % block_len == 0,
            ChunkBlockSizeInvalid {}
        );

        let mut chunks = total_bytes / chunk_size_bytes;
        let remainder = total_bytes % chunk_size_bytes;
        // must be aligned to block device
        snafu::ensure!(remainder % block_len == 0, ChunkBlockSizeInvalid {});
        let extra_chunk_size_bytes = if remainder == 0 {
            None
        } else {
            chunks += 1;
            Some(remainder)
        };

        Ok(Self {
            start_offset,
            total_bytes,
            chunk_size_bytes,
            extra_chunk_size_bytes,
            wiped_chunks: 0,
            wiped_bytes: 0,
            remaining_chunks: chunks,
            total_chunks: chunks,
        })
    }
    fn complete_chunk(&mut self, size: u64) {
        self.wiped_chunks += 1;
        self.wiped_bytes += size;
        self.remaining_chunks -= 1;
    }
}
impl Iterator for WipeIterator {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        // we've wiped, let caller bail out.
        if self.wiped_chunks >= self.total_chunks {
            None
        } else {
            let offset =
                self.start_offset + (self.wiped_chunks * self.chunk_size_bytes);
            match self.extra_chunk_size_bytes {
                // the very last chunk might have a different size is the bdev
                // size is not an exact multiple of the chunk
                // size.
                Some(size) if self.remaining_chunks == 1 => {
                    Some((offset, size))
                }
                None | Some(_) => Some((offset, self.chunk_size_bytes)),
            }
        }
    }
}
