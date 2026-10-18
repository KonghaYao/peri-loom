//! The figures W4 asks the data face to collect before it decides anything.
//!
//! Every field of [`MaintenanceSignals`] is read from state the process already holds:
//! lengths of in-memory buffers, counters, and one age. Collecting them takes the `RemoteIO`
//! state lock and the storage lock once and performs no object-store request, which is what
//! makes it safe to call from a periodic tick. The two inputs that would need a store read —
//! the number of `wal/index` entries and the encoded index size — are deliberately *not*
//! here: they are observed where the index is already read, through `work_metrics` (group D),
//! so no tick pays for a GET.
//!
//! Units are stated per field. `replay_bytes` is the one field with a precondition rather
//! than a plain length, because "what a cold start has to replay" is only the WAL mirror
//! while the published baseline is behind the durable LSN.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Cheap maintenance inputs for one database.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MaintenanceSignals {
    /// Native WAL mirror bytes held in memory. This is the volume a Full checkpoint would
    /// backfill, so it is the watermark input for the checkpoint lane.
    pub wal_bytes: u64,
    /// Payload bytes of the pending WAL batch: delta bytes that are not durable yet.
    pub pending_wal_bytes: u64,
    /// Pending WAL operations (delta records awaiting an append).
    pub delta_count: u64,
    /// Bytes of the WAL mirror a cold start still has to apply, i.e. the part not covered by
    /// the published baseline. Zero when the baseline is current.
    pub replay_bytes: u64,
    /// Segment payload bytes that are not durable under the current manifest.
    pub dirty_segment_bytes: u64,
    /// Age of the oldest lane that currently holds a native read lock. Zero when no reader
    /// holds one. This is the reader a truncate checkpoint cannot backfill past.
    pub oldest_reader_ms: u64,
    /// Milliseconds since the last foreground statement started on this database, measured
    /// from the database's open instant. The compaction lane takes the writer admission and
    /// the operations gate for its whole unit, so a unit started under load would hold every
    /// writer for its multi-second duration; the lane waits for this to pass its quiet
    /// threshold. A passive checkpoint does not use the figure: it never excludes writers.
    pub quiet_for_ms: u64,
    /// True while an append or a database publication is unsettled: an append in flight, a
    /// pending batch, a dirty image or a failed publication. The image's applied LSN is not
    /// part of this flag because the truncate checkpoint itself applies the WAL tail and
    /// `allow_reset` re-checks coverage after that backfill.
    pub unconfirmed_wal: bool,
}

impl MaintenanceSignals {
    /// The volume a unit would backfill, used as the byte charge against the budget.
    pub(crate) fn backfill_bytes(&self) -> u64 {
        self.wal_bytes
    }
}

/// Highest reader age seen on one database.
///
/// The age is measured from the session's open rather than from its first read, so it is an
/// upper bound on how long the reader has been blocking: a session that has been open a long
/// time but only just started reading reports more age than the reader really has. That is the
/// safe direction for a figure that only gates maintenance, and it needs no native hook.
///
/// Only the peak is stored, so an export taken after the reader left can still report who
/// blocked reclamation. Nothing here holds a reader alive, and nothing here is consulted
/// before reclaiming: the reader's own native read lock is what blocks a truncate checkpoint,
/// exactly as before.
#[derive(Default)]
pub(crate) struct ReaderAge {
    peak_ms: AtomicU64,
}

impl ReaderAge {
    /// Highest age seen so far.
    pub(crate) fn peak(&self) -> Duration {
        Duration::from_millis(self.peak_ms.load(Ordering::Relaxed))
    }

    pub(crate) fn observe(&self, age: Duration) {
        if !age.is_zero() {
            self.peak_ms.fetch_max(millis(age), Ordering::Relaxed);
        }
    }
}

pub(crate) fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
