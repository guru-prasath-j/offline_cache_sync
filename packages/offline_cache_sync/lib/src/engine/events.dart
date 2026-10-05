import '../model/keys.dart';
import '../model/operation.dart';
import '../policy/freshness.dart';
import '../ports/runtime.dart';

/// Typed engine events. They carry keys, ids, kinds, timings and error codes,
/// never payloads or headers.
sealed class SyncEvent {
  /// Creates an event that happened [at].
  const SyncEvent(this.at);

  /// When the event happened, from the engine's `Clock`.
  final DateTime at;
}

// ---- cache -----------------------------------------------------------------

/// A read was answered from the local cache.
final class CacheHit extends SyncEvent {
  /// Creates a cache-hit event.
  const CacheHit(super.at, this.key, this.freshness);

  /// The record this event is about.
  final RecordKey key;
  /// How fresh the cached value was when served.
  final Freshness freshness;
}

/// A read found nothing usable in the cache and had to fetch.
final class CacheMiss extends SyncEvent {
  /// Creates a cache-miss event.
  const CacheMiss(super.at, this.key);

  /// The record this event is about.
  final RecordKey key;
}

/// A cached record was revalidated against the server.
final class Revalidated extends SyncEvent {
  /// Creates a revalidation event.
  const Revalidated(super.at, this.key, {required this.notModified, required this.latency});

  /// The record this event is about.
  final RecordKey key;
  /// Whether the server answered "not modified" (for example HTTP 304).
  final bool notModified;
  /// How long the revalidation round trip took.
  final Duration latency;
}

/// Why a stale value was served instead of a fresh one.
enum StaleReason { error, offline }

/// A stale value was served because a fresh one could not be fetched.
final class ServedStale extends SyncEvent {
  /// Creates a served-stale event.
  const ServedStale(super.at, this.key, this.reason);

  /// The record this event is about.
  final RecordKey key;
  /// Whether the fetch failed or the device was offline.
  final StaleReason reason;
}

/// A fetch from the remote source failed.
final class FetchFailed extends SyncEvent {
  /// Creates a fetch-failed event.
  const FetchFailed(super.at, this.key, this.errorType);

  /// The record this event is about.
  final RecordKey key;

  /// Runtime type name of the error (no message: it may contain PII).
  final String errorType;
}

/// A fetched value was discarded because a newer write was acknowledged
/// after the fetch started, or the response carried an older version.
final class StaleResponseDiscarded extends SyncEvent {
  /// Creates a stale-response event.
  const StaleResponseDiscarded(super.at, this.key);

  /// The record this event is about.
  final RecordKey key;
}

/// Records were evicted to stay within the retention budget.
final class Evicted extends SyncEvent {
  /// Creates an eviction event.
  const Evicted(super.at, {required this.count, required this.bytes});

  /// Number of records evicted.
  final int count;
  /// Approximate bytes freed.
  final int bytes;
}

// ---- queue -----------------------------------------------------------------

/// Base class for events about a queued operation (outbox entry).
sealed class OpEvent extends SyncEvent {
  /// Creates an operation event.
  const OpEvent(super.at, this.opId, this.entity, this.kind);

  /// The operation's id.
  final OpId opId;
  /// The record the operation changes.
  final RecordKey entity;
  /// The mutation kind, for example `$set` or `$delete`.
  final String kind;
}

/// An operation was durably added to the outbox.
final class OpEnqueued extends OpEvent {
  /// Creates an enqueued event.
  const OpEnqueued(super.at, super.opId, super.entity, super.kind);
}

/// An operation was handed to the `MutationSender`.
final class OpSent extends OpEvent {
  /// Creates a sent event.
  const OpSent(super.at, super.opId, super.entity, super.kind, this.attempt);

  /// Attempt number, starting at 1.
  final int attempt;
}

/// The server acknowledged an operation.
final class OpSucceeded extends OpEvent {
  /// Creates a succeeded event.
  const OpSucceeded(super.at, super.opId, super.entity, super.kind, this.latency);

  /// From enqueue to acknowledgement.
  final Duration latency;
}

/// A send failed with a retryable error and a retry was scheduled.
final class OpRetryScheduled extends OpEvent {
  /// Creates a retry-scheduled event.
  const OpRetryScheduled(super.at, super.opId, super.entity, super.kind, this.nextAttemptAt, this.error);

  /// When the next attempt is due.
  final DateTime nextAttemptAt;
  /// The error that caused the retry.
  final ErrorInfo error;
}

/// The server reported a conflict and the resource's resolver ran.
final class OpConflicted extends OpEvent {
  /// Creates a conflicted event.
  const OpConflicted(super.at, super.opId, super.entity, super.kind, this.resolution);

  /// `TakeServer`, `ResendLocal`, `MergeWith` or `Park`.
  final String resolution;
}

/// An operation failed permanently (rejected or ambiguous).
final class OpFailed extends OpEvent {
  /// Creates a failed event.
  const OpFailed(super.at, super.opId, super.entity, super.kind, this.error);

  /// Why the operation failed.
  final ErrorInfo error;
}

/// An operation ran out of retries or could not be decoded and was dead-lettered.
final class OpDeadLettered extends OpEvent {
  /// Creates a dead-lettered event.
  const OpDeadLettered(super.at, super.opId, super.entity, super.kind, this.error);

  /// The last error before the operation was dead-lettered.
  final ErrorInfo error;
}

/// An operation was discarded before it was acknowledged.
final class OpCancelled extends OpEvent {
  /// Creates a cancelled event.
  const OpCancelled(super.at, super.opId, super.entity, super.kind);
}

/// An op found `inFlight` at startup (the previous process died mid-send).
final class OpRecovered extends OpEvent {
  /// Creates a recovered event.
  const OpRecovered(super.at, super.opId, super.entity, super.kind, {required this.resent});

  /// True if it was put back to `pending`; false if marked `failed(ambiguous)`.
  final bool resent;
}

// ---- runtime ---------------------------------------------------------------

/// The `ConnectivityProvider` reported a new reachability state.
final class ReachabilityChanged extends SyncEvent {
  /// Creates a reachability event.
  const ReachabilityChanged(super.at, this.reachability);

  /// The new reachability state.
  final Reachability reachability;
}

/// This engine gained or lost the single-writer lease.
final class LeaseChanged extends SyncEvent {
  /// Creates a lease event.
  const LeaseChanged(super.at, {required this.held});

  /// Whether this engine now holds the lease.
  final bool held;
}

/// Something unusual but non-fatal (e.g. pins exceed the retention budget).
final class PolicyWarning extends SyncEvent {
  /// Creates a policy warning.
  const PolicyWarning(super.at, this.message);

  /// What was unusual, in plain words.
  final String message;
}

/// Receives every event. Forward to your logger / APM here.
abstract interface class EventSink {
  /// Called once for every event, synchronously, in order.
  void emit(SyncEvent event);
}

/// Running counters folded from events. Queue gauges come from the store.
final class SyncMetrics {
  /// Reads answered from the cache.
  int cacheHits = 0;
  /// Reads that had to fetch.
  int cacheMisses = 0;
  /// Stale values served because a fetch failed.
  int staleServedOnError = 0;
  /// Stale values served because the device was offline.
  int staleServedOffline = 0;
  /// Revalidation round trips.
  int revalidations = 0;
  /// Revalidations the server answered with "not modified".
  int notModified = 0;
  /// Failed fetches.
  int fetchFailures = 0;
  /// Records evicted.
  int evicted = 0;
  /// Operations added to the outbox.
  int opsEnqueued = 0;
  /// Send attempts.
  int opsSent = 0;
  /// Operations acknowledged by the server.
  int opsSucceeded = 0;
  /// Retries scheduled.
  int opsRetried = 0;
  /// Conflicts reported by the server.
  int opsConflicted = 0;
  /// Operations that failed permanently.
  int opsFailed = 0;
  /// Operations dead-lettered.
  int opsDeadLettered = 0;
  /// Operations recovered after a crash.
  int opsRecovered = 0;
  /// When the last operation was acknowledged, if any.
  DateTime? lastSuccessfulSync;
  /// The latest reachability state.
  Reachability reachability = Reachability.unknown;

  /// Folds [e] into the counters.
  void record(SyncEvent e) {
    switch (e) {
      case CacheHit():
        cacheHits++;
      case CacheMiss():
        cacheMisses++;
      case ServedStale(reason: StaleReason.error):
        staleServedOnError++;
      case ServedStale(reason: StaleReason.offline):
        staleServedOffline++;
      case Revalidated(:final notModified):
        revalidations++;
        if (notModified) this.notModified++;
      case FetchFailed():
        fetchFailures++;
      case Evicted(:final count):
        evicted += count;
      case OpEnqueued():
        opsEnqueued++;
      case OpSent():
        opsSent++;
      case OpSucceeded(:final at):
        opsSucceeded++;
        lastSuccessfulSync = at;
      case OpRetryScheduled():
        opsRetried++;
      case OpConflicted():
        opsConflicted++;
      case OpFailed():
        opsFailed++;
      case OpDeadLettered():
        opsDeadLettered++;
      case OpRecovered():
        opsRecovered++;
      case ReachabilityChanged(reachability: final r):
        reachability = r;
      case StaleResponseDiscarded() || OpCancelled() || LeaseChanged() || PolicyWarning():
        break;
    }
  }

  /// The counters as a JSON-compatible map.
  Map<String, Object?> toJson() => {
        'cacheHits': cacheHits,
        'cacheMisses': cacheMisses,
        'staleServedOnError': staleServedOnError,
        'staleServedOffline': staleServedOffline,
        'revalidations': revalidations,
        'notModified': notModified,
        'fetchFailures': fetchFailures,
        'evicted': evicted,
        'opsEnqueued': opsEnqueued,
        'opsSent': opsSent,
        'opsSucceeded': opsSucceeded,
        'opsRetried': opsRetried,
        'opsConflicted': opsConflicted,
        'opsFailed': opsFailed,
        'opsDeadLettered': opsDeadLettered,
        'opsRecovered': opsRecovered,
        'lastSuccessfulSync': lastSuccessfulSync?.toIso8601String(),
        'reachability': reachability.name,
      };
}
