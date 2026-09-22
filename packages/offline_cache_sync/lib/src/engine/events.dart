import '../model/keys.dart';
import '../model/operation.dart';
import '../policy/freshness.dart';
import '../ports/runtime.dart';

/// Typed engine events. They carry keys, ids, kinds, timings and error codes,
/// never payloads or headers.
sealed class SyncEvent {
  const SyncEvent(this.at);

  final DateTime at;
}

// ---- cache -----------------------------------------------------------------

final class CacheHit extends SyncEvent {
  const CacheHit(super.at, this.key, this.freshness);

  final RecordKey key;
  final Freshness freshness;
}

final class CacheMiss extends SyncEvent {
  const CacheMiss(super.at, this.key);

  final RecordKey key;
}

final class Revalidated extends SyncEvent {
  const Revalidated(super.at, this.key, {required this.notModified, required this.latency});

  final RecordKey key;
  final bool notModified;
  final Duration latency;
}

enum StaleReason { error, offline }

final class ServedStale extends SyncEvent {
  const ServedStale(super.at, this.key, this.reason);

  final RecordKey key;
  final StaleReason reason;
}

final class FetchFailed extends SyncEvent {
  const FetchFailed(super.at, this.key, this.errorType);

  final RecordKey key;

  /// Runtime type name of the error (no message: it may contain PII).
  final String errorType;
}

/// A fetched value was discarded because a newer write was acknowledged
/// after the fetch started, or the response carried an older version.
final class StaleResponseDiscarded extends SyncEvent {
  const StaleResponseDiscarded(super.at, this.key);

  final RecordKey key;
}

final class Evicted extends SyncEvent {
  const Evicted(super.at, {required this.count, required this.bytes});

  final int count;
  final int bytes;
}

// ---- queue -----------------------------------------------------------------

sealed class OpEvent extends SyncEvent {
  const OpEvent(super.at, this.opId, this.entity, this.kind);

  final OpId opId;
  final RecordKey entity;
  final String kind;
}

final class OpEnqueued extends OpEvent {
  const OpEnqueued(super.at, super.opId, super.entity, super.kind);
}

final class OpSent extends OpEvent {
  const OpSent(super.at, super.opId, super.entity, super.kind, this.attempt);

  final int attempt;
}

final class OpSucceeded extends OpEvent {
  const OpSucceeded(super.at, super.opId, super.entity, super.kind, this.latency);

  /// From enqueue to acknowledgement.
  final Duration latency;
}

final class OpRetryScheduled extends OpEvent {
  const OpRetryScheduled(super.at, super.opId, super.entity, super.kind, this.nextAttemptAt, this.error);

  final DateTime nextAttemptAt;
  final ErrorInfo error;
}

final class OpConflicted extends OpEvent {
  const OpConflicted(super.at, super.opId, super.entity, super.kind, this.resolution);

  /// `TakeServer`, `ResendLocal`, `MergeWith` or `Park`.
  final String resolution;
}

final class OpFailed extends OpEvent {
  const OpFailed(super.at, super.opId, super.entity, super.kind, this.error);

  final ErrorInfo error;
}

final class OpDeadLettered extends OpEvent {
  const OpDeadLettered(super.at, super.opId, super.entity, super.kind, this.error);

  final ErrorInfo error;
}

final class OpCancelled extends OpEvent {
  const OpCancelled(super.at, super.opId, super.entity, super.kind);
}

/// An op found `inFlight` at startup (the previous process died mid-send).
final class OpRecovered extends OpEvent {
  const OpRecovered(super.at, super.opId, super.entity, super.kind, {required this.resent});

  /// True if it was put back to `pending`; false if marked `failed(ambiguous)`.
  final bool resent;
}

// ---- runtime ---------------------------------------------------------------

final class ReachabilityChanged extends SyncEvent {
  const ReachabilityChanged(super.at, this.reachability);

  final Reachability reachability;
}

final class LeaseChanged extends SyncEvent {
  const LeaseChanged(super.at, {required this.held});

  final bool held;
}

/// Something unusual but non-fatal (e.g. pins exceed the retention budget).
final class PolicyWarning extends SyncEvent {
  const PolicyWarning(super.at, this.message);

  final String message;
}

/// Receives every event. Forward to your logger / APM here.
abstract interface class EventSink {
  void emit(SyncEvent event);
}

/// Running counters folded from events. Queue gauges come from the store.
final class SyncMetrics {
  int cacheHits = 0;
  int cacheMisses = 0;
  int staleServedOnError = 0;
  int staleServedOffline = 0;
  int revalidations = 0;
  int notModified = 0;
  int fetchFailures = 0;
  int evicted = 0;
  int opsEnqueued = 0;
  int opsSent = 0;
  int opsSucceeded = 0;
  int opsRetried = 0;
  int opsConflicted = 0;
  int opsFailed = 0;
  int opsDeadLettered = 0;
  int opsRecovered = 0;
  DateTime? lastSuccessfulSync;
  Reachability reachability = Reachability.unknown;

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
