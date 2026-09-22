import '../model/keys.dart';
import '../policy/freshness.dart';
import '../resource/conflicts.dart';

/// Where the returned base came from.
enum DataSource {
  /// Local store, no request made for this read.
  cache,

  /// A request completed during this read.
  network,

  /// Nothing available.
  none,
}

/// Thrown into [Snapshot.error] when an entity is needed, the device is
/// offline and nothing usable is cached.
final class OfflineMiss implements Exception {
  const OfflineMiss();

  @override
  String toString() => 'OfflineMiss: not cached and offline';
}

/// The result of a read: the folded value (server base plus pending local
/// writes) and everything the UI needs to present it honestly.
final class Snapshot<T> {
  const Snapshot({
    required this.key,
    required this.value,
    required this.freshness,
    required this.source,
    this.pendingWrites = 0,
    this.hasConflicts = false,
    this.hasFailedWrites = false,
    this.servedOnError = false,
    this.servedOffline = false,
    this.isRevalidating = false,
    this.error,
    this.fetchedAt,
  });

  final RecordKey key;

  /// `fold(base, pendingOps)`. Null when the entity does not exist (never
  /// fetched, deleted on the server, or deleted by a pending local op).
  final T? value;

  /// Freshness of the server-confirmed BASE (pending writes don't change it).
  final Freshness freshness;
  final DataSource source;

  /// Number of pending / in-flight / parked ops folded into [value].
  final int pendingWrites;

  /// At least one op for this entity is parked in `conflicted`.
  final bool hasConflicts;

  /// At least one op for this entity is `failed` or `deadLetter` (and is NOT
  /// folded into [value]).
  final bool hasFailedWrites;

  /// An expired value served because the fetch failed (stale-if-error).
  final bool servedOnError;

  /// An expired value served because the device is offline.
  final bool servedOffline;

  /// A background revalidation was started by this read.
  final bool isRevalidating;

  /// The fetch error, or [OfflineMiss]. A snapshot can carry both a usable
  /// [value] and an [error].
  final Object? error;

  /// When the server last confirmed the base.
  final DateTime? fetchedAt;

  bool get hasValue => value != null;
  bool get hasPendingWrites => pendingWrites > 0;

  /// The value, or throws [error] / [StateError].
  T get requireValue {
    final v = value;
    if (v != null) return v;
    final e = error;
    if (e != null) throw e;
    throw StateError('No value for ${key.value}');
  }

  /// Used by `watch` to suppress duplicate emissions.
  bool sameAs(Snapshot<T>? other, Object? Function(T value) encode) {
    if (other == null) return false;
    final a = value, b = other.value;
    final valuesEqual = (a == null || b == null) ? a == b : jsonEquals(encode(a), encode(b));
    return valuesEqual &&
        freshness == other.freshness &&
        pendingWrites == other.pendingWrites &&
        hasConflicts == other.hasConflicts &&
        hasFailedWrites == other.hasFailedWrites &&
        servedOffline == other.servedOffline &&
        servedOnError == other.servedOnError &&
        (error == null) == (other.error == null);
  }

  @override
  String toString() => 'Snapshot(${key.value}, value: $value, $freshness, $source'
      '${pendingWrites > 0 ? ', pending: $pendingWrites' : ''}'
      '${servedOffline ? ', servedOffline' : ''}${servedOnError ? ', servedOnError' : ''}'
      '${error != null ? ', error: $error' : ''})';
}
