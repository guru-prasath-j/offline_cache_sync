import '../model/keys.dart';
import '../model/operation.dart';
import '../model/records.dart';

/// The storage port. Records, the op log and engine metadata live behind ONE
/// transaction boundary, so "write locally + enqueue" is atomic.
///
/// Implementations must guarantee:
/// * [transaction] bodies are serialized (no two run concurrently) or are
///   isolated with serializable semantics.
/// * If the body throws, nothing it wrote is visible afterwards.
/// * After a successful commit, [committedChanges] emits the set of record
///   keys whose record or ops were written.
///
/// `offline_cache_sync_test` ships a conformance suite for these rules.
abstract interface class LocalStore {
  Future<R> transaction<R>(Future<R> Function(StoreTxn tx) body);

  /// Keys touched by each committed transaction (records or ops).
  Stream<Set<RecordKey>> get committedChanges;

  Future<void> close();
}

/// Tables available inside one transaction.
abstract interface class StoreTxn {
  RecordTable get records;
  OpLog get ops;
  MetaTable get meta;
}

abstract interface class RecordTable {
  Future<StoredRecord?> get(RecordKey key);

  Future<void> put(StoredRecord record);

  Future<void> delete(RecordKey key);

  /// Updates access statistics for [keys] (batched by the engine).
  Future<void> markAccessed(Map<RecordKey, int> accessCounts, DateTime at);

  /// Records that may be evicted: not pinned and without any op whose status
  /// blocks the entity. Ordered by `lastAccessedAt` ascending.
  Future<List<StoredRecord>> evictionCandidates({required int limit});

  Future<StoreStats> stats();
}

abstract interface class OpLog {
  Future<void> append(Operation op);

  /// Replaces an existing op (same id). Throws [StateError] if it is missing.
  Future<void> replace(Operation op);

  Future<Operation?> get(OpId id);

  Future<void> remove(OpId id);

  /// All ops of [key] that are not terminal, ordered by `seq`.
  Future<List<Operation>> forEntity(RecordKey key);

  /// For every entity, its oldest op that is not terminal, ordered by `seq`.
  /// The engine decides which heads are ready.
  Future<List<Operation>> heads({required int limit});

  /// Ops with the given statuses, ordered by `seq`.
  Future<List<Operation>> where(Set<OpStatus> statuses, {int? limit});

  Future<Map<OpStatus, int>> countByStatus();
}

/// Small key/value table for engine bookkeeping (sequence, lease, epochs).
abstract interface class MetaTable {
  Future<Object?> get(String key);

  Future<void> put(String key, Object? value);

  /// Atomically increments and returns the next value of counter [name].
  Future<int> next(String name);
}

final class StoreStats {
  const StoreStats({required this.records, required this.bytes});

  final int records;
  final int bytes;

  @override
  String toString() => 'StoreStats(records: $records, bytes: $bytes)';
}
