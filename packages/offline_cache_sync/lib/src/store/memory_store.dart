import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import '../model/keys.dart';
import '../model/operation.dart';
import '../model/records.dart';
import '../ports/store.dart';
import 'mutex.dart';

/// A [LocalStore] kept in memory.
///
/// Transactions are serialized and atomic (copy-on-write: the body works on a
/// copy that replaces the committed state only if it completes). The store
/// object outlives any engine using it, so tests simulate an app restart by
/// opening a new engine on the same store.
final class InMemoryLocalStore implements LocalStore {
  InMemoryLocalStore();

  final Mutex _mutex = Mutex();
  final StreamController<Set<RecordKey>> _changes = StreamController<Set<RecordKey>>.broadcast(sync: false);

  _State _state = _State.empty();
  bool _closed = false;

  /// Read-only view of committed records (for tests and inspectors).
  Map<RecordKey, StoredRecord> get debugRecords => UnmodifiableMapView(_state.records);

  /// Read-only view of committed ops (for tests and inspectors).
  Map<OpId, Operation> get debugOps => UnmodifiableMapView(_state.ops);

  @override
  Stream<Set<RecordKey>> get committedChanges => _changes.stream;

  @override
  Future<R> transaction<R>(Future<R> Function(StoreTxn tx) body) {
    if (_closed) return Future.error(StateError('store closed'));
    return _mutex.protect(() async {
      final working = _state.copy();
      final txn = _MemoryTxn(working);
      final result = await body(txn);
      txn._done = true;
      _state = working;
      if (txn.touched.isNotEmpty && !_changes.isClosed) _changes.add(Set.unmodifiable(txn.touched));
      return result;
    });
  }

  @override
  Future<void> close() async {
    _closed = true;
    await _changes.close();
  }
}

final class _State {
  _State(this.records, this.ops, this.meta);

  _State.empty() : this({}, {}, {});

  final Map<RecordKey, StoredRecord> records;
  final Map<OpId, Operation> ops;
  final Map<String, Object?> meta;

  // Records and ops are immutable values, so shallow copies are enough.
  _State copy() => _State(Map.of(records), Map.of(ops), Map.of(meta));
}

final class _MemoryTxn implements StoreTxn {
  _MemoryTxn(this._s);

  final _State _s;
  final Set<RecordKey> touched = {};
  bool _done = false;

  void _check() {
    if (_done) throw StateError('transaction already completed');
  }

  @override
  late final RecordTable records = _Records(this);

  @override
  late final OpLog ops = _Ops(this);

  @override
  late final MetaTable meta = _Meta(this);
}

final class _Records implements RecordTable {
  _Records(this._t);

  final _MemoryTxn _t;

  Map<RecordKey, StoredRecord> get _m => _t._s.records;

  @override
  Future<StoredRecord?> get(RecordKey key) async {
    _t._check();
    return _m[key];
  }

  @override
  Future<void> put(StoredRecord record) async {
    _t._check();
    final size = record.sizeBytes > 0 ? record.sizeBytes : estimateSize(record.payload);
    _m[record.key] = record.sizeBytes == size ? record : record.copyWith(sizeBytes: size);
    _t.touched.add(record.key);
  }

  @override
  Future<void> delete(RecordKey key) async {
    _t._check();
    if (_m.remove(key) != null) _t.touched.add(key);
  }

  @override
  Future<void> markAccessed(Map<RecordKey, int> accessCounts, DateTime at) async {
    _t._check();
    accessCounts.forEach((key, n) {
      final r = _m[key];
      if (r != null) _m[key] = r.copyWith(lastAccessedAt: at, accessCount: r.accessCount + n);
    });
    // Access bookkeeping does not change values: no change notification.
  }

  @override
  Future<List<StoredRecord>> evictionCandidates({required int limit}) async {
    _t._check();
    final blocked = <RecordKey>{
      for (final op in _t._s.ops.values)
        if (op.status.blocksEntity) op.entity,
    };
    final list = _m.values.where((r) => !r.pinned && !blocked.contains(r.key)).toList()
      ..sort((a, b) => a.lastAccessedAt.compareTo(b.lastAccessedAt));
    return list.length > limit ? list.sublist(0, limit) : list;
  }

  @override
  Future<StoreStats> stats() async {
    _t._check();
    var bytes = 0;
    for (final r in _m.values) {
      bytes += r.sizeBytes;
    }
    return StoreStats(records: _m.length, bytes: bytes);
  }
}

final class _Ops implements OpLog {
  _Ops(this._t);

  final _MemoryTxn _t;

  Map<OpId, Operation> get _m => _t._s.ops;

  List<Operation> _sorted(Iterable<Operation> ops) => ops.toList()..sort((a, b) => a.seq.compareTo(b.seq));

  @override
  Future<void> append(Operation op) async {
    _t._check();
    if (_m.containsKey(op.id)) throw StateError('duplicate op id ${op.id.value}');
    _m[op.id] = op;
    _t.touched.add(op.entity);
  }

  @override
  Future<void> replace(Operation op) async {
    _t._check();
    if (!_m.containsKey(op.id)) throw StateError('unknown op id ${op.id.value}');
    _m[op.id] = op;
    _t.touched.add(op.entity);
  }

  @override
  Future<Operation?> get(OpId id) async {
    _t._check();
    return _m[id];
  }

  @override
  Future<void> remove(OpId id) async {
    _t._check();
    final op = _m.remove(id);
    if (op != null) _t.touched.add(op.entity);
  }

  @override
  Future<List<Operation>> forEntity(RecordKey key) async {
    _t._check();
    return _sorted(_m.values.where((o) => o.entity == key && !o.status.isTerminal));
  }

  @override
  Future<List<Operation>> heads({required int limit}) async {
    _t._check();
    final byEntity = <RecordKey, Operation>{};
    for (final op in _m.values) {
      if (op.status.isTerminal) continue;
      final cur = byEntity[op.entity];
      if (cur == null || op.seq < cur.seq) byEntity[op.entity] = op;
    }
    final heads = _sorted(byEntity.values);
    return heads.length > limit ? heads.sublist(0, limit) : heads;
  }

  @override
  Future<List<Operation>> where(Set<OpStatus> statuses, {int? limit}) async {
    _t._check();
    final list = _sorted(_m.values.where((o) => statuses.contains(o.status)));
    return limit != null && list.length > limit ? list.sublist(0, limit) : list;
  }

  @override
  Future<Map<OpStatus, int>> countByStatus() async {
    _t._check();
    final counts = <OpStatus, int>{};
    for (final op in _m.values) {
      counts[op.status] = (counts[op.status] ?? 0) + 1;
    }
    return counts;
  }
}

final class _Meta implements MetaTable {
  _Meta(this._t);

  final _MemoryTxn _t;

  @override
  Future<Object?> get(String key) async {
    _t._check();
    return _t._s.meta[key];
  }

  @override
  Future<void> put(String key, Object? value) async {
    _t._check();
    if (value == null) {
      _t._s.meta.remove(key);
    } else {
      _t._s.meta[key] = value;
    }
  }

  @override
  Future<int> next(String name) async {
    _t._check();
    final n = ((_t._s.meta[name] as int?) ?? 0) + 1;
    _t._s.meta[name] = n;
    return n;
  }
}

/// Approximate encoded size of a JSON-compatible value, in bytes.
int estimateSize(Object? payload) {
  if (payload == null) return 0;
  try {
    return utf8.encode(jsonEncode(payload)).length;
  } on Object {
    return 0;
  }
}
