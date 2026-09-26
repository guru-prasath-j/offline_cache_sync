import 'dart:async';

import 'package:offline_cache_sync/adapter_api.dart';
import 'package:sqlite3/common.dart' show CommonDatabase;

import 'mutex.dart';
import 'row_codec.dart';
import 'schema.dart';

/// A [LocalStore] backed by a SQLite database from `package:sqlite3`.
///
/// Records, the op log and engine metadata live in three tables of the same
/// database, so every engine transaction (for example "write locally and
/// enqueue") commits or rolls back as one SQLite transaction.
///
/// ```dart
/// final store = SqliteLocalStore(sqlite3.open('offline.db'));
/// ```
///
/// Works with any `CommonDatabase`: native databases from
/// `package:sqlite3/sqlite3.dart` and WebAssembly databases from
/// `package:sqlite3/wasm.dart`.
///
/// Notes:
/// * Transactions run one at a time. Do not start a transaction on the same
///   store from inside a transaction body: it waits for the outer one and
///   deadlocks.
/// * Timestamps are read back as UTC `DateTime`s (the same instant, with
///   microsecond precision).
/// * Payloads, versions and metadata values are stored as JSON, so they must be
///   JSON-encodable, as the engine already requires.
/// * The store owns [database] and closes it in [close] unless
///   `closeDatabase` is false.
final class SqliteLocalStore implements LocalStore {
  /// Wraps [database], creating or upgrading the tables if needed.
  ///
  /// For file databases this also enables WAL journaling with
  /// `synchronous = FULL`, so a committed op survives an app crash or power
  /// loss. Pass `configure: false` to keep your own pragmas.
  SqliteLocalStore(this.database, {bool closeDatabase = true, bool configure = true})
      : _closeDatabase = closeDatabase {
    if (configure) {
      database.execute('PRAGMA journal_mode = WAL');
      database.execute('PRAGMA synchronous = FULL');
    }
    migrate(database);
  }

  /// The underlying database. Tables are prefixed with `ocs_`, so it can be
  /// shared with your own tables.
  final CommonDatabase database;

  final bool _closeDatabase;
  final Mutex _mutex = Mutex();
  final StreamController<Set<RecordKey>> _changes = StreamController<Set<RecordKey>>.broadcast();
  bool _closed = false;

  @override
  Stream<Set<RecordKey>> get committedChanges => _changes.stream;

  @override
  Future<R> transaction<R>(Future<R> Function(StoreTxn tx) body) {
    if (_closed) return Future.error(StateError('store closed'));
    return _mutex.protect(() async {
      final txn = _SqliteTxn(database);
      database.execute('BEGIN IMMEDIATE');
      try {
        final result = await body(txn);
        txn._done = true;
        database.execute('COMMIT');
        if (txn.touched.isNotEmpty && !_changes.isClosed) {
          _changes.add(Set.unmodifiable(txn.touched));
        }
        return result;
      } catch (_) {
        txn._done = true;
        if (!database.autocommit) database.execute('ROLLBACK');
        rethrow;
      }
    });
  }

  /// Waits for queued transactions to finish, then closes the stream and, by
  /// default, the database.
  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    await _mutex.protect(() async {});
    if (_closeDatabase) database.close();
    await _changes.close();
  }
}

final class _SqliteTxn implements StoreTxn {
  _SqliteTxn(this.db);

  final CommonDatabase db;
  final Set<RecordKey> touched = {};
  bool _done = false;

  void check() {
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

  final _SqliteTxn _t;

  CommonDatabase get _db => _t.db;

  @override
  Future<StoredRecord?> get(RecordKey key) async {
    _t.check();
    final rows = _db.select('SELECT $recordColumns FROM ocs_records WHERE key = ?', [key.value]);
    return rows.isEmpty ? null : recordFromRow(rows.first);
  }

  @override
  Future<void> put(StoredRecord record) async {
    _t.check();
    final size = record.sizeBytes > 0 ? record.sizeBytes : estimateSize(record.payload);
    final r = record.sizeBytes == size ? record : record.copyWith(sizeBytes: size);
    _db.execute('INSERT OR REPLACE INTO ocs_records ($recordColumns) VALUES ${placeholders(13)}', recordValues(r));
    _t.touched.add(record.key);
  }

  @override
  Future<void> delete(RecordKey key) async {
    _t.check();
    _db.execute('DELETE FROM ocs_records WHERE key = ?', [key.value]);
    if (_db.updatedRows > 0) _t.touched.add(key);
  }

  @override
  Future<void> markAccessed(Map<RecordKey, int> accessCounts, DateTime at) async {
    _t.check();
    final stmt = _db.prepare(
      'UPDATE ocs_records SET last_accessed_at = ?, access_count = access_count + ? WHERE key = ?',
    );
    try {
      accessCounts.forEach((key, n) => stmt.execute([encodeTime(at), n, key.value]));
    } finally {
      stmt.close();
    }
    // Access bookkeeping does not change values: no change notification.
  }

  @override
  Future<List<StoredRecord>> evictionCandidates({required int limit}) async {
    _t.check();
    final rows = _db.select(
      'SELECT $recordColumns FROM ocs_records r WHERE r.pinned = 0 AND NOT EXISTS ('
      'SELECT 1 FROM ocs_ops o WHERE o.entity = r.key AND o.status NOT IN $terminalStatuses'
      ') ORDER BY r.last_accessed_at, r.key LIMIT ?',
      [limit],
    );
    return [for (final row in rows) recordFromRow(row)];
  }

  @override
  Future<StoreStats> stats() async {
    _t.check();
    final row = _db.select('SELECT COUNT(*) AS n, COALESCE(SUM(size_bytes), 0) AS b FROM ocs_records').first;
    return StoreStats(records: row['n'] as int, bytes: row['b'] as int);
  }
}

final class _Ops implements OpLog {
  _Ops(this._t);

  final _SqliteTxn _t;

  CommonDatabase get _db => _t.db;

  List<Operation> _query(String where, [List<Object?> args = const []]) =>
      [for (final row in _db.select('SELECT $opColumns FROM ocs_ops $where', args)) opFromRow(row)];

  bool _exists(OpId id) => _db.select('SELECT 1 FROM ocs_ops WHERE id = ?', [id.value]).isNotEmpty;

  @override
  Future<void> append(Operation op) async {
    _t.check();
    if (_exists(op.id)) throw StateError('duplicate op id ${op.id.value}');
    _db.execute('INSERT INTO ocs_ops ($opColumns) VALUES ${placeholders(19)}', opValues(op));
    _t.touched.add(op.entity);
  }

  @override
  Future<void> replace(Operation op) async {
    _t.check();
    if (!_exists(op.id)) throw StateError('unknown op id ${op.id.value}');
    _db.execute('INSERT OR REPLACE INTO ocs_ops ($opColumns) VALUES ${placeholders(19)}', opValues(op));
    _t.touched.add(op.entity);
  }

  @override
  Future<Operation?> get(OpId id) async {
    _t.check();
    final ops = _query('WHERE id = ?', [id.value]);
    return ops.isEmpty ? null : ops.first;
  }

  @override
  Future<void> remove(OpId id) async {
    _t.check();
    final rows = _db.select('SELECT entity FROM ocs_ops WHERE id = ?', [id.value]);
    if (rows.isEmpty) return;
    _db.execute('DELETE FROM ocs_ops WHERE id = ?', [id.value]);
    _t.touched.add(RecordKey.parse(rows.first['entity'] as String));
  }

  @override
  Future<List<Operation>> forEntity(RecordKey key) async {
    _t.check();
    return _query('WHERE entity = ? AND status NOT IN $terminalStatuses ORDER BY seq', [key.value]);
  }

  @override
  Future<List<Operation>> heads({required int limit}) async {
    _t.check();
    return [
      for (final row in _db.select(
        'SELECT ${_prefixed('o')} FROM ocs_ops o JOIN ('
        'SELECT entity, MIN(seq) AS head FROM ocs_ops WHERE status NOT IN $terminalStatuses GROUP BY entity'
        ') h ON o.entity = h.entity AND o.seq = h.head ORDER BY o.seq LIMIT ?',
        [limit],
      ))
        opFromRow(row),
    ];
  }

  @override
  Future<List<Operation>> where(Set<OpStatus> statuses, {int? limit}) async {
    _t.check();
    if (statuses.isEmpty) return const [];
    final names = [for (final s in statuses) s.name];
    final limitClause = limit != null ? ' LIMIT ?' : '';
    return _query(
      'WHERE status IN ${placeholders(names.length)} ORDER BY seq$limitClause',
      [...names, if (limit != null) limit],
    );
  }

  @override
  Future<Map<OpStatus, int>> countByStatus() async {
    _t.check();
    return {
      for (final row in _db.select('SELECT status, COUNT(*) AS n FROM ocs_ops GROUP BY status'))
        OpStatus.values.byName(row['status'] as String): row['n'] as int,
    };
  }
}

String _prefixed(String alias) => opColumns.split(', ').map((c) => '$alias.$c').join(', ');

final class _Meta implements MetaTable {
  _Meta(this._t);

  final _SqliteTxn _t;

  CommonDatabase get _db => _t.db;

  @override
  Future<Object?> get(String key) async {
    _t.check();
    final rows = _db.select('SELECT value FROM ocs_meta WHERE key = ?', [key]);
    return rows.isEmpty ? null : decodeJson(rows.first['value']);
  }

  @override
  Future<void> put(String key, Object? value) async {
    _t.check();
    if (value == null) {
      _db.execute('DELETE FROM ocs_meta WHERE key = ?', [key]);
    } else {
      _db.execute('INSERT OR REPLACE INTO ocs_meta (key, value) VALUES (?, ?)', [key, encodeJson(value)]);
    }
  }

  @override
  Future<int> next(String name) async {
    _t.check();
    final current = await get(name);
    final n = ((current as int?) ?? 0) + 1;
    await put(name, n);
    return n;
  }
}
