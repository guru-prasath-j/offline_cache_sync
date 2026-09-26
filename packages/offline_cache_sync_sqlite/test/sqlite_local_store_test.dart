import 'dart:io';

import 'package:offline_cache_sync/adapter_api.dart';
import 'package:offline_cache_sync_sqlite/offline_cache_sync_sqlite.dart';
import 'package:sqlite3/sqlite3.dart' show sqlite3;
import 'package:test/test.dart';

void main() {
  final t0 = DateTime.utc(2026, 1, 1, 12, 30, 15, 123, 456);

  StoredRecord fullRecord() => StoredRecord(
        key: RecordKey('todo', 'a:b'),
        payload: {
          'title': 'Write docs',
          'tags': ['x', 'y'],
          'done': false,
          'n': 1.5,
        },
        tombstone: false,
        invalidated: true,
        serverVersion: Version(etag: '"v1"', revision: 7, updatedAt: t0),
        validators: const Validators(etag: '"v1"', lastModified: 'Wed, 01 Jan 2026 12:00:00 GMT'),
        fetchedAt: t0,
        httpMaxAge: const Duration(minutes: 5),
        lastAccessedAt: t0.add(const Duration(seconds: 1)),
        accessCount: 4,
        sizeBytes: 99,
        priority: 2,
        pinned: true,
      );

  Operation fullOp() => Operation(
        id: const OpId('op-1'),
        seq: 42,
        entity: RecordKey('todo', 'a:b'),
        kind: 'todo.rename',
        payload: {'title': 'New'},
        idempotencyKey: const IdempotencyKey('idem-1'),
        baseVersion: const Version(revision: 7),
        localVersion: 3,
        dependsOn: const [OpId('op-0')],
        status: OpStatus.conflicted,
        attempts: 2,
        everSent: true,
        createdAt: t0,
        updatedAt: t0.add(const Duration(seconds: 2)),
        nextAttemptAt: t0.add(const Duration(minutes: 1)),
        leaseOwner: 'runner-1',
        inFlightSince: t0.add(const Duration(seconds: 3)),
        lastError: const ErrorInfo(code: 'http_409', httpStatus: 409, message: 'conflict', ambiguous: true),
        serverVersion: const Version(etag: '"v2"'),
      );

  group('round trips', () {
    late SqliteLocalStore store;

    setUp(() => store = SqliteLocalStore(sqlite3.openInMemory()));
    tearDown(() => store.close());

    test('every StoredRecord field', () async {
      final r = fullRecord();
      await store.transaction((tx) => tx.records.put(r));
      final back = (await store.transaction((tx) => tx.records.get(r.key)))!;
      expect(back.key, r.key);
      expect(back.payload, r.payload);
      expect(back.tombstone, r.tombstone);
      expect(back.invalidated, r.invalidated);
      expect(back.serverVersion, r.serverVersion);
      expect(back.validators!.etag, r.validators!.etag);
      expect(back.validators!.lastModified, r.validators!.lastModified);
      expect(back.fetchedAt, r.fetchedAt);
      expect(back.httpMaxAge, r.httpMaxAge);
      expect(back.lastAccessedAt, r.lastAccessedAt);
      expect(back.accessCount, r.accessCount);
      expect(back.sizeBytes, r.sizeBytes);
      expect(back.priority, r.priority);
      expect(back.pinned, r.pinned);
    });

    test('tombstone with a null payload', () async {
      final r = StoredRecord(key: RecordKey('todo', 'gone'), payload: null, tombstone: true, fetchedAt: t0, lastAccessedAt: t0);
      await store.transaction((tx) => tx.records.put(r));
      final back = (await store.transaction((tx) => tx.records.get(r.key)))!;
      expect(back.payload, isNull);
      expect(back.tombstone, isTrue);
      expect(back.serverVersion, isNull);
      expect(back.httpMaxAge, isNull);
    });

    test('every Operation field', () async {
      final op = fullOp();
      await store.transaction((tx) => tx.ops.append(op));
      final back = (await store.transaction((tx) => tx.ops.get(op.id)))!;
      expect(back.id, op.id);
      expect(back.seq, op.seq);
      expect(back.entity, op.entity);
      expect(back.kind, op.kind);
      expect(back.payload, op.payload);
      expect(back.idempotencyKey, op.idempotencyKey);
      expect(back.baseVersion, op.baseVersion);
      expect(back.localVersion, op.localVersion);
      expect(back.dependsOn, op.dependsOn);
      expect(back.status, op.status);
      expect(back.attempts, op.attempts);
      expect(back.everSent, op.everSent);
      expect(back.createdAt, op.createdAt);
      expect(back.updatedAt, op.updatedAt);
      expect(back.nextAttemptAt, op.nextAttemptAt);
      expect(back.leaseOwner, op.leaseOwner);
      expect(back.inFlightSince, op.inFlightSince);
      expect(back.lastError!.toJson(), op.lastError!.toJson());
      expect(back.serverVersion, op.serverVersion);
    });

    test('meta values of every JSON type', () async {
      final values = <String, Object?>{
        'int': 7,
        'string': 's',
        'bool': true,
        'double': 1.25,
        'list': [1, 'two'],
        'map': {'owner': 'r1', 'expiresAt': 123},
      };
      await store.transaction((tx) async {
        for (final e in values.entries) {
          await tx.meta.put(e.key, e.value);
        }
      });
      await store.transaction((tx) async {
        for (final e in values.entries) {
          expect(await tx.meta.get(e.key), e.value, reason: e.key);
        }
        await tx.meta.put('int', null);
        expect(await tx.meta.get('int'), isNull);
      });
    });

    test('countByStatus and where with a limit', () async {
      await store.transaction((tx) async {
        await tx.ops.append(fullOp());
        await tx.ops.append(fullOp().withId('op-2', seq: 43, status: OpStatus.pending));
        await tx.ops.append(fullOp().withId('op-3', seq: 44, status: OpStatus.pending));
      });
      await store.transaction((tx) async {
        expect(await tx.ops.countByStatus(), {OpStatus.conflicted: 1, OpStatus.pending: 2});
        final limited = await tx.ops.where({OpStatus.pending, OpStatus.conflicted}, limit: 2);
        expect(limited.map((o) => o.id.value), ['op-1', 'op-2']);
        expect(await tx.ops.where({}), isEmpty);
      });
    });
  });

  group('file database', () {
    late Directory dir;

    setUp(() => dir = Directory.systemTemp.createTempSync('ocs_sqlite_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('committed data survives closing and reopening', () async {
      final path = '${dir.path}/offline.db';
      final first = SqliteLocalStore(sqlite3.open(path));
      await first.transaction((tx) async {
        await tx.records.put(fullRecord());
        await tx.ops.append(fullOp());
        await tx.meta.next('seq');
      });
      await first.close();

      final second = SqliteLocalStore(sqlite3.open(path));
      await second.transaction((tx) async {
        expect(await tx.records.get(RecordKey('todo', 'a:b')), isNotNull);
        expect((await tx.ops.get(const OpId('op-1')))!.kind, 'todo.rename');
        expect(await tx.meta.next('seq'), 2);
      });
      await second.close();
    });

    test('a rolled-back transaction is not persisted', () async {
      final path = '${dir.path}/offline.db';
      final first = SqliteLocalStore(sqlite3.open(path));
      await expectLater(
        first.transaction((tx) async {
          await tx.ops.append(fullOp());
          throw StateError('crash');
        }),
        throwsStateError,
      );
      await first.close();

      final second = SqliteLocalStore(sqlite3.open(path));
      expect(await second.transaction((tx) => tx.ops.get(const OpId('op-1'))), isNull);
      await second.close();
    });

    test('refuses a database from a newer schema version', () async {
      final path = '${dir.path}/offline.db';
      final db = sqlite3.open(path)..userVersion = 99;
      db.close();
      expect(() => SqliteLocalStore(sqlite3.open(path)), throwsStateError);
    });
  });

  group('lifecycle', () {
    test('transactions after close fail', () async {
      final store = SqliteLocalStore(sqlite3.openInMemory());
      await store.close();
      await expectLater(store.transaction((tx) => tx.meta.get('x')), throwsStateError);
    });

    test('close waits for queued transactions', () async {
      final store = SqliteLocalStore(sqlite3.openInMemory());
      final pending = store.transaction((tx) async {
        await Future<void>.delayed(const Duration(milliseconds: 5));
        return tx.meta.next('n');
      });
      await store.close();
      expect(await pending, 1);
    });

    test('closeDatabase: false leaves the database open', () async {
      final db = sqlite3.openInMemory();
      final store = SqliteLocalStore(db, closeDatabase: false);
      await store.close();
      expect(db.select('SELECT COUNT(*) AS n FROM ocs_records').first['n'], 0);
      db.close();
    });

    test('using a transaction object after it completed throws', () async {
      final store = SqliteLocalStore(sqlite3.openInMemory());
      final leaked = await store.transaction((tx) async => tx);
      await expectLater(leaked.meta.get('x'), throwsStateError);
      await store.close();
    });
  });
}

extension on Operation {
  Operation withId(String id, {required int seq, required OpStatus status}) => Operation(
        id: OpId(id),
        seq: seq,
        entity: entity,
        kind: kind,
        payload: payload,
        localVersion: localVersion,
        createdAt: createdAt,
        updatedAt: updatedAt,
        status: status,
      );
}
