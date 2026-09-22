import 'dart:async';

import 'package:offline_cache_sync/adapter_api.dart';
import 'package:test/test.dart';

/// Registers the tests every [LocalStore] implementation must pass.
///
/// ```dart
/// void main() => runLocalStoreConformance('sqlite', () => SqliteStore.memory());
/// ```
void runLocalStoreConformance(String name, Future<LocalStore> Function() create) {
  group('LocalStore conformance: $name', () {
    late LocalStore store;
    final t0 = DateTime.utc(2026, 1, 1);

    setUp(() async => store = await create());
    tearDown(() async => store.close());

    StoredRecord record(String id, {bool pinned = false, DateTime? accessed, Object? payload}) => StoredRecord(
          key: RecordKey('item', id),
          payload: payload ?? {'id': id},
          fetchedAt: t0,
          lastAccessedAt: accessed ?? t0,
          pinned: pinned,
        );

    Operation op(String id, int seq, {OpStatus status = OpStatus.pending, String entity = 'a'}) => Operation(
          id: OpId(id),
          seq: seq,
          entity: RecordKey('item', entity),
          kind: r'$set',
          payload: {'n': seq},
          localVersion: seq,
          createdAt: t0,
          updatedAt: t0,
          status: status,
        );

    test('committed writes are visible to later transactions', () async {
      await store.transaction((tx) => tx.records.put(record('a')));
      final r = await store.transaction((tx) => tx.records.get(RecordKey('item', 'a')));
      expect(r, isNotNull);
      expect(r!.payload, {'id': 'a'});
    });

    test('a throwing transaction leaves no trace (atomicity)', () async {
      await expectLater(
        store.transaction((tx) async {
          await tx.records.put(record('a'));
          await tx.ops.append(op('o1', 1));
          await tx.meta.put('x', 1);
          throw StateError('boom');
        }),
        throwsStateError,
      );
      await store.transaction((tx) async {
        expect(await tx.records.get(RecordKey('item', 'a')), isNull);
        expect(await tx.ops.get(const OpId('o1')), isNull);
        expect(await tx.meta.get('x'), isNull);
      });
    });

    test('transactions are serialized', () async {
      var inside = 0;
      var maxInside = 0;
      Future<void> body(StoreTxn tx) async {
        inside++;
        maxInside = inside > maxInside ? inside : maxInside;
        await Future<void>.delayed(const Duration(milliseconds: 2));
        await tx.meta.next('n');
        inside--;
      }

      await Future.wait([for (var i = 0; i < 10; i++) store.transaction(body)]);
      expect(maxInside, 1);
      expect(await store.transaction((tx) => tx.meta.get('n')), 10);
    });

    test('meta.next is monotonic', () async {
      final a = await store.transaction((tx) => tx.meta.next('seq'));
      final b = await store.transaction((tx) => tx.meta.next('seq'));
      expect(b, a + 1);
    });

    test('committedChanges reports touched keys', () async {
      final seen = <Set<RecordKey>>[];
      final sub = store.committedChanges.listen(seen.add);
      await store.transaction((tx) => tx.records.put(record('a')));
      await store.transaction((tx) => tx.ops.append(op('o1', 1, entity: 'b')));
      await Future<void>.delayed(Duration.zero);
      await sub.cancel();
      expect(seen.expand((s) => s), containsAll([RecordKey('item', 'a'), RecordKey('item', 'b')]));
    });

    test('ops: ordering by seq, forEntity, heads, replace, remove', () async {
      await store.transaction((tx) async {
        await tx.ops.append(op('o3', 3, entity: 'a'));
        await tx.ops.append(op('o1', 1, entity: 'a'));
        await tx.ops.append(op('o2', 2, entity: 'b'));
        await tx.ops.append(op('o4', 4, entity: 'b', status: OpStatus.cancelled));
      });
      await store.transaction((tx) async {
        final a = await tx.ops.forEntity(RecordKey('item', 'a'));
        expect(a.map((o) => o.id.value), ['o1', 'o3']);
        final heads = await tx.ops.heads(limit: 10);
        expect(heads.map((o) => o.id.value), ['o1', 'o2']);
        final b = await tx.ops.forEntity(RecordKey('item', 'b'));
        expect(b.map((o) => o.id.value), ['o2'], reason: 'terminal ops are excluded');
        await tx.ops.replace(a.first.copyWith(status: OpStatus.inFlight, updatedAt: t0));
        expect((await tx.ops.get(const OpId('o1')))!.status, OpStatus.inFlight);
        await tx.ops.remove(const OpId('o1'));
        expect(await tx.ops.get(const OpId('o1')), isNull);
        expect((await tx.ops.where({OpStatus.pending})).map((o) => o.id.value), ['o2', 'o3']);
      });
    });

    test('replace of a missing op throws', () async {
      await expectLater(store.transaction((tx) => tx.ops.replace(op('nope', 1))), throwsStateError);
    });

    test('append of a duplicate op id throws', () async {
      await store.transaction((tx) => tx.ops.append(op('o1', 1)));
      await expectLater(store.transaction((tx) => tx.ops.append(op('o1', 2))), throwsStateError);
    });

    test('eviction candidates exclude pinned and entities with blocking ops', () async {
      await store.transaction((tx) async {
        await tx.records.put(record('a', accessed: t0.add(const Duration(minutes: 3))));
        await tx.records.put(record('b', accessed: t0.add(const Duration(minutes: 1))));
        await tx.records.put(record('c', pinned: true));
        await tx.records.put(record('d', accessed: t0.add(const Duration(minutes: 2))));
        await tx.ops.append(op('o1', 1, entity: 'd', status: OpStatus.deadLetter));
      });
      final c = await store.transaction((tx) => tx.records.evictionCandidates(limit: 10));
      expect(c.map((r) => r.key.id), ['b', 'a']);
    });

    test('markAccessed updates stats without changing values', () async {
      await store.transaction((tx) => tx.records.put(record('a')));
      final at = t0.add(const Duration(hours: 1));
      await store.transaction((tx) => tx.records.markAccessed({RecordKey('item', 'a'): 3}, at));
      final r = await store.transaction((tx) => tx.records.get(RecordKey('item', 'a')));
      expect(r!.lastAccessedAt, at);
      expect(r.accessCount, 3);
      expect(r.payload, {'id': 'a'});
    });

    test('stats count records and bytes', () async {
      await store.transaction((tx) async {
        await tx.records.put(record('a'));
        await tx.records.put(record('b'));
      });
      final s = await store.transaction((tx) => tx.records.stats());
      expect(s.records, 2);
      expect(s.bytes, greaterThan(0));
    });
  });
}
