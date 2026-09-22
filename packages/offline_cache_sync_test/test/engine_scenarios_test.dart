import 'dart:async';

import 'package:offline_cache_sync/offline_cache_sync.dart';
import 'package:offline_cache_sync_test/offline_cache_sync_test.dart';
import 'package:test/test.dart';

/// Lets microtasks and zero-delay futures run without waiting for gated work.
Future<void> pump([int times = 20]) async {
  for (var i = 0; i < times; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

Future<void> advanceUntil(EngineHarness h, bool Function() done, {int max = 50}) async {
  for (var i = 0; i < max && !done(); i++) {
    await h.advance(const Duration(minutes: 10));
  }
}

void main() {
  late EngineHarness h;

  tearDown(() async => h.dispose());

  group('reads', () {
    test('miss -> network, then fresh hit from cache', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});

      final s1 = await h.items.get('1');
      expect(s1.value, {'name': 'a'});
      expect(s1.source, DataSource.network);
      expect(h.server.fetchCount, 1);

      await h.advance(const Duration(seconds: 10));
      final s2 = await h.items.get('1');
      expect(s2.value, {'name': 'a'});
      expect(s2.source, DataSource.cache);
      expect(s2.freshness, Freshness.fresh);
      expect(h.server.fetchCount, 1);
    });

    test('stale -> served immediately + one background revalidation (SWR)', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      await h.items.get('1');
      h.server.externalEdit('1', {'name': 'b'});

      await h.advance(const Duration(minutes: 2));
      final s = await h.items.get('1');
      expect(s.freshness, Freshness.stale);
      expect(s.value, {'name': 'a'}, reason: 'stale value is returned without waiting');
      expect(s.isRevalidating, isTrue);

      await h.settle();
      expect(h.server.fetchCount, 2);
      expect((await h.items.peek('1')).value, {'name': 'b'});
    });

    test('revalidation with an unchanged ETag is a 304 that renews freshness', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      await h.items.get('1');
      await h.advance(const Duration(minutes: 2));
      await h.items.get('1');
      await h.settle();

      expect(h.events.whereType<Revalidated>().last.notModified, isTrue);
      final peek = await h.items.peek('1');
      expect(peek.freshness, Freshness.fresh);
      expect(peek.value, {'name': 'a'});
    });

    test('stale-if-error: expired value served when the fetch fails', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      await h.items.get('1');
      await h.advance(const Duration(hours: 2)); // past expireAfter (1h), within staleIfError (1d)
      h.server.failNextFetches(1);

      final s = await h.items.get('1');
      expect(s.value, {'name': 'a'});
      expect(s.servedOnError, isTrue);
      expect(s.error, isNotNull);
    });

    test('stale-if-offline: expired value served with no request', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      await h.items.get('1');
      await h.advance(const Duration(hours: 5));
      h.connectivity.goOffline();

      final s = await h.items.get('1');
      expect(s.value, {'name': 'a'});
      expect(s.servedOffline, isTrue);
      expect(h.server.fetchCount, 1);
    });

    test('offline miss reports OfflineMiss', () async {
      h = await EngineHarness.create();
      h.connectivity.goOffline();
      final s = await h.items.get('404');
      expect(s.value, isNull);
      expect(s.error, isA<OfflineMiss>());
    });

    test('concurrent reads of one key share a single request', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      final gate = h.server.fetchGate = Completer<void>();
      final futures = [h.items.get('1'), h.items.get('1'), h.items.get('1')];
      await pump();
      gate.complete();
      final results = await Future.wait(futures);
      expect(h.server.fetchCount, 1);
      expect(results.map((s) => s.value), everyElement({'name': 'a'}));
    });

    test('404 stores a tombstone', () async {
      h = await EngineHarness.create();
      final s = await h.items.get('missing');
      expect(s.value, isNull);
      expect(h.disk.debugRecords[RecordKey('item', 'missing')]!.tombstone, isTrue);
    });
  });

  group('writes', () {
    test('offline write: optimistic immediately, sent on reconnect', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      await h.items.get('1');
      h.connectivity.goOffline();

      await h.items.mutate('1', const SetFields({'name': 'b'}));
      final local = await h.items.peek('1');
      expect(local.value, {'name': 'b'});
      expect(local.pendingWrites, 1);
      await h.settle();
      expect(h.server.calls, isEmpty);

      h.connectivity.goOnline();
      await h.settle();
      expect(h.server.valueOf('1'), {'name': 'b'});
      expect(h.storedOps, isEmpty);
      final after = await h.items.peek('1');
      expect(after.value, {'name': 'b'});
      expect(after.pendingWrites, 0);
    });

    test('create while offline, then delete', () async {
      h = await EngineHarness.create();
      h.connectivity.goOffline();
      await h.items.mutate('n', const SetFields({'title': 'new'}));
      expect((await h.items.peek('n')).value, {'title': 'new'});
      h.connectivity.goOnline();
      await h.settle();
      expect(h.server.valueOf('n'), {'title': 'new'});

      await h.items.mutate('n', const Delete());
      await h.settle();
      expect(h.server.valueOf('n'), isNull);
      expect((await h.items.peek('n')).value, isNull);
    });

    test('retry with backoff reuses the same idempotency key', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'n': 0});
      await h.items.get('1');
      h.server.failNextSends(2, SendFailure.unavailable);

      await h.items.mutate('1', const Increment('n', 1));
      await h.settle();
      expect(h.server.calls, hasLength(1));
      expect(h.storedOps.single.status, OpStatus.pending);
      expect(h.storedOps.single.nextAttemptAt, isNotNull);

      await advanceUntil(h, () => h.storedOps.isEmpty);
      expect(h.server.calls, hasLength(3));
      expect(h.server.calls.map((c) => c.idempotencyKey).toSet(), hasLength(1));
      expect(h.server.valueOf('1'), {'n': 1});
    });

    test('response lost after the server applied it: no duplicate (idempotency)', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'n': 0});
      await h.items.get('1');
      h.server.failNextSends(1, SendFailure.lostAfterApply);

      await h.items.mutate('1', const Increment('n', 1));
      await advanceUntil(h, () => h.storedOps.isEmpty);

      expect(h.server.calls, hasLength(2));
      expect(h.server.applied, hasLength(1));
      expect(h.server.valueOf('1'), {'n': 1});
      expect((await h.items.peek('1')).value, {'n': 1});
    });

    test('same failure WITHOUT idempotency keys duplicates (why keys matter)', () async {
      // Versions off so the resend is not caught as a conflict: we want to
      // show the raw duplicate an at-least-once resend causes without keys.
      h = await EngineHarness.create(useIdempotencyKeys: false, server: FakeServer(checkVersions: false));
      h.server.seed('1', {'n': 0});
      await h.items.get('1');
      h.server.failNextSends(1, SendFailure.lostAfterApply);

      await h.items.mutate('1', const Increment('n', 1));
      await advanceUntil(h, () => h.storedOps.isEmpty);
      expect(h.server.valueOf('1'), {'n': 2});
    });

    test('at-most-once without keys: an ambiguous failure is parked as failed, not resent', () async {
      h = await EngineHarness.create(useIdempotencyKeys: false, delivery: Delivery.atMostOnce);
      h.server.seed('1', {'n': 0});
      await h.items.get('1');
      h.server.failNextSends(1, SendFailure.lostAfterApply);

      final op = await h.items.mutate('1', const Increment('n', 1));
      await h.settle();
      expect(await op.done, OpOutcome.failed);
      expect(h.storedOps.single.lastError!.ambiguous, isTrue);
      expect(h.server.calls, hasLength(1));
      final s = await h.items.peek('1');
      expect(s.hasFailedWrites, isTrue);
    });

    test('permanent rejection: op failed, fold drops it (visible rollback)', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      await h.items.get('1');
      h.server.failNextSends(1, SendFailure.reject);

      final op = await h.items.mutate('1', const SetFields({'name': 'bad'}));
      expect(await op.done, OpOutcome.failed);
      final s = await h.items.peek('1');
      expect(s.value, {'name': 'a'});
      expect(s.hasFailedWrites, isTrue);

      expect(await op.discard(), isTrue);
      expect(h.storedOps, isEmpty);
    });

    test('per-entity order: B is not sent before A completes', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'x'});
      await h.items.get('1');
      final gate = h.server.sendGate = Completer<void>();

      await h.items.mutate('1', const SetFields({'name': 'A'}));
      await h.items.mutate('1', const SetFields({'name': 'B'}));
      await pump();
      expect(h.server.calls.map((c) => c.payload), [
        {'name': 'A'},
      ]);
      expect((await h.items.peek('1')).value, {'name': 'B'});

      h.server.sendGate = null;
      gate.complete();
      await h.settle();
      expect(h.server.calls.map((c) => c.payload), [
        {'name': 'A'},
        {'name': 'B'},
      ]);
      expect(h.server.valueOf('1'), {'name': 'B'});
      expect(h.events.whereType<OpConflicted>(), isEmpty,
          reason: 'B was rebased on the version A produced, so If-Match does not self-conflict');
    });

    test('different entities are sent in parallel', () async {
      h = await EngineHarness.create();
      final gate = h.server.sendGate = Completer<void>();
      await h.items.mutate('a', const SetFields({'v': 1}));
      await h.items.mutate('b', const SetFields({'v': 1}));
      await pump();
      expect(h.server.calls.map((c) => c.entityId).toSet(), {'a', 'b'});
      h.server.sendGate = null;
      gate.complete();
      await h.settle();
    });

    test('a slow GET that started before a write was acked cannot regress the base', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      await h.items.get('1');
      final gate = h.server.fetchGate = Completer<void>();
      final slow = h.items.get('1', forceRefresh: true); // response computed now: {'name': 'a'}
      await pump();

      h.server.fetchGate = null;
      await h.items.mutate('1', const SetFields({'name': 'b'}));
      await pump(50);
      expect(h.server.valueOf('1'), {'name': 'b'});

      gate.complete();
      await slow;
      await h.settle();
      expect(h.events.whereType<StaleResponseDiscarded>(), isNotEmpty);
      expect((await h.items.peek('1')).value, {'name': 'b'});
    });

    test('dependsOn: child waits for parent', () async {
      h = await EngineHarness.create();
      h.connectivity.goOffline();
      final parent = await h.items.mutate('p', const SetFields({'kind': 'parent'}));
      await h.items.mutate('c', const SetFields({'parent': 'p'}), dependsOn: [parent.id]);
      h.connectivity.goOnline();
      await h.settle();
      expect(h.server.calls.map((c) => c.entityId), ['p', 'c']);
    });
  });

  group('refresh vs pending writes (I1)', () {
    test('a background refresh never overwrites a pending edit', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'server', 'age': 1});
      await h.items.get('1');
      h.connectivity.goOffline();
      await h.items.mutate('1', const SetFields({'name': 'local'}));

      h.server.externalEdit('1', {'age': 2});
      final gate = h.server.sendGate = Completer<void>(); // the write gets stuck in flight...
      h.connectivity.set(Reachability.unknown); // ...while reads hit the network
      await pump();
      h.clock.advance(const Duration(minutes: 2)); // no settle(): the send is gated
      final s = await h.items.get('1'); // stale -> SWR
      await pump(50);

      final after = await h.items.peek('1');
      expect(after.value, {'name': 'local', 'age': 2}, reason: 'new server base + pending edit on top');
      expect(after.hasPendingWrites, isTrue);
      expect(s.value!['name'], 'local');
      h.server.sendGate = null;
      gate.complete();
    });
  });

  group('conflicts', () {
    Future<EngineHarness> conflicting(ConflictResolver<Map<String, Object?>> resolver) async {
      final h = await EngineHarness.create(conflicts: resolver);
      h.server.seed('1', {'name': 'a', 'age': 1});
      await h.items.get('1');
      h.server.externalEdit('1', {'age': 2}); // someone else, revision 2
      return h;
    }

    test('fieldMerge: three-way merge, resent as a replace with a NEW key', () async {
      h = await conflicting(ConflictStrategies.fieldMerge());
      await h.items.mutate('1', const SetFields({'name': 'b'}));
      await h.settle();

      expect(h.server.valueOf('1'), {'name': 'b', 'age': 2});
      expect(h.server.calls, hasLength(2));
      expect(h.server.calls.last.kind, r'$replace');
      expect(h.server.calls.first.idempotencyKey, isNot(h.server.calls.last.idempotencyKey));
      expect(h.storedOps, isEmpty);
    });

    test('serverWins: op dropped, server value shown', () async {
      h = await conflicting(ConflictStrategies.serverWins());
      final op = await h.items.mutate('1', const SetFields({'name': 'b'}));
      await h.settle();
      expect(await op.done, OpOutcome.droppedByConflict);
      expect((await h.items.peek('1')).value, {'name': 'a', 'age': 2});
    });

    test('clientWins: rebased and resent', () async {
      h = await conflicting(ConflictStrategies.clientWins());
      await h.items.mutate('1', const SetFields({'name': 'b'}));
      await h.settle();
      expect(h.server.valueOf('1'), {'name': 'b', 'age': 2});
    });

    test('park: op kept as conflicted and still shown', () async {
      h = await conflicting(ConflictStrategies.park());
      final op = await h.items.mutate('1', const SetFields({'name': 'b'}));
      await h.settle();
      expect(await op.done, OpOutcome.conflicted);
      final s = await h.items.peek('1');
      expect(s.hasConflicts, isTrue);
      expect(s.value, {'name': 'b', 'age': 2});
    });
  });

  group('crash recovery', () {
    test('C1: crash during enqueue leaves nothing behind', () async {
      h = await EngineHarness.create();
      h.store.crashBeforeCommit(1);
      await expectLater(h.items.mutate('1', const SetFields({'a': 1})), throwsA(isA<SimulatedCrash>()));
      await h.crashAndRestart();
      expect(h.storedOps, isEmpty);
      expect(h.server.calls, isEmpty);
    });

    test('C3: crash while in flight (request never arrived) -> resent after restart', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'n': 0});
      await h.items.get('1');
      final gate = h.server.sendGate = Completer<void>();
      await h.items.mutate('1', const Increment('n', 1));
      await pump();
      expect(h.storedOps.single.status, OpStatus.inFlight);

      h.server.sendGate = null;
      await h.crashAndRestart();
      expect(h.events.whereType<OpRecovered>().single.resent, isTrue);
      expect(h.server.valueOf('1'), {'n': 1});
      expect(h.storedOps, isEmpty);

      gate.complete(); // the old request finally arrives: same key -> replay, no effect
      await pump(50);
      expect(h.server.valueOf('1'), {'n': 1});
      expect(h.server.applied, hasLength(1));
    });

    test('C4: server applied it, crash before the result was stored -> no duplicate', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'n': 0});
      await h.items.get('1');
      h.store.crashBeforeCommitWhen((_) => h.server.applied.isNotEmpty);

      await h.items.mutate('1', const Increment('n', 1));
      await h.settle();
      expect(h.store.isDead, isTrue);
      expect(h.storedOps.single.status, OpStatus.inFlight);

      await h.crashAndRestart();
      expect(h.server.valueOf('1'), {'n': 1});
      expect(h.server.calls, hasLength(2));
      expect(h.storedOps, isEmpty);
      expect((await h.items.peek('1')).value, {'n': 1});
    });

    test('ops of an unknown kind (after an app update) are dead-lettered, never dropped', () async {
      h = await EngineHarness.create();
      final t = h.clock.now();
      await h.disk.transaction((tx) => tx.ops.append(Operation(
            id: const OpId('legacy'),
            seq: 99,
            entity: RecordKey('item', '1'),
            kind: 'removed.kind',
            payload: null,
            localVersion: 1,
            createdAt: t,
            updatedAt: t,
          )));
      await h.restart();
      final op = h.storedOps.single;
      expect(op.status, OpStatus.deadLetter);
      expect(op.lastError!.code, 'unknown_kind');
    });
  });

  group('lease', () {
    test('a secondary engine does not drain while the primary holds the lease', () async {
      h = await EngineHarness.create();
      expect(h.engine.holdsLease, isTrue);
      final background = await OfflineEngine.open(
        store: h.disk,
        resources: [h.itemResource],
        clock: h.clock,
        scheduler: h.clock,
        leaseMode: LeaseMode.secondary,
        runnerId: 'bg',
      );
      expect(background.holdsLease, isFalse);
      await background.close();
    });
  });

  group('retention', () {
    test('LRU never evicts dirty or watched records', () async {
      h = await EngineHarness.create(retention: const RetentionPolicy(maxEntries: 5));
      for (var i = 0; i < 8; i++) {
        h.server.seed('$i', {'i': i});
      }
      await h.items.get('0');
      h.server.failNextSends(1, SendFailure.reject);
      await h.items.mutate('0', const SetFields({'x': 1})); // stays `failed` => dirty
      await h.settle();

      final sub = h.items.watch('1').listen((_) {});
      await h.settle();
      for (var i = 2; i < 8; i++) {
        h.clock.advance(const Duration(seconds: 1));
        await h.items.get('$i');
        await h.settle();
      }
      await h.engine.compact();

      final keys = h.disk.debugRecords.keys.map((k) => k.id).toSet();
      expect(keys, containsAll(['0', '1']));
      expect(keys.length, lessThanOrEqualTo(5));
      expect(h.events.whereType<Evicted>(), isNotEmpty);
      await sub.cancel();
    });

    test('pinned records survive', () async {
      h = await EngineHarness.create(retention: const RetentionPolicy(maxEntries: 2));
      for (var i = 0; i < 5; i++) {
        h.server.seed('$i', {'i': i});
      }
      await h.items.get('0');
      await h.items.pin('0');
      for (var i = 1; i < 5; i++) {
        h.clock.advance(const Duration(seconds: 1));
        await h.items.get('$i');
      }
      await h.settle();
      await h.engine.compact();
      expect(h.disk.debugRecords.keys.map((k) => k.id), contains('0'));
    });
  });

  group('watch', () {
    test('emits optimistic value, then the acknowledged one', () async {
      h = await EngineHarness.create();
      h.server.seed('1', {'name': 'a'});
      final seen = <Snapshot<Map<String, Object?>>>[];
      final sub = h.items.watch('1').listen(seen.add);
      await h.settle();
      h.connectivity.goOffline();
      await h.items.mutate('1', const SetFields({'name': 'b'}));
      await h.settle();
      h.connectivity.goOnline();
      await h.settle();
      await pump();
      await sub.cancel();

      expect(seen.first.value, {'name': 'a'});
      expect(seen.any((s) => s.value?['name'] == 'b' && s.hasPendingWrites), isTrue);
      expect(seen.last.value, {'name': 'b'});
      expect(seen.last.hasPendingWrites, isFalse);
    });
  });
}
