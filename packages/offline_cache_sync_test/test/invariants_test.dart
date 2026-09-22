// Model-based randomized tests (doc section 11.2).
//
// Random command sequences (reads, writes, connectivity flips, injected send
// failures, crashes at random transactions, time jumps) run against the real
// engine on the in-memory store. After every step the invariants below are
// checked against the committed "disk" and the fake server; at the end the
// system must converge to the reference model.
//
// Run more sequences with:  INVARIANT_RUNS=2000 dart test -t invariants

import 'dart:io';
import 'dart:math';

import 'package:offline_cache_sync/offline_cache_sync.dart';
import 'package:offline_cache_sync_test/offline_cache_sync_test.dart';
import 'package:test/test.dart';

const _entities = ['a', 'b', 'c'];
const _runsDefault = 60;

Map<String, Object?> _initial(String id) => {'n': 0, 's': 'init-$id'};

/// Reference model: what each entity must look like once every acknowledged
/// write has been applied exactly once, in order.
final class _Model {
  final Map<String, Map<String, Object?>> values = {for (final e in _entities) e: _initial(e)};

  /// Acknowledged enqueues in order: (opId, entity).
  final List<(OpId, String)> enqueued = [];

  void apply(String entity, Mutation<Map<String, Object?>> m) {
    values[entity] = m.apply(values[entity])!;
  }
}

Future<void> _runOne(int seed) async {
  final rng = Random(seed);
  final h = await EngineHarness.create(seed: seed);
  final model = _Model();
  for (final e in _entities) {
    h.server.seed(e, _initial(e));
  }
  var budget = 8; // injected failures + crashes per run: stays below maxAttempts (10)

  Future<void> check(String step) async {
    final ops = h.storedOps;

    // 1. At most one op in flight per entity.
    final inFlight = <String>{};
    for (final op in ops.where((o) => o.status == OpStatus.inFlight)) {
      expect(inFlight.add(op.entity.id), isTrue, reason: '[$seed/$step] two in-flight ops for ${op.entity.id}');
    }

    // 2. No lost writes: every acknowledged op is still stored, or the server applied it.
    final applied = h.server.applied.map((a) => a.$2).toSet();
    final stored = ops.map((o) => o.id).toSet();
    for (final (id, entity) in model.enqueued) {
      expect(stored.contains(id) || applied.contains(id), isTrue,
          reason: '[$seed/$step] op ${id.value} for $entity vanished');
    }

    // 3. Nothing unexpected: no failed / dead-lettered / conflicted ops in this workload.
    for (final op in ops) {
      expect(op.status, isIn([OpStatus.pending, OpStatus.inFlight]),
          reason: '[$seed/$step] ${op.id.value} is ${op.status} (${op.lastError})');
    }

    // 4. Key stability: every attempt of an op carried the same idempotency key.
    final keyOf = <OpId, IdempotencyKey?>{};
    for (final c in h.server.calls) {
      final prev = keyOf.putIfAbsent(c.opId, () => c.idempotencyKey);
      expect(prev, c.idempotencyKey, reason: '[$seed/$step] key changed for ${c.opId.value}');
    }

    // 5. Per-entity order: the server applied each entity's ops in enqueue order,
    //    each exactly once.
    final order = {for (var i = 0; i < model.enqueued.length; i++) model.enqueued[i].$1: i};
    final lastIndex = <String, int>{};
    final seen = <OpId>{};
    for (final (entity, id) in h.server.applied) {
      expect(seen.add(id), isTrue, reason: '[$seed/$step] ${id.value} applied twice');
      final i = order[id]!;
      expect(i, greaterThan(lastIndex[entity] ?? -1), reason: '[$seed/$step] out-of-order apply on $entity');
      lastIndex[entity] = i;
    }
  }

  Future<void> readYourWrites(String step) async {
    for (final e in _entities) {
      final ops = h.storedOps.where((o) => o.entity.id == e);
      // Known limitation (documented): after an ambiguous send the server may
      // already contain the op's effect while the op is still pending, so a
      // refresh can show it twice until the resend is acknowledged.
      if (ops.any((o) => o.lastError?.ambiguous ?? false)) continue;
      final s = await h.items.peek(e);
      if (s.value == null) continue; // never fetched yet
      expect(s.value, model.values[e], reason: '[$seed/$step] read-your-writes broken for $e');
    }
  }

  Future<void> restartIfDead() async {
    if (h.store.isDead) await h.crashAndRestart();
  }

  for (final e in _entities) {
    await h.items.get(e);
  }

  for (var step = 0; step < 40; step++) {
    final roll = rng.nextInt(100);
    final name = 'step $step';
    if (roll < 40) {
      // Write.
      final e = _entities[rng.nextInt(_entities.length)];
      final Mutation<Map<String, Object?>> m =
          rng.nextBool() ? Increment('n', 1 + rng.nextInt(5)) : SetFields({'s': 'v$step'});
      final handle = await h.items.mutate(e, m);
      model.enqueued.add((handle.id, e));
      model.apply(e, m);
      await readYourWrites('$name write');
    } else if (roll < 55) {
      // Read (may fetch / revalidate).
      final e = _entities[rng.nextInt(_entities.length)];
      try {
        await h.items.get(e);
      } on SimulatedCrash {
        // the process died during the read
      }
    } else if (roll < 65) {
      if (h.connectivity.current == Reachability.offline) {
        h.connectivity.goOnline();
      } else {
        h.connectivity.goOffline();
      }
    } else if (roll < 75 && budget > 0 && h.server.pendingSendFailures == 0) {
      budget--;
      final kinds = [SendFailure.lostBeforeServer, SendFailure.lostAfterApply, SendFailure.unavailable];
      h.server.failNextSends(1, kinds[rng.nextInt(kinds.length)]);
    } else if (roll < 85 && budget > 0) {
      // Arm a crash on a random upcoming transaction, then let time pass.
      budget--;
      final target = h.store.transactions + 1 + rng.nextInt(6);
      h.store.crashBeforeCommitWhen((n) => n == target);
      await h.advance(Duration(seconds: 1 + rng.nextInt(600)));
      h.store.crashBeforeCommitWhen((_) => false);
    } else {
      await h.advance(Duration(seconds: 1 + rng.nextInt(600)));
    }
    await h.settle();
    await restartIfDead();
    await check(name);
  }

  // Quiesce: back online, no more faults, let every retry run.
  h.connectivity.goOnline();
  h.server.clearFailures();
  for (var i = 0; i < 60 && h.storedOps.isNotEmpty; i++) {
    await h.advance(const Duration(minutes: 10));
    await restartIfDead();
  }
  await check('final');

  expect(h.storedOps, isEmpty, reason: '[$seed] outbox did not drain: ${h.storedOps}');
  for (final e in _entities) {
    expect(h.server.valueOf(e), model.values[e], reason: '[$seed] server diverged for $e');
    final local = await h.items.get(e, forceRefresh: true);
    expect(local.value, model.values[e], reason: '[$seed] local diverged for $e');
  }
  await h.dispose();
}

void main() {
  final runs = int.tryParse(Platform.environment['INVARIANT_RUNS'] ?? '') ?? _runsDefault;
  test('invariants hold under random faults and crashes ($runs seeds)', () async {
    for (var seed = 1; seed <= runs; seed++) {
      await _runOne(seed);
    }
  }, timeout: const Timeout(Duration(minutes: 5)), tags: ['invariants']);
}
