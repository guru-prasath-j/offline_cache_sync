import 'dart:async';

import 'package:offline_cache_sync/offline_cache_sync.dart';

/// Thrown by [CrashingStore] to simulate the process dying.
final class SimulatedCrash implements Exception {
  const SimulatedCrash(this.where);

  final String where;

  @override
  String toString() => 'SimulatedCrash($where)';
}

/// Wraps a [LocalStore] and kills it at a chosen transaction, simulating a
/// process that dies mid-flight. After the crash every call throws, so the
/// old engine can no longer change anything; open a new engine on [inner]
/// to simulate the restart.
final class CrashingStore implements LocalStore {
  CrashingStore(this.inner);

  final LocalStore inner;

  int _count = 0;
  int? _crashBeforeCommitOn;
  int? _crashAfterCommitOn;
  bool Function(int n)? _predicateBefore;
  bool _dead = false;

  /// Number of transactions started so far.
  int get transactions => _count;

  bool get isDead => _dead;

  /// The [n]-th transaction from now runs its body, then dies before commit.
  void crashBeforeCommit(int n) => _crashBeforeCommitOn = _count + n;

  /// The [n]-th transaction from now commits, then the process dies.
  void crashAfterCommit(int n) => _crashAfterCommitOn = _count + n;

  /// Dies before commit on the first transaction for which [test] is true.
  void crashBeforeCommitWhen(bool Function(int n) test) => _predicateBefore = test;

  /// Dies immediately.
  void kill() => _dead = true;

  @override
  Stream<Set<RecordKey>> get committedChanges => inner.committedChanges;

  @override
  Future<R> transaction<R>(Future<R> Function(StoreTxn tx) body) async {
    if (_dead) throw const SimulatedCrash('store is dead');
    final n = ++_count;
    final before = n == _crashBeforeCommitOn || (_predicateBefore?.call(n) ?? false);
    if (before) {
      try {
        await inner.transaction<R>((tx) async {
          await body(tx);
          throw SimulatedCrash('before commit of txn $n');
        });
      } on SimulatedCrash {
        // rolled back
      }
      _dead = true;
      throw SimulatedCrash('before commit of txn $n');
    }
    final result = await inner.transaction(body);
    if (n == _crashAfterCommitOn) {
      _dead = true;
      throw SimulatedCrash('after commit of txn $n');
    }
    if (_dead) throw const SimulatedCrash('store died during transaction');
    return result;
  }

  @override
  Future<void> close() async {}
}
