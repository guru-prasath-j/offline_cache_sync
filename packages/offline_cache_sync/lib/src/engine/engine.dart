import 'dart:async';

import '../model/keys.dart';
import '../model/operation.dart';
import '../model/records.dart';
import '../policy/freshness.dart';
import '../policy/retention.dart';
import '../policy/retry.dart';
import '../ports/remote.dart';
import '../ports/runtime.dart';
import '../ports/store.dart';
import '../resource/conflicts.dart';
import '../resource/mutation.dart';
import '../resource/resource.dart';
import 'events.dart';
import 'snapshot.dart';

/// Who drains the outbox when several engines share one store (e.g. the UI
/// isolate and a background isolate started by workmanager).
enum LeaseMode {
  /// Takes the lease at open and on every drain. Use for the app's main engine.
  primary,

  /// Drains only when no live lease is held by someone else. Use for
  /// background isolates.
  secondary,
}

/// How an operation ended, as seen by this engine instance.
enum OpOutcome {
  /// Acknowledged by the server.
  succeeded,

  /// Dropped by a `TakeServer` conflict resolution.
  droppedByConflict,

  /// Parked in `conflicted` for the app to decide.
  conflicted,

  /// Permanently rejected (`failed`), including ambiguous at-most-once sends.
  failed,

  /// Retries exhausted or unprocessable.
  deadLetter,

  /// Discarded by the app.
  cancelled,

  /// Not found (e.g. finished before this engine instance started).
  unknown,
}

/// The offline-first engine: one per account/store.
///
/// Reads return `fold(serverBase, pendingOps)`; writes are enqueued
/// atomically and drained by a single lease holder; refreshes never touch
/// pending ops; eviction never touches entities with pending ops.
final class OfflineEngine {
  OfflineEngine._({
    required this.store,
    required Map<String, Resource<Object?, Object?>> resources,
    required this.clock,
    required this.scheduler,
    required this.connectivity,
    required this.idempotency,
    required this.retry,
    required this.retention,
    required this.eventSink,
    required this.runnerId,
    required this.leaseMode,
    required this.leaseTtl,
    required this.maxConcurrentSends,
    required this.accessFlushInterval,
    required UuidV4 uuid,
  })  : _resources = resources,
        _uuid = uuid;

  /// Opens an engine on [store], recovering any operation that was in flight
  /// when the previous process died.
  static Future<OfflineEngine> open({
    required LocalStore store,
    required List<Resource<Object?, Object?>> resources,
    Clock clock = const SystemClock(),
    Scheduler scheduler = const TimerScheduler(),
    ConnectivityProvider connectivity = const AssumeOnline(),
    IdempotencyProvider? idempotency,
    RetryPolicy? retry,
    RetentionPolicy retention = const RetentionPolicy.unbounded(),
    EventSink? eventSink,
    String? runnerId,
    LeaseMode leaseMode = LeaseMode.primary,
    Duration leaseTtl = const Duration(seconds: 30),
    int maxConcurrentSends = 4,
    Duration accessFlushInterval = const Duration(seconds: 5),
    bool autoDrain = true,
    UuidV4? uuid,
  }) async {
    final byType = <String, Resource<Object?, Object?>>{};
    for (final r in resources) {
      if (byType.containsKey(r.type)) throw ArgumentError('Duplicate resource type "${r.type}"');
      byType[r.type] = r;
    }
    final u = uuid ?? UuidV4();
    final engine = OfflineEngine._(
      store: store,
      resources: byType,
      clock: clock,
      scheduler: scheduler,
      connectivity: connectivity,
      idempotency: idempotency ?? UuidIdempotency(u),
      retry: retry ?? ExponentialBackoff(),
      retention: retention,
      eventSink: eventSink,
      runnerId: runnerId ?? 'runner-${u.next()}',
      leaseMode: leaseMode,
      leaseTtl: leaseTtl,
      maxConcurrentSends: maxConcurrentSends,
      accessFlushInterval: accessFlushInterval,
      uuid: u,
    );
    await engine._recover();
    engine._connectivitySub = connectivity.changes.listen(engine._onReachability);
    if (autoDrain) engine._kick();
    return engine;
  }

  final LocalStore store;
  final Clock clock;
  final Scheduler scheduler;
  final ConnectivityProvider connectivity;
  final IdempotencyProvider idempotency;
  final RetryPolicy retry;
  final RetentionPolicy retention;
  final EventSink? eventSink;
  final String runnerId;
  final LeaseMode leaseMode;
  final Duration leaseTtl;
  final int maxConcurrentSends;
  final Duration accessFlushInterval;

  final Map<String, Resource<Object?, Object?>> _resources;
  final UuidV4 _uuid;

  final SyncMetrics metrics = SyncMetrics();
  final StreamController<SyncEvent> _events = StreamController<SyncEvent>.broadcast();

  final Map<RecordKey, Future<FetchResult?>> _inflightFetches = {};
  final Set<RecordKey> _activeSends = {};
  final Map<RecordKey, int> _watchers = {};
  final Map<RecordKey, int> _accessBuffer = {};
  final Map<OpId, OpOutcome> _outcomes = {};
  final Map<OpId, List<Completer<OpOutcome>>> _waiters = {};
  final Set<Future<void>> _tracked = {};

  /// Fetch-epoch guard (section 7.5): a fetch that started before the latest
  /// acknowledged write of its key is discarded.
  int _epoch = 0;
  final Map<RecordKey, int> _ackEpoch = {};

  StreamSubscription<Reachability>? _connectivitySub;
  Cancelable? _retryTimer;
  DateTime? _retryTimerAt;
  Cancelable? _accessTimer;
  final Set<Future<void>> _sendsInFlight = {};
  bool _pumping = false;
  bool _pumpAgain = false;
  bool _compactScheduled = false;
  bool _leaseHeld = false;
  bool _closed = false;
  Object? _lastDrainError;

  /// Every engine event (also forwarded to [eventSink]).
  Stream<SyncEvent> get events => _events.stream;

  /// True while this engine holds the drain lease.
  bool get holdsLease => _leaseHeld;

  /// The last error that stopped a drain (e.g. the store failed), if any.
  Object? get lastDrainError => _lastDrainError;

  /// Typed access to one resource.
  Repository<K, T> repository<K, T>(Resource<K, T> resource) {
    final registered = _resources[resource.type];
    if (!identical(registered, resource)) {
      throw ArgumentError('Resource "${resource.type}" is not registered with this engine');
    }
    return Repository<K, T>._(this, resource);
  }

  // ===========================================================================
  // Reads
  // ===========================================================================

  Future<Snapshot<T>> _get<K, T>(Resource<K, T> r, K id, {bool forceRefresh = false}) async {
    _ensureOpen();
    final key = r.keyOf(id);
    _touch(key);
    final local = await _readLocal(key);
    final now = clock.now();
    final record = local.record;
    var freshness = r.freshness.classify(record, now);
    if (forceRefresh && freshness != Freshness.missing) freshness = Freshness.expired;

    switch (freshness) {
      case Freshness.fresh:
        _emit(CacheHit(now, key, freshness));
        return _snapshot(r, key, local, freshness: freshness, source: DataSource.cache);
      case Freshness.stale:
        _emit(CacheHit(now, key, freshness));
        final started = _revalidateInBackground(r, key);
        return _snapshot(r, key, local, freshness: freshness, source: DataSource.cache, isRevalidating: started);
      case Freshness.expired:
      case Freshness.missing:
        if (connectivity.current == Reachability.offline) {
          if (record != null && r.freshness.usableOffline(record, now)) {
            _emit(ServedStale(now, key, StaleReason.offline));
            return _snapshot(r, key, local, freshness: freshness, source: DataSource.cache, servedOffline: true);
          }
          _emit(CacheMiss(now, key));
          return _snapshot(r, key, local,
              freshness: freshness, source: DataSource.none, error: const OfflineMiss());
        }
        _emit(freshness == Freshness.missing ? CacheMiss(now, key) : CacheHit(now, key, freshness));
        try {
          await _fetch(r, key);
        } on Object catch (e) {
          final after = clock.now();
          if (record != null && r.freshness.usableOnError(record, after)) {
            _emit(ServedStale(after, key, StaleReason.error));
            return _snapshot(r, key, local,
                freshness: freshness, source: DataSource.cache, servedOnError: true, error: e);
          }
          return _snapshot(r, key, local, freshness: freshness, source: DataSource.none, error: e);
        }
        final fresh = await _readLocal(key);
        return _snapshot(r, key, fresh,
            freshness: r.freshness.classify(fresh.record, clock.now()), source: DataSource.network);
    }
  }

  /// Local-only snapshot (no network). Used by `watch` re-emissions.
  Future<Snapshot<T>> _peek<K, T>(Resource<K, T> r, RecordKey key) async {
    final local = await _readLocal(key);
    return _snapshot(r, key, local,
        freshness: r.freshness.classify(local.record, clock.now()), source: DataSource.cache);
  }

  Stream<Snapshot<T>> _watch<K, T>(Resource<K, T> r, K id) {
    final key = r.keyOf(id);
    late final StreamController<Snapshot<T>> controller;
    StreamSubscription<Set<RecordKey>>? sub;
    Snapshot<T>? last;
    var active = true;

    void deliver(Snapshot<T> s) {
      if (!active || controller.isClosed) return;
      if (s.sameAs(last, (v) => r.codec.encode(v))) return;
      last = s;
      controller.add(s);
    }

    controller = StreamController<Snapshot<T>>(
      onListen: () {
        _watchers[key] = (_watchers[key] ?? 0) + 1;
        sub = store.committedChanges.where((keys) => keys.contains(key)).listen((_) {
          if (_closed) return;
          _track(_peek(r, key).then(deliver, onError: (Object e, StackTrace st) {
            if (active && !controller.isClosed) controller.addError(e, st);
          }));
        });
        _track(_get(r, id).then(deliver, onError: (Object e, StackTrace st) {
          if (active && !controller.isClosed) controller.addError(e, st);
        }));
      },
      onCancel: () async {
        active = false;
        final n = (_watchers[key] ?? 1) - 1;
        if (n <= 0) {
          _watchers.remove(key);
        } else {
          _watchers[key] = n;
        }
        await sub?.cancel();
      },
    );
    return controller.stream;
  }

  bool _revalidateInBackground(Resource<Object?, Object?> r, RecordKey key) {
    if (connectivity.current == Reachability.offline) return false;
    _track(_fetch(r, key).then((_) {}, onError: (Object _) {}));
    return true;
  }

  /// Fetches [key] and stores the result. Concurrent calls for the same key
  /// share one request (read coalescing). Throws transport errors.
  Future<FetchResult?> _fetch(Resource<Object?, Object?> r, RecordKey key) {
    final existing = _inflightFetches[key];
    if (existing != null) return existing;
    final f = _doFetch(r, key);
    _inflightFetches[key] = f;
    _track(f.then((_) {}, onError: (Object _) {}).whenComplete(() {
      _inflightFetches.remove(key);
    }));
    return f;
  }

  Future<FetchResult?> _doFetch(Resource<Object?, Object?> r, RecordKey key) async {
    final startEpoch = _epoch;
    final cached = await store.transaction((tx) => tx.records.get(key));
    final started = clock.now();
    final FetchResult result;
    try {
      result = await r.fetchByKey(key, FetchContext(key: key, validators: cached?.validators));
    } on Object catch (e) {
      _emit(FetchFailed(clock.now(), key, e.runtimeType.toString()));
      rethrow;
    }
    final stored = await store.transaction((tx) async {
      final now = clock.now();
      final cur = await tx.records.get(key);
      if ((_ackEpoch[key] ?? 0) > startEpoch) return false; // a write was acked meanwhile
      switch (result) {
        case Fetched(:final value, :final version, :final validators, :final maxAge):
          final curVersion = cur?.serverVersion;
          if (curVersion != null && curVersion.isNewerThan(version)) return false;
          await tx.records.put(StoredRecord(
            key: key,
            payload: value,
            fetchedAt: now,
            lastAccessedAt: cur?.lastAccessedAt ?? now,
            accessCount: cur?.accessCount ?? 0,
            serverVersion: version,
            validators: validators,
            httpMaxAge: maxAge,
            priority: cur?.priority ?? 0,
            pinned: cur?.pinned ?? false,
          ));
        case NotModified(:final validators, :final maxAge):
          if (cur == null) return false;
          await tx.records.put(cur.copyWith(
            fetchedAt: now,
            invalidated: false,
            validators: validators ?? cur.validators,
            httpMaxAge: maxAge ?? cur.httpMaxAge,
          ));
        case Gone():
          await tx.records.put(StoredRecord(
            key: key,
            payload: null,
            tombstone: true,
            fetchedAt: now,
            lastAccessedAt: cur?.lastAccessedAt ?? now,
            accessCount: cur?.accessCount ?? 0,
            priority: cur?.priority ?? 0,
            pinned: cur?.pinned ?? false,
          ));
      }
      return true;
    });
    final now = clock.now();
    if (stored) {
      _emit(Revalidated(now, key, notModified: result is NotModified, latency: now.difference(started)));
      _scheduleCompact();
    } else {
      _emit(StaleResponseDiscarded(now, key));
    }
    return result;
  }

  Future<_Local> _readLocal(RecordKey key) => store.transaction((tx) async {
        final record = await tx.records.get(key);
        final ops = await tx.ops.forEntity(key);
        return _Local(record, ops);
      });

  Snapshot<T> _snapshot<K, T>(
    Resource<K, T> r,
    RecordKey key,
    _Local local, {
    required Freshness freshness,
    required DataSource source,
    bool servedOffline = false,
    bool servedOnError = false,
    bool isRevalidating = false,
    Object? error,
  }) {
    final record = local.record;
    T? value;
    Object? err = error;
    try {
      value = (record == null || record.tombstone) ? null : r.decode(record.payload);
    } on Object catch (e) {
      value = null; // corrupt payload: quarantine as a miss
      err ??= e;
    }
    var pending = 0;
    var conflicts = false;
    var failed = false;
    for (final op in local.ops) {
      if (op.status.isFolded) {
        try {
          value = r.decodeMutation(op.kind, op.payload).apply(value);
          pending++;
        } on Object catch (e) {
          err ??= e; // undecodable op: skipped here, dead-lettered by the runner
        }
      }
      if (op.status == OpStatus.conflicted) conflicts = true;
      if (op.status == OpStatus.failed || op.status == OpStatus.deadLetter) failed = true;
    }
    return Snapshot<T>(
      key: key,
      value: value,
      freshness: freshness,
      source: source,
      pendingWrites: pending,
      hasConflicts: conflicts,
      hasFailedWrites: failed,
      servedOffline: servedOffline,
      servedOnError: servedOnError,
      isRevalidating: isRevalidating,
      error: err,
      fetchedAt: record?.fetchedAt,
    );
  }

  // ===========================================================================
  // Writes
  // ===========================================================================

  Future<OpHandle> _mutate<K, T>(Resource<K, T> r, K id, Mutation<T> m, {List<OpId> dependsOn = const []}) async {
    _ensureOpen();
    if (!r.knowsKind(m.kind)) {
      throw ArgumentError('Mutation kind "${m.kind}" is not registered on resource "${r.type}"');
    }
    final key = r.keyOf(id);
    final payload = m.toJson(); // frozen now; never re-serialized
    r.decodeMutation(m.kind, payload); // fail fast if it cannot be rebuilt after a restart
    final op = await store.transaction((tx) async {
      final now = clock.now();
      final seq = await tx.meta.next('seq');
      final localVersion = await tx.meta.next('lv:${key.value}');
      final base = await tx.records.get(key);
      final opId = OpId(_uuid.next());
      final idem = r.useIdempotencyKeys
          ? idempotency.mint(OperationDraft(opId: opId, entity: key, kind: m.kind))
          : null;
      final op = Operation(
        id: opId,
        seq: seq,
        entity: key,
        kind: m.kind,
        payload: payload,
        idempotencyKey: idem,
        baseVersion: base?.serverVersion,
        localVersion: localVersion,
        dependsOn: dependsOn,
        createdAt: now,
        updatedAt: now,
      );
      await tx.ops.append(op);
      return op;
    });
    _emit(OpEnqueued(op.createdAt, op.id, key, op.kind));
    _kick();
    return OpHandle._(this, op.id, key);
  }

  /// Puts a `failed`, `deadLetter` or `conflicted` op back to `pending`.
  Future<void> retryOp(OpId id) async {
    await store.transaction((tx) async {
      final op = await tx.ops.get(id);
      if (op == null) return;
      if (op.status != OpStatus.failed && op.status != OpStatus.deadLetter && op.status != OpStatus.conflicted) {
        return;
      }
      await tx.ops.replace(op.copyWith(
        status: OpStatus.pending,
        attempts: 0,
        nextAttemptAt: null,
        leaseOwner: null,
        updatedAt: clock.now(),
      ));
    });
    _kick();
  }

  /// Removes an op that is not in flight. The fold drops it (visible
  /// rollback). Returns false if the op is in flight or missing.
  Future<bool> discardOp(OpId id) async {
    final op = await store.transaction((tx) async {
      final op = await tx.ops.get(id);
      if (op == null || op.status == OpStatus.inFlight) return null;
      await tx.ops.remove(id);
      return op;
    });
    if (op == null) return false;
    _emit(OpCancelled(clock.now(), op.id, op.entity, op.kind));
    _settle(op.id, OpOutcome.cancelled);
    _kick();
    return true;
  }

  // ===========================================================================
  // Outbox runner
  // ===========================================================================

  /// Sends every ready op and completes when nothing is in flight. Retries
  /// scheduled for later are not awaited. Safe to call any time.
  Future<void> drain() async {
    while (!_closed) {
      await _pump();
      if (_sendsInFlight.isEmpty) return;
      await Future.wait(_sendsInFlight.toList().map((f) => f.catchError((Object _) {})));
    }
  }

  void _kick() {
    if (_closed) return;
    _track(_pump());
  }

  /// Selects ready ops (write-ahead marking them `inFlight`) and launches
  /// their sends without waiting for them. Each finished send kicks another
  /// pump, so new work never waits behind a slow request of another entity.
  Future<void> _pump() async {
    if (_pumping) {
      _pumpAgain = true;
      return;
    }
    _pumping = true;
    try {
      var leaseDenied = false;
      do {
        _pumpAgain = false;
        // Offline: stop. The connectivity listener kicks us when it changes.
        if (_closed || connectivity.current == Reachability.offline) return;
        final batch = await store.transaction((tx) async {
          if (!await _ensureLease(tx, takeover: leaseMode == LeaseMode.primary)) {
            leaseDenied = true;
            return const <Operation>[];
          }
          final now = clock.now();
          final heads = await tx.ops.heads(limit: 1 << 20);
          final ready = <Operation>[];
          for (final h in heads) {
            if (ready.length + _activeSends.length >= maxConcurrentSends) break;
            if (_activeSends.contains(h.entity)) continue;
            if (h.status == OpStatus.inFlight && h.leaseOwner != runnerId && _isStuck(h, now)) {
              await _recoverOne(tx, h, now); // a dead runner left it behind
              continue;
            }
            if (h.status != OpStatus.pending || !h.isDue(now)) continue;
            final r = _resources[h.entity.type];
            if (r == null || !r.knowsKind(h.kind)) {
              await _deadLetter(tx, h, const ErrorInfo(code: 'unknown_kind'), now);
              continue;
            }
            if (!await _dependenciesMet(tx, h)) continue;
            final marked = h.copyWith(
              status: OpStatus.inFlight,
              attempts: h.attempts + 1,
              everSent: true,
              leaseOwner: runnerId,
              inFlightSince: now,
              nextAttemptAt: null,
              updatedAt: now,
            );
            await tx.ops.replace(marked); // write-ahead: committed BEFORE the send
            ready.add(marked);
          }
          return ready;
        });
        for (final op in batch) {
          _activeSends.add(op.entity);
          late final Future<void> f;
          f = _send(op).then((_) {}, onError: (Object e) {
            _lastDrainError = e; // e.g. the store died while applying the result
          }).whenComplete(() {
            _sendsInFlight.remove(f);
            _kick();
          });
          _sendsInFlight.add(f);
          _track(f);
        }
      } while (_pumpAgain && !_closed);
      if (_closed) return;
      if (leaseDenied) {
        // Someone else drains; try again when their lease could have expired.
        _scheduleWakeUp(clock.now().add(leaseTtl));
        return;
      }
      if (_activeSends.isEmpty) await _scheduleRetryTimer();
      _lastDrainError = null;
    } on Object catch (e) {
      _lastDrainError = e; // the store failed; a later kick will try again
    } finally {
      _pumping = false;
      if (_pumpAgain && !_closed) {
        _pumpAgain = false;
        _kick();
      }
    }
  }

  bool _isStuck(Operation op, DateTime now) {
    final since = op.inFlightSince;
    return since == null || now.difference(since) > leaseTtl * 2;
  }

  Future<bool> _dependenciesMet(StoreTxn tx, Operation op) async {
    for (final dep in op.dependsOn) {
      final d = await tx.ops.get(dep);
      if (d != null && d.status != OpStatus.succeeded) return false;
    }
    return true;
  }

  Future<void> _send(Operation op) async {
    final r = _resources[op.entity.type]!;
    _emit(OpSent(clock.now(), op.id, op.entity, op.kind, op.attempts));
    SendResult result;
    try {
      result = await r.sender.send(
        op,
        SendContext(attempt: op.attempts, idempotencyKey: op.idempotencyKey, baseVersion: op.baseVersion),
      );
    } on Object catch (e) {
      // Unknown whether the server applied it.
      result = Retryable(ErrorInfo(code: 'exception:${e.runtimeType}', ambiguous: true));
    }
    try {
      switch (result) {
        case final Accepted accepted:
          await _onAccepted(r, op, accepted);
        case final Conflicted conflicted:
          await _onConflicted(r, op, conflicted);
        case Rejected(:final error):
          await _transition(op, (cur, now) => cur.copyWith(
                status: OpStatus.failed,
                lastError: error,
                leaseOwner: null,
                updatedAt: now,
              ));
          _emit(OpFailed(clock.now(), op.id, op.entity, op.kind, error));
          _settle(op.id, OpOutcome.failed);
        case Retryable(:final error, :final retryAfter):
          await _onRetryable(r, op, error, retryAfter);
      }
    } finally {
      _activeSends.remove(op.entity);
    }
  }

  Future<void> _onAccepted(Resource<Object?, Object?> r, Operation op, Accepted a) async {
    final done = await store.transaction((tx) async {
      final cur = await tx.ops.get(op.id);
      if (cur == null) return false;
      final now = clock.now();
      final base = await tx.records.get(op.entity);
      final StoredRecord next;
      if (a.deleted) {
        next = _tombstone(op.entity, base, now, version: a.version);
      } else if (a.hasServerValue) {
        next = StoredRecord(
          key: op.entity,
          payload: a.serverValue,
          tombstone: a.serverValue == null,
          fetchedAt: now,
          lastAccessedAt: base?.lastAccessedAt ?? now,
          accessCount: base?.accessCount ?? 0,
          serverVersion: a.version ?? base?.serverVersion,
          validators: null, // body changed: old validators are useless
          priority: base?.priority ?? 0,
          pinned: base?.pinned ?? false,
        );
      } else {
        // No body: derive the new base with the op's own reducer so the UI
        // does not flicker back, and mark it for revalidation.
        final basePayload = (base == null || base.tombstone) ? null : base.payload;
        final derived = r.foldEncoded(basePayload, [(op.kind, op.payload)]);
        next = StoredRecord(
          key: op.entity,
          payload: derived,
          tombstone: derived == null,
          invalidated: true,
          fetchedAt: base?.fetchedAt ?? now,
          lastAccessedAt: base?.lastAccessedAt ?? now,
          accessCount: base?.accessCount ?? 0,
          serverVersion: a.version ?? base?.serverVersion,
          validators: null,
          priority: base?.priority ?? 0,
          pinned: base?.pinned ?? false,
        );
      }
      await tx.records.put(next);
      await tx.ops.remove(op.id);
      // Later ops of this entity were authored on top of THIS op's local
      // result; their base is the version this op produced. Rebase them so an
      // If-Match does not conflict with our own write.
      final newVersion = next.serverVersion;
      if (newVersion != null) {
        for (final later in await tx.ops.forEntity(op.entity)) {
          if (!later.everSent && later.seq > op.seq) {
            await tx.ops.replace(later.copyWith(baseVersion: newVersion, updatedAt: now));
          }
        }
      }
      return true;
    });
    if (!done) return;
    _ackEpoch[op.entity] = ++_epoch;
    final now = clock.now();
    _emit(OpSucceeded(now, op.id, op.entity, op.kind, now.difference(op.createdAt)));
    _settle(op.id, OpOutcome.succeeded);
  }

  Future<void> _onConflicted(Resource<Object?, Object?> r, Operation op, Conflicted c) async {
    // 1. Learn the server state (from the response, or by refetching).
    final before = await store.transaction((tx) => tx.records.get(op.entity));
    Object? serverPayload;
    var serverDeleted = false;
    var serverVersion = c.version;
    if (c.hasServerValue) {
      serverPayload = c.serverValue;
      serverDeleted = c.serverValue == null;
      await store.transaction((tx) async {
        final now = clock.now();
        final cur = await tx.records.get(op.entity);
        await tx.records.put(serverDeleted
            ? _tombstone(op.entity, cur, now, version: c.version)
            : StoredRecord(
                key: op.entity,
                payload: c.serverValue,
                fetchedAt: now,
                lastAccessedAt: cur?.lastAccessedAt ?? now,
                accessCount: cur?.accessCount ?? 0,
                serverVersion: c.version,
                priority: cur?.priority ?? 0,
                pinned: cur?.pinned ?? false,
              ));
      });
    } else {
      try {
        await _fetch(r, op.entity);
      } on Object {
        // Could not learn the server state: retry the op later instead.
        await _onRetryable(r, op, const ErrorInfo(code: 'conflict_unresolved'), null);
        return;
      }
      final after = await store.transaction((tx) => tx.records.get(op.entity));
      serverDeleted = after == null || after.tombstone;
      serverPayload = after?.payload;
      serverVersion = after?.serverVersion;
    }

    // 2. Ask the resolver (outside any transaction: it may be slow).
    Resolution<Object?> resolution;
    try {
      resolution = await r.resolveConflict(
        op: op,
        basePayload: (before == null || before.tombstone) ? null : before.payload,
        serverPayload: serverPayload,
        serverDeleted: serverDeleted,
        serverVersion: serverVersion,
      );
    } on Object {
      resolution = const Park<Object?>();
    }

    // 3. Apply it.
    final now = clock.now();
    switch (resolution) {
      case TakeServer():
        await store.transaction((tx) async {
          if (await tx.ops.get(op.id) != null) await tx.ops.remove(op.id);
        });
        _emit(OpConflicted(now, op.id, op.entity, op.kind, 'TakeServer'));
        _settle(op.id, OpOutcome.droppedByConflict);
      case ResendLocal():
        await _transition(op, (cur, now) => cur.copyWith(
              status: OpStatus.pending,
              baseVersion: serverVersion,
              nextAttemptAt: null,
              leaseOwner: null,
              updatedAt: now,
            ));
        _emit(OpConflicted(now, op.id, op.entity, op.kind, 'ResendLocal'));
      case MergeWith(:final value):
        final payload = r.encodeUntyped(value);
        await _transition(op, (cur, now) => cur.copyWith(
              kind: Replace.kindName,
              payload: payload,
              baseVersion: serverVersion,
              // Different payload => a NEW idempotency key (never reuse a sent key).
              idempotencyKey: r.useIdempotencyKeys
                  ? idempotency.mint(OperationDraft(opId: cur.id, entity: cur.entity, kind: Replace.kindName))
                  : null,
              status: OpStatus.pending,
              nextAttemptAt: null,
              leaseOwner: null,
              updatedAt: now,
            ));
        _emit(OpConflicted(now, op.id, op.entity, op.kind, 'MergeWith'));
      case Park():
        await _transition(op, (cur, now) => cur.copyWith(
              status: OpStatus.conflicted,
              serverVersion: serverVersion,
              lastError: const ErrorInfo(code: 'conflict'),
              leaseOwner: null,
              updatedAt: now,
            ));
        _emit(OpConflicted(now, op.id, op.entity, op.kind, 'Park'));
        _settle(op.id, OpOutcome.conflicted);
    }
  }

  Future<void> _onRetryable(Resource<Object?, Object?> r, Operation op, ErrorInfo error, Duration? retryAfter) async {
    final now = clock.now();
    if (error.ambiguous && op.idempotencyKey == null && r.delivery == Delivery.atMostOnce) {
      final e = ErrorInfo(code: 'ambiguous', message: error.code, ambiguous: true);
      await _transition(op, (cur, now) => cur.copyWith(
            status: OpStatus.failed,
            lastError: e,
            leaseOwner: null,
            updatedAt: now,
          ));
      _emit(OpFailed(now, op.id, op.entity, op.kind, e));
      _settle(op.id, OpOutcome.failed);
      return;
    }
    switch (retry.decide(attempts: op.attempts, error: error, retryAfter: retryAfter)) {
      case RetryAt(:final delay):
        final at = now.add(delay);
        await _transition(op, (cur, now) => cur.copyWith(
              status: OpStatus.pending,
              nextAttemptAt: at,
              lastError: error,
              leaseOwner: null,
              updatedAt: now,
            ));
        _emit(OpRetryScheduled(now, op.id, op.entity, op.kind, at, error));
      case GiveUp():
        await store.transaction((tx) async {
          final cur = await tx.ops.get(op.id);
          if (cur != null) await _deadLetter(tx, cur, error, clock.now());
        });
    }
  }

  Future<void> _deadLetter(StoreTxn tx, Operation op, ErrorInfo error, DateTime now) async {
    await tx.ops.replace(op.copyWith(
      status: OpStatus.deadLetter,
      lastError: error,
      leaseOwner: null,
      updatedAt: now,
    ));
    _emit(OpDeadLettered(now, op.id, op.entity, op.kind, error));
    _settle(op.id, OpOutcome.deadLetter);
  }

  /// Applies [change] to the current stored version of [op], if it still exists.
  Future<void> _transition(Operation op, Operation Function(Operation cur, DateTime now) change) =>
      store.transaction((tx) async {
        final cur = await tx.ops.get(op.id);
        if (cur == null) return;
        await tx.ops.replace(change(cur, clock.now()));
      });

  StoredRecord _tombstone(RecordKey key, StoredRecord? base, DateTime now, {Version? version}) => StoredRecord(
        key: key,
        payload: null,
        tombstone: true,
        fetchedAt: now,
        lastAccessedAt: base?.lastAccessedAt ?? now,
        accessCount: base?.accessCount ?? 0,
        serverVersion: version ?? base?.serverVersion,
        priority: base?.priority ?? 0,
        pinned: base?.pinned ?? false,
      );

  Future<void> _scheduleRetryTimer() async {
    if (_closed) return;
    final next = await store.transaction((tx) async {
      final now = clock.now();
      DateTime? earliest;
      for (final h in await tx.ops.heads(limit: 1 << 20)) {
        final at = h.nextAttemptAt;
        if (h.status == OpStatus.pending &&
            at != null &&
            at.isAfter(now) &&
            (earliest == null || at.isBefore(earliest))) {
          earliest = at;
        }
      }
      return earliest;
    });
    if (next != null) _scheduleWakeUp(next);
  }

  void _scheduleWakeUp(DateTime next) {
    if (_closed) return;
    if (_retryTimerAt != null && !_retryTimerAt!.isAfter(next)) return; // an earlier timer exists
    _retryTimer?.cancel();
    _retryTimerAt = next;
    final delay = next.difference(clock.now());
    _retryTimer = scheduler.schedule(delay.isNegative ? Duration.zero : delay, () {
      _retryTimer = null;
      _retryTimerAt = null;
      _kick();
    });
  }

  // ===========================================================================
  // Lease + crash recovery
  // ===========================================================================

  Future<bool> _ensureLease(StoreTxn tx, {required bool takeover}) async {
    final now = clock.now();
    final raw = await tx.meta.get('lease');
    String? owner;
    var expiresAt = 0;
    if (raw is Map) {
      owner = raw['owner'] as String?;
      expiresAt = (raw['expiresAt'] as int?) ?? 0;
    }
    final free = owner == null || owner == runnerId || expiresAt <= now.millisecondsSinceEpoch;
    final held = free || takeover;
    if (held) {
      await tx.meta.put('lease', {'owner': runnerId, 'expiresAt': now.add(leaseTtl).millisecondsSinceEpoch});
    }
    if (held != _leaseHeld) {
      _leaseHeld = held;
      _emit(LeaseChanged(now, held: held));
    }
    return held;
  }

  Future<void> _recover() async {
    await store.transaction((tx) async {
      if (!await _ensureLease(tx, takeover: leaseMode == LeaseMode.primary)) return;
      final now = clock.now();
      final open = await tx.ops.where({OpStatus.pending, OpStatus.inFlight});
      for (final op in open) {
        final r = _resources[op.entity.type];
        if (r == null || !r.knowsKind(op.kind)) {
          // Never drop user intent silently.
          await _deadLetter(tx, op, const ErrorInfo(code: 'unknown_kind'), now);
          continue;
        }
        if (op.status == OpStatus.inFlight && op.leaseOwner != runnerId) {
          await _recoverOne(tx, op, now);
        }
      }
    });
  }

  /// An op found `inFlight` whose runner is gone: the send is ambiguous.
  Future<void> _recoverOne(StoreTxn tx, Operation op, DateTime now) async {
    final r = _resources[op.entity.type];
    final resend = op.idempotencyKey != null || r == null || r.delivery == Delivery.atLeastOnce;
    if (resend) {
      await tx.ops.replace(op.copyWith(
        status: OpStatus.pending,
        nextAttemptAt: null,
        leaseOwner: null,
        lastError: const ErrorInfo(code: 'recovered', ambiguous: true),
        updatedAt: now,
      ));
    } else {
      const e = ErrorInfo(code: 'ambiguous', message: 'process died while in flight', ambiguous: true);
      await tx.ops.replace(op.copyWith(status: OpStatus.failed, lastError: e, leaseOwner: null, updatedAt: now));
      _settle(op.id, OpOutcome.failed);
    }
    _emit(OpRecovered(now, op.id, op.entity, op.kind, resent: resend));
  }

  void _onReachability(Reachability r) {
    _emit(ReachabilityChanged(clock.now(), r));
    if (r != Reachability.offline) _kick();
  }

  // ===========================================================================
  // Retention
  // ===========================================================================

  /// Pins [key] ("available offline"): never evicted.
  Future<void> setPinned(RecordKey key, bool pinned) => store.transaction((tx) async {
        final r = await tx.records.get(key);
        if (r != null) await tx.records.put(r.copyWith(pinned: pinned));
      });

  /// Marks [key] for revalidation on the next read.
  Future<void> invalidate(RecordKey key) => store.transaction((tx) async {
        final r = await tx.records.get(key);
        if (r != null) await tx.records.put(r.copyWith(invalidated: true));
      });

  void _scheduleCompact() {
    if (!retention.isBounded || _compactScheduled || _closed) return;
    _compactScheduled = true;
    scheduleMicrotask(() {
      _compactScheduled = false;
      _track(compact().then((_) {}, onError: (Object _) {}));
    });
  }

  /// Applies the [retention] policy now. Entities with blocking ops, pinned
  /// records and watched records are never evicted.
  Future<int> compact() async {
    if (!retention.isBounded) return 0;
    await flushAccess();
    var evicted = 0;
    var bytes = 0;
    var warn = false;
    await store.transaction((tx) async {
      final now = clock.now();
      final stats = await tx.records.stats();
      var records = stats.records;
      var total = stats.bytes;
      final candidates = (await tx.records.evictionCandidates(limit: records))
          .where((c) => !_watchers.containsKey(c.key) && !_activeSends.contains(c.key))
          .toList();
      final retainFor = retention.retainFor;
      final kept = <StoredRecord>[];
      for (final c in candidates) {
        if (retainFor != null && now.difference(c.lastAccessedAt) > retainFor) {
          await tx.records.delete(c.key);
          records--;
          total -= c.sizeBytes;
          evicted++;
          bytes += c.sizeBytes;
        } else {
          kept.add(c);
        }
      }
      if (retention.overBudget(records, total)) {
        kept.sort((a, b) => retention.score(a).compareTo(retention.score(b)));
        for (final c in kept) {
          if (retention.underLowWatermark(records, total)) break;
          await tx.records.delete(c.key);
          records--;
          total -= c.sizeBytes;
          evicted++;
          bytes += c.sizeBytes;
        }
        warn = retention.overBudget(records, total);
      }
    });
    final now = clock.now();
    if (evicted > 0) _emit(Evicted(now, count: evicted, bytes: bytes));
    if (warn) _emit(PolicyWarning(now, 'Pinned, dirty or watched records exceed the retention budget'));
    return evicted;
  }

  void _touch(RecordKey key) {
    _accessBuffer[key] = (_accessBuffer[key] ?? 0) + 1;
    if (_accessTimer == null && !_closed) {
      _accessTimer = scheduler.schedule(accessFlushInterval, () {
        _accessTimer = null;
        _track(flushAccess().then((_) {}, onError: (Object _) {}));
      });
    }
  }

  /// Writes buffered access statistics (approximate LRU, section 8.3).
  Future<void> flushAccess() async {
    if (_accessBuffer.isEmpty) return;
    final batch = Map<RecordKey, int>.of(_accessBuffer);
    _accessBuffer.clear();
    await store.transaction((tx) => tx.records.markAccessed(batch, clock.now()));
  }

  // ===========================================================================
  // Inspection, lifecycle, plumbing
  // ===========================================================================

  /// Ops with the given statuses (all non-terminal by default), by `seq`.
  Future<List<Operation>> ops([Set<OpStatus>? statuses]) => store.transaction((tx) => tx.ops.where(
        statuses ??
            {OpStatus.pending, OpStatus.inFlight, OpStatus.conflicted, OpStatus.failed, OpStatus.deadLetter},
      ));

  /// Queue gauges for dashboards.
  Future<Map<OpStatus, int>> queueCounts() => store.transaction((tx) => tx.ops.countByStatus());

  /// Waits until no fetch, drain, compaction or watch refresh started by this
  /// engine is running. Timers scheduled for the future are not awaited.
  Future<void> settle() async {
    for (var i = 0; i < 1000; i++) {
      await Future<void>.delayed(Duration.zero);
      if (_tracked.isEmpty) return;
      await Future.wait(_tracked.toList().map((f) => f.catchError((Object _) {})));
    }
  }

  /// Stops timers and listeners. Does NOT close the store.
  Future<void> close() async {
    if (_closed) return;
    await settle();
    try {
      await flushAccess();
    } on Object {
      // best effort
    }
    _closed = true;
    _retryTimer?.cancel();
    _accessTimer?.cancel();
    await _connectivitySub?.cancel();
    for (final e in _waiters.entries) {
      for (final c in e.value) {
        if (!c.isCompleted) c.complete(_outcomes[e.key] ?? OpOutcome.unknown);
      }
    }
    _waiters.clear();
    await _events.close();
  }

  void _ensureOpen() {
    if (_closed) throw StateError('OfflineEngine is closed');
  }

  void _emit(SyncEvent e) {
    metrics.record(e);
    eventSink?.emit(e);
    if (!_events.isClosed) _events.add(e);
  }

  void _track(Future<void> f) {
    _tracked.add(f);
    f.whenComplete(() {
      _tracked.remove(f);
    }).ignore();
  }

  void _settle(OpId id, OpOutcome outcome) {
    _outcomes[id] = outcome;
    final waiters = _waiters.remove(id);
    if (waiters == null) return;
    for (final c in waiters) {
      if (!c.isCompleted) c.complete(outcome);
    }
  }

  Future<OpOutcome> _awaitOutcome(OpId id) async {
    final known = _outcomes[id];
    if (known != null) return known;
    final current = await store.transaction((tx) => tx.ops.get(id));
    if (current == null) return _outcomes[id] ?? OpOutcome.unknown;
    switch (current.status) {
      case OpStatus.failed:
        return OpOutcome.failed;
      case OpStatus.deadLetter:
        return OpOutcome.deadLetter;
      case OpStatus.conflicted:
        return OpOutcome.conflicted;
      case OpStatus.cancelled:
        return OpOutcome.cancelled;
      case OpStatus.succeeded:
        return OpOutcome.succeeded;
      case OpStatus.pending:
      case OpStatus.inFlight:
        final known2 = _outcomes[id];
        if (known2 != null) return known2;
        final c = Completer<OpOutcome>();
        (_waiters[id] ??= []).add(c);
        return c.future;
    }
  }
}

final class _Local {
  const _Local(this.record, this.ops);

  final StoredRecord? record;
  final List<Operation> ops;
}

/// Typed facade over one [Resource].
final class Repository<K, T> {
  Repository._(this._engine, this.resource);

  final OfflineEngine _engine;
  final Resource<K, T> resource;

  /// Reads [id] following the freshness windows (fresh / SWR / fetch /
  /// stale-if-error / stale-if-offline). Never throws for network errors:
  /// they are reported in [Snapshot.error].
  Future<Snapshot<T>> get(K id, {bool forceRefresh = false}) =>
      _engine._get(resource, id, forceRefresh: forceRefresh);

  /// Reads and then follows every committed change of [id] (optimistic
  /// writes, acknowledgements, refreshes). Duplicate snapshots are skipped.
  Stream<Snapshot<T>> watch(K id) => _engine._watch(resource, id);

  /// Local-only read: never touches the network.
  Future<Snapshot<T>> peek(K id) => _engine._peek(resource, resource.keyOf(id));

  /// Enqueues [mutation] atomically. Returns once the op is durable; the UI
  /// sees the optimistic value immediately through [get]/[watch].
  Future<OpHandle> mutate(K id, Mutation<T> mutation, {List<OpId> dependsOn = const []}) =>
      _engine._mutate(resource, id, mutation, dependsOn: dependsOn);

  /// Marks [id] for revalidation on the next read.
  Future<void> invalidate(K id) => _engine.invalidate(resource.keyOf(id));

  /// Pins [id] so it is never evicted.
  Future<void> pin(K id, {bool pinned = true}) => _engine.setPinned(resource.keyOf(id), pinned);

  RecordKey keyOf(K id) => resource.keyOf(id);
}

/// Handle to an enqueued operation.
final class OpHandle {
  OpHandle._(this._engine, this.id, this.entity);

  final OfflineEngine _engine;
  final OpId id;
  final RecordKey entity;

  /// Current stored state (null once removed after success or discard).
  Future<Operation?> current() => _engine.store.transaction((tx) => tx.ops.get(id));

  /// Completes when the op leaves the automatic pipeline: acknowledged,
  /// dropped by a conflict, parked, failed, dead-lettered or discarded.
  Future<OpOutcome> get done => _engine._awaitOutcome(id);

  Future<void> retry() => _engine.retryOp(id);

  Future<bool> discard() => _engine.discardOp(id);
}
