import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'package:offline_cache_sync/offline_cache_sync.dart';

/// Scripted failure for the next send.
enum SendFailure {
  /// The request never reaches the server; the client sees a timeout.
  lostBeforeServer,

  /// The server applies the op, then the response is lost (ambiguous).
  lostAfterApply,

  /// The server answers 503 without applying anything.
  unavailable,

  /// The server rejects the op permanently (422).
  reject,
}

/// One call observed by the server.
final class SentCall {
  const SentCall({
    required this.opId,
    required this.entityId,
    required this.kind,
    required this.attempt,
    required this.idempotencyKey,
    required this.payload,
  });

  final OpId opId;
  final String entityId;
  final String kind;
  final int attempt;
  final IdempotencyKey? idempotencyKey;
  final Object? payload;
}

/// An in-memory REST-like backend for `Map<String, Object?>` entities.
///
/// * Versions are integer revisions; ETags are `"r<revision>"`.
/// * Honours `Idempotency-Key` like Stripe / the IETF draft: a replay with the
///   same key and payload returns the stored result; a different payload is
///   rejected (422, `idempotency_mismatch`).
/// * Detects conflicts when `SendContext.baseVersion.revision` is stale.
/// * Understands the built-in kinds (`$set`, `$replace`, `$delete`) plus
///   `counter.increment` ({"field": f, "by": n}), which is NOT idempotent:
///   a duplicated send is visible in the data.
final class FakeServer implements RemoteSource<String>, MutationSender {
  FakeServer({this.checkVersions = true, this.honourIdempotencyKeys = true});

  bool checkVersions;
  bool honourIdempotencyKeys;

  final Map<String, Map<String, Object?>> _data = {};
  final Map<String, int> _revision = {};
  final Map<String, (String, SendResult)> _idempotency = {};
  final Queue<SendFailure> _sendFailures = Queue();
  final Queue<Object> _fetchFailures = Queue();

  /// Every send call, in arrival order.
  final List<SentCall> calls = [];

  /// Ops actually applied (after idempotency dedup), in order: (entityId, opId).
  final List<(String, OpId)> applied = [];

  int fetchCount = 0;

  /// If set, every send waits for this before being processed.
  Completer<void>? sendGate;

  /// If set, every fetch waits for this before being processed.
  Completer<void>? fetchGate;

  // ---- scripting -------------------------------------------------------------

  void seed(String id, Map<String, Object?> value) {
    _data[id] = Map.of(value);
    _revision[id] = (_revision[id] ?? 0) + 1;
  }

  /// Another client edits the entity (bumps the revision).
  void externalEdit(String id, Map<String, Object?> fields) {
    final cur = Map<String, Object?>.of(_data[id] ?? {});
    cur.addAll(fields);
    _data[id] = cur;
    _revision[id] = (_revision[id] ?? 0) + 1;
  }

  void externalDelete(String id) {
    _data.remove(id);
    _revision[id] = (_revision[id] ?? 0) + 1;
  }

  void failNextSends(int n, SendFailure kind) {
    for (var i = 0; i < n; i++) {
      _sendFailures.add(kind);
    }
  }

  void failNextFetches(int n, [Object error = const _Unavailable()]) {
    for (var i = 0; i < n; i++) {
      _fetchFailures.add(error);
    }
  }

  Map<String, Object?>? valueOf(String id) => _data[id] == null ? null : Map.unmodifiable(_data[id]!);

  int revisionOf(String id) => _revision[id] ?? 0;

  Map<String, Map<String, Object?>> get snapshot => {for (final e in _data.entries) e.key: Map.of(e.value)};

  // ---- RemoteSource ----------------------------------------------------------

  @override
  Future<FetchResult> fetch(String id, FetchContext context) async {
    fetchCount++;
    if (_fetchFailures.isNotEmpty) throw _fetchFailures.removeFirst();
    // The response is computed NOW (request time); the gate only delays its
    // arrival, which is how a slow, out-of-date response is simulated.
    final FetchResult result;
    final value = _data[id];
    if (value == null) {
      result = const Gone();
    } else {
      final rev = _revision[id]!;
      final etag = '"r$rev"';
      result = context.validators?.etag == etag
          ? NotModified(validators: Validators(etag: etag))
          : Fetched(Map<String, Object?>.of(value), version: Version(revision: rev), validators: Validators(etag: etag));
    }
    final gate = fetchGate;
    if (gate != null) await gate.future;
    return result;
  }

  /// Drops any scripted send/fetch failures that were not consumed.
  void clearFailures() {
    _sendFailures.clear();
    _fetchFailures.clear();
  }

  int get pendingSendFailures => _sendFailures.length;

  // ---- MutationSender --------------------------------------------------------

  @override
  Future<SendResult> send(Operation op, SendContext context) async {
    final id = op.entity.id;
    calls.add(SentCall(
      opId: op.id,
      entityId: id,
      kind: op.kind,
      attempt: context.attempt,
      idempotencyKey: context.idempotencyKey,
      payload: op.payload,
    ));
    final gate = sendGate;
    if (gate != null) await gate.future;

    final failure = _sendFailures.isEmpty ? null : _sendFailures.removeFirst();
    switch (failure) {
      case SendFailure.lostBeforeServer:
        throw TimeoutException('lost before server');
      case SendFailure.unavailable:
        return const Retryable(ErrorInfo(code: 'http_503', httpStatus: 503));
      case SendFailure.reject:
        return const Rejected(ErrorInfo(code: 'validation', httpStatus: 422));
      case SendFailure.lostAfterApply:
      case null:
        break;
    }

    final key = honourIdempotencyKeys ? context.idempotencyKey : null;
    final fingerprint = jsonEncode([op.kind, op.payload]);
    if (key != null) {
      final stored = _idempotency[key.value];
      if (stored != null) {
        if (stored.$1 != fingerprint) {
          return const Rejected(ErrorInfo(code: 'idempotency_mismatch', httpStatus: 422));
        }
        if (failure == SendFailure.lostAfterApply) throw TimeoutException('lost after apply (replay)');
        return stored.$2; // replay: no second effect
      }
    }

    final base = context.baseVersion?.revision;
    if (checkVersions && base != null && base != (_revision[id] ?? 0)) {
      final cur = _data[id];
      return Conflicted.withValue(
        cur == null ? null : Map<String, Object?>.of(cur),
        version: Version(revision: _revision[id] ?? 0),
      );
    }

    final result = _apply(id, op);
    applied.add((id, op.id));
    if (key != null) _idempotency[key.value] = (fingerprint, result);
    if (failure == SendFailure.lostAfterApply) throw TimeoutException('lost after apply');
    return result;
  }

  SendResult _apply(String id, Operation op) {
    final cur = _data[id];
    switch (op.kind) {
      case r'$delete':
        _data.remove(id);
        _revision[id] = (_revision[id] ?? 0) + 1;
        return Accepted(deleted: true, version: Version(revision: _revision[id]));
      case r'$replace':
        _data[id] = Map<String, Object?>.from(op.payload! as Map);
      case r'$set':
        final next = Map<String, Object?>.of(cur ?? {});
        (op.payload! as Map).forEach((k, v) {
          if (v == null) {
            next.remove(k);
          } else {
            next[k as String] = v;
          }
        });
        _data[id] = next;
      case 'counter.increment':
        final args = op.payload! as Map;
        final field = args['field'] as String;
        final next = Map<String, Object?>.of(cur ?? {});
        next[field] = ((next[field] as int?) ?? 0) + (args['by'] as int);
        _data[id] = next;
      default:
        return Rejected(ErrorInfo(code: 'unknown_kind:${op.kind}', httpStatus: 400));
    }
    _revision[id] = (_revision[id] ?? 0) + 1;
    return Accepted.withValue(Map<String, Object?>.of(_data[id]!), version: Version(revision: _revision[id]));
  }
}

final class _Unavailable implements Exception {
  const _Unavailable();

  @override
  String toString() => 'Unavailable (simulated 503)';
}

/// Client-side mutation matching the server's `counter.increment`.
final class Increment extends Mutation<Map<String, Object?>> {
  const Increment(this.field, this.by);

  factory Increment.fromJson(Object? json) {
    final m = json! as Map;
    return Increment(m['field'] as String, m['by'] as int);
  }

  static const kindName = 'counter.increment';

  final String field;
  final int by;

  @override
  String get kind => kindName;

  @override
  Object? toJson() => {'field': field, 'by': by};

  @override
  Map<String, Object?>? apply(Map<String, Object?>? current) {
    final next = <String, Object?>{...?current};
    next[field] = ((next[field] as int?) ?? 0) + by;
    return next;
  }
}

/// Codec for `Map<String, Object?>` entities.
final Codec<Map<String, Object?>> mapCodec = JsonCodecOf<Map<String, Object?>>(
  (json) => Map<String, Object?>.from(json! as Map),
  (value) => value,
);
