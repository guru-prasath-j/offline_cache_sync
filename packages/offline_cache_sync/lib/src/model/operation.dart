import 'keys.dart';
import 'records.dart';

/// Durable lifecycle of an [Operation].
///
/// "Retrying" is not a separate state: it is [pending] with a future
/// `nextAttemptAt`. "Running" is [inFlight], which after a crash means
/// "may or may not have reached the server".
enum OpStatus {
  /// Waiting to be sent (possibly after a backoff delay).
  pending,

  /// Marked (write-ahead) before being handed to the sender.
  inFlight,

  /// The server reported a conflict and the resolver parked it for the app.
  conflicted,

  /// Permanently rejected by the server (e.g. 4xx validation). Kept until the
  /// app edits, retries or discards it.
  failed,

  /// Retries exhausted, or the op cannot be processed (unknown kind,
  /// idempotency mismatch). Kept for inspection.
  deadLetter,

  /// Acknowledged by the server. Usually removed right away.
  succeeded,

  /// Cancelled by the app before it was ever sent.
  cancelled;

  /// Ops in these states still describe user intent and are folded into reads.
  bool get isFolded => this == pending || this == inFlight || this == conflicted;

  /// Ops in these states block later ops of the same entity.
  bool get blocksEntity => this != succeeded && this != cancelled;

  /// No further automatic processing will happen.
  bool get isTerminal => this == succeeded || this == cancelled;
}

/// A classified error, safe to persist (messages must already be redacted).
final class ErrorInfo {
  const ErrorInfo({required this.code, this.httpStatus, this.message, this.ambiguous = false});

  /// Machine-readable code: `timeout`, `http_503`, `validation`, ...
  final String code;
  final int? httpStatus;
  final String? message;

  /// True when the server may have applied the request (e.g. a timeout after
  /// the request was written). Drives the `Delivery` rules.
  final bool ambiguous;

  Map<String, Object?> toJson() => {
        'code': code,
        if (httpStatus != null) 'httpStatus': httpStatus,
        if (message != null) 'message': message,
        if (ambiguous) 'ambiguous': true,
      };

  static ErrorInfo? fromJson(Object? json) {
    if (json is! Map) return null;
    return ErrorInfo(
      code: json['code'] as String,
      httpStatus: json['httpStatus'] as int?,
      message: json['message'] as String?,
      ambiguous: json['ambiguous'] == true,
    );
  }

  @override
  String toString() => 'ErrorInfo($code${httpStatus != null ? ', $httpStatus' : ''}${ambiguous ? ', ambiguous' : ''})';
}

/// One durable entry in the outbox. Immutable; transitions produce copies.
final class Operation {
  const Operation({
    required this.id,
    required this.seq,
    required this.entity,
    required this.kind,
    required this.payload,
    required this.localVersion,
    required this.createdAt,
    required this.updatedAt,
    this.idempotencyKey,
    this.baseVersion,
    this.dependsOn = const [],
    this.status = OpStatus.pending,
    this.attempts = 0,
    this.everSent = false,
    this.nextAttemptAt,
    this.leaseOwner,
    this.inFlightSince,
    this.lastError,
    this.serverVersion,
  });

  final OpId id;

  /// Global monotonic sequence allocated by the store inside the enqueue
  /// transaction. Defines fold order and per-entity send order.
  final int seq;

  final RecordKey entity;

  /// Registered mutation kind, e.g. `user.rename`.
  final String kind;

  /// Frozen JSON arguments of the mutation. Never re-serialized from live
  /// objects, so retries send byte-identical payloads.
  final Object? payload;

  final IdempotencyKey? idempotencyKey;

  /// Server version the user edited against (for If-Match / conflicts).
  final Version? baseVersion;

  /// Per-entity counter: 1, 2, 3, ...
  final int localVersion;

  /// Ops that must succeed before this one may be sent.
  final List<OpId> dependsOn;

  final OpStatus status;
  final int attempts;

  /// True once the op has ever been marked in flight. Coalescing and
  /// payload rewrites are forbidden after that.
  final bool everSent;

  final DateTime createdAt;
  final DateTime updatedAt;
  final DateTime? nextAttemptAt;
  final String? leaseOwner;
  final DateTime? inFlightSince;
  final ErrorInfo? lastError;
  final Version? serverVersion;

  bool isDue(DateTime now) => nextAttemptAt == null || !nextAttemptAt!.isAfter(now);

  Operation copyWith({
    Object? payload = _unset,
    String? kind,
    Object? idempotencyKey = _unset,
    Object? baseVersion = _unset,
    OpStatus? status,
    int? attempts,
    bool? everSent,
    required DateTime updatedAt,
    Object? nextAttemptAt = _unset,
    Object? leaseOwner = _unset,
    Object? inFlightSince = _unset,
    Object? lastError = _unset,
    Object? serverVersion = _unset,
  }) {
    return Operation(
      id: id,
      seq: seq,
      entity: entity,
      kind: kind ?? this.kind,
      payload: identical(payload, _unset) ? this.payload : payload,
      idempotencyKey:
          identical(idempotencyKey, _unset) ? this.idempotencyKey : idempotencyKey as IdempotencyKey?,
      baseVersion: identical(baseVersion, _unset) ? this.baseVersion : baseVersion as Version?,
      localVersion: localVersion,
      dependsOn: dependsOn,
      status: status ?? this.status,
      attempts: attempts ?? this.attempts,
      everSent: everSent ?? this.everSent,
      createdAt: createdAt,
      updatedAt: updatedAt,
      nextAttemptAt: identical(nextAttemptAt, _unset) ? this.nextAttemptAt : nextAttemptAt as DateTime?,
      leaseOwner: identical(leaseOwner, _unset) ? this.leaseOwner : leaseOwner as String?,
      inFlightSince: identical(inFlightSince, _unset) ? this.inFlightSince : inFlightSince as DateTime?,
      lastError: identical(lastError, _unset) ? this.lastError : lastError as ErrorInfo?,
      serverVersion: identical(serverVersion, _unset) ? this.serverVersion : serverVersion as Version?,
    );
  }

  @override
  String toString() => 'Operation(${id.value}, #$seq, ${entity.value}, $kind, $status, attempts=$attempts)';
}

const Object _unset = Object();
