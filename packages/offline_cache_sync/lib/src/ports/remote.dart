import '../model/keys.dart';
import '../model/operation.dart';
import '../model/records.dart';

/// Passed to [RemoteSource.fetch].
final class FetchContext {
  /// Creates a fetch context.
  const FetchContext({required this.key, this.validators, this.attempt = 0});

  /// The record being fetched.
  final RecordKey key;

  /// Validators of the stored record, for `If-None-Match` /
  /// `If-Modified-Since`. Null when nothing is cached.
  final Validators? validators;

  /// Retry attempt, starting at 0 for the first fetch.
  final int attempt;
}

/// Reads one entity from the server. Knows your URLs; the engine does not.
///
/// Implementations return a [FetchResult] for protocol outcomes and THROW
/// for transport failures (timeouts, DNS, TLS, 5xx). Throwing is how the
/// engine decides stale-if-error.
abstract interface class RemoteSource<K> {
  /// Fetches entity [id]. Return a [FetchResult]; throw on transport failure.
  Future<FetchResult> fetch(K id, FetchContext context);
}

/// Outcome of [RemoteSource.fetch]: [Fetched], [NotModified] or [Gone].
sealed class FetchResult {
  /// Base constructor for subclasses.
  const FetchResult();
}

/// 200 with a body. [value] must be JSON-compatible (it is stored as-is and
/// decoded with the resource codec).
final class Fetched extends FetchResult {
  /// Creates a result carrying [value].
  const Fetched(this.value, {this.version, this.validators, this.maxAge});

  /// The entity as a JSON-compatible value.
  final Object? value;
  /// Server version of the entity, used for conflict detection.
  final Version? version;
  /// `ETag` / `Last-Modified` to send on the next revalidation.
  final Validators? validators;

  /// From `Cache-Control: max-age`, if the server sent one.
  final Duration? maxAge;
}

/// 304: the cached body is still current. Renews freshness.
final class NotModified extends FetchResult {
  /// Creates a not-modified result.
  const NotModified({this.validators, this.maxAge});

  /// Updated validators, if the server sent new ones.
  final Validators? validators;
  /// From `Cache-Control: max-age`, if the server sent one.
  final Duration? maxAge;
}

/// 404 / 410: the entity does not exist on the server.
final class Gone extends FetchResult {
  /// Creates a gone result.
  const Gone();
}

/// Passed to [MutationSender.send].
final class SendContext {
  /// Creates a send context.
  const SendContext({required this.attempt, this.idempotencyKey, this.baseVersion});

  /// 1 for the first attempt.
  final int attempt;

  /// Persisted with the op; identical on every attempt. Send it as the
  /// `Idempotency-Key` header.
  final IdempotencyKey? idempotencyKey;

  /// Version the user edited against. Send it as `If-Match` when you can.
  final Version? baseVersion;
}

/// Sends one operation. Returns a classified [SendResult]. A thrown
/// exception is treated as an ambiguous, retryable failure (the request may
/// or may not have reached the server).
abstract interface class MutationSender {
  /// Sends [op] to the server and classifies the response.
  Future<SendResult> send(Operation op, SendContext context);
}

/// Outcome of [MutationSender.send]: [Accepted], [Conflicted], [Rejected]
/// or [Retryable].
sealed class SendResult {
  /// Base constructor for subclasses.
  const SendResult();
}

/// 2xx. If the server returned the entity, pass it as [serverValue]
/// (JSON-compatible); it becomes the new base.
final class Accepted extends SendResult {
  /// Creates an accepted result. Set [hasServerValue] when [serverValue]
  /// is meaningful (it may legitimately be null).
  const Accepted({this.serverValue, this.hasServerValue = false, this.version, this.deleted = false});

  /// Convenience for a response that carries the entity.
  const Accepted.withValue(Object? value, {Version? version})
      : this(serverValue: value, hasServerValue: true, version: version);

  /// The entity as returned by the server, if any.
  final Object? serverValue;
  /// Whether [serverValue] was returned (distinguishes null from absent).
  final bool hasServerValue;
  /// New server version of the entity.
  final Version? version;

  /// The server confirmed the entity is deleted.
  final bool deleted;
}

/// 409 (domain conflict) or 412 (If-Match failed). Include the current
/// server value when you have it; otherwise the engine refetches.
final class Conflicted extends SendResult {
  /// Creates a conflict result.
  const Conflicted({this.serverValue, this.hasServerValue = false, this.version});

  /// Convenience for a conflict response that carries the server entity.
  const Conflicted.withValue(Object? value, {Version? version})
      : this(serverValue: value, hasServerValue: true, version: version);

  /// The current server entity, if the response carried it.
  final Object? serverValue;
  /// Whether [serverValue] was returned (distinguishes null from absent).
  final bool hasServerValue;
  /// Current server version of the entity.
  final Version? version;
}

/// Permanent rejection (4xx validation, 403...). Not retried.
final class Rejected extends SendResult {
  /// Creates a rejection.
  const Rejected(this.error);

  /// Why the server rejected the operation.
  final ErrorInfo error;
}

/// Temporary failure the server reported explicitly (429, 503, or 409 "an
/// operation with this Idempotency-Key is still processing").
final class Retryable extends SendResult {
  /// Creates a retryable failure.
  const Retryable(this.error, {this.retryAfter});

  /// What went wrong.
  final ErrorInfo error;
  /// From the `Retry-After` header, if sent; overrides the backoff delay.
  final Duration? retryAfter;
}
