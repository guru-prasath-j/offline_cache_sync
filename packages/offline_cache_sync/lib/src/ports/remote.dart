import '../model/keys.dart';
import '../model/operation.dart';
import '../model/records.dart';

/// Passed to [RemoteSource.fetch].
final class FetchContext {
  const FetchContext({required this.key, this.validators, this.attempt = 0});

  final RecordKey key;

  /// Validators of the stored record, for `If-None-Match` /
  /// `If-Modified-Since`. Null when nothing is cached.
  final Validators? validators;

  final int attempt;
}

/// Reads one entity from the server. Knows your URLs; the engine does not.
///
/// Implementations return a [FetchResult] for protocol outcomes and THROW
/// for transport failures (timeouts, DNS, TLS, 5xx). Throwing is how the
/// engine decides stale-if-error.
abstract interface class RemoteSource<K> {
  Future<FetchResult> fetch(K id, FetchContext context);
}

sealed class FetchResult {
  const FetchResult();
}

/// 200 with a body. [value] must be JSON-compatible (it is stored as-is and
/// decoded with the resource codec).
final class Fetched extends FetchResult {
  const Fetched(this.value, {this.version, this.validators, this.maxAge});

  final Object? value;
  final Version? version;
  final Validators? validators;

  /// From `Cache-Control: max-age`, if the server sent one.
  final Duration? maxAge;
}

/// 304: the cached body is still current. Renews freshness.
final class NotModified extends FetchResult {
  const NotModified({this.validators, this.maxAge});

  final Validators? validators;
  final Duration? maxAge;
}

/// 404 / 410: the entity does not exist on the server.
final class Gone extends FetchResult {
  const Gone();
}

/// Passed to [MutationSender.send].
final class SendContext {
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
  Future<SendResult> send(Operation op, SendContext context);
}

sealed class SendResult {
  const SendResult();
}

/// 2xx. If the server returned the entity, pass it as [serverValue]
/// (JSON-compatible); it becomes the new base.
final class Accepted extends SendResult {
  const Accepted({this.serverValue, this.hasServerValue = false, this.version, this.deleted = false});

  /// Convenience for a response that carries the entity.
  const Accepted.withValue(Object? value, {Version? version})
      : this(serverValue: value, hasServerValue: true, version: version);

  final Object? serverValue;
  final bool hasServerValue;
  final Version? version;

  /// The server confirmed the entity is deleted.
  final bool deleted;
}

/// 409 (domain conflict) or 412 (If-Match failed). Include the current
/// server value when you have it; otherwise the engine refetches.
final class Conflicted extends SendResult {
  const Conflicted({this.serverValue, this.hasServerValue = false, this.version});

  const Conflicted.withValue(Object? value, {Version? version})
      : this(serverValue: value, hasServerValue: true, version: version);

  final Object? serverValue;
  final bool hasServerValue;
  final Version? version;
}

/// Permanent rejection (4xx validation, 403...). Not retried.
final class Rejected extends SendResult {
  const Rejected(this.error);

  final ErrorInfo error;
}

/// Temporary failure the server reported explicitly (429, 503, or 409 "an
/// operation with this Idempotency-Key is still processing").
final class Retryable extends SendResult {
  const Retryable(this.error, {this.retryAfter});

  final ErrorInfo error;
  final Duration? retryAfter;
}
