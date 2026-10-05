import 'dart:async';

import '../model/keys.dart';

/// Source of time. Replace with `FakeClock` in tests.
abstract interface class Clock {
  /// The current time.
  DateTime now();
}

/// Something that can be cancelled.
abstract interface class Cancelable {
  /// Cancels the pending work. Safe to call more than once.
  void cancel();
}

/// Schedules callbacks. Replace with `FakeClock` (which also implements this)
/// in tests to make retries and backoff deterministic.
abstract interface class Scheduler {
  /// Runs [callback] once after [delay].
  Cancelable schedule(Duration delay, void Function() callback);
}

/// A [Clock] backed by `DateTime.now`.
final class SystemClock implements Clock {
  /// Creates a system clock.
  const SystemClock();

  @override
  DateTime now() => DateTime.now();
}

/// A [Scheduler] backed by `dart:async` timers.
final class TimerScheduler implements Scheduler {
  /// Creates a timer scheduler.
  const TimerScheduler();

  @override
  Cancelable schedule(Duration delay, void Function() callback) => _TimerCancelable(Timer(delay, callback));
}

final class _TimerCancelable implements Cancelable {
  _TimerCancelable(this._timer);

  final Timer _timer;

  @override
  void cancel() => _timer.cancel();
}

/// Network reachability as reported by a [ConnectivityProvider].
enum Reachability { online, offline, unknown }

/// A HINT about the network, used only for scheduling. The engine never
/// treats `online` as proof that requests will succeed.
abstract interface class ConnectivityProvider {
  /// The latest known state.
  Reachability get current;

  /// Emits whenever the state changes.
  Stream<Reachability> get changes;
}

/// Default when the app supplies no provider: always try the network.
final class AssumeOnline implements ConnectivityProvider {
  /// Creates the default provider.
  const AssumeOnline();

  @override
  Reachability get current => Reachability.unknown;

  @override
  Stream<Reachability> get changes => const Stream.empty();
}

/// Draft of an operation, given to [IdempotencyProvider.mint].
final class OperationDraft {
  /// Creates a draft.
  const OperationDraft({required this.opId, required this.entity, required this.kind});

  /// Id of the operation being created.
  final OpId opId;
  /// The record the operation changes.
  final RecordKey entity;
  /// The mutation kind.
  final String kind;
}

/// Mints the idempotency key ONCE, inside the enqueue transaction.
abstract interface class IdempotencyProvider {
  /// Returns the key for [draft], or null to send without one.
  IdempotencyKey? mint(OperationDraft draft);
}

/// Random UUIDv4 keys (recommended by the IETF Idempotency-Key draft).
final class UuidIdempotency implements IdempotencyProvider {
  /// Creates a provider; pass [uuid] to control randomness in tests.
  UuidIdempotency([UuidV4? uuid]) : _uuid = uuid ?? UuidV4();

  final UuidV4 _uuid;

  @override
  IdempotencyKey? mint(OperationDraft draft) => IdempotencyKey(_uuid.next());
}

/// No keys: use when the backend ignores them and you rely on
/// client-generated ids or `Delivery.atMostOnce`.
final class NoIdempotency implements IdempotencyProvider {
  /// Creates a provider that never mints keys.
  const NoIdempotency();

  @override
  IdempotencyKey? mint(OperationDraft draft) => null;
}

/// Converts between your model and a JSON-compatible value.
abstract interface class Codec<T> {
  /// Converts [value] to a JSON-compatible value.
  Object? encode(T value);

  /// Converts a JSON-compatible value back to `T`.
  T decode(Object? json);
}

/// A [Codec] from two functions.
final class JsonCodecOf<T> implements Codec<T> {
  /// Creates a codec from a `fromJson` and a `toJson` function.
  const JsonCodecOf(this._fromJson, this._toJson);

  final T Function(Object? json) _fromJson;
  final Object? Function(T value) _toJson;

  @override
  T decode(Object? json) => _fromJson(json);

  @override
  Object? encode(T value) => _toJson(value);
}
