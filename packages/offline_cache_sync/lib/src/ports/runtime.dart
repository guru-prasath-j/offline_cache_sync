import 'dart:async';

import '../model/keys.dart';

/// Source of time. Replace with `FakeClock` in tests.
abstract interface class Clock {
  DateTime now();
}

/// Something that can be cancelled.
abstract interface class Cancelable {
  void cancel();
}

/// Schedules callbacks. Replace with `FakeClock` (which also implements this)
/// in tests to make retries and backoff deterministic.
abstract interface class Scheduler {
  Cancelable schedule(Duration delay, void Function() callback);
}

final class SystemClock implements Clock {
  const SystemClock();

  @override
  DateTime now() => DateTime.now();
}

final class TimerScheduler implements Scheduler {
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

enum Reachability { online, offline, unknown }

/// A HINT about the network, used only for scheduling. The engine never
/// treats `online` as proof that requests will succeed.
abstract interface class ConnectivityProvider {
  Reachability get current;

  Stream<Reachability> get changes;
}

/// Default when the app supplies no provider: always try the network.
final class AssumeOnline implements ConnectivityProvider {
  const AssumeOnline();

  @override
  Reachability get current => Reachability.unknown;

  @override
  Stream<Reachability> get changes => const Stream.empty();
}

/// Draft of an operation, given to [IdempotencyProvider.mint].
final class OperationDraft {
  const OperationDraft({required this.opId, required this.entity, required this.kind});

  final OpId opId;
  final RecordKey entity;
  final String kind;
}

/// Mints the idempotency key ONCE, inside the enqueue transaction.
abstract interface class IdempotencyProvider {
  IdempotencyKey? mint(OperationDraft draft);
}

/// Random UUIDv4 keys (recommended by the IETF Idempotency-Key draft).
final class UuidIdempotency implements IdempotencyProvider {
  UuidIdempotency([UuidV4? uuid]) : _uuid = uuid ?? UuidV4();

  final UuidV4 _uuid;

  @override
  IdempotencyKey? mint(OperationDraft draft) => IdempotencyKey(_uuid.next());
}

/// No keys: use when the backend ignores them and you rely on
/// client-generated ids or `Delivery.atMostOnce`.
final class NoIdempotency implements IdempotencyProvider {
  const NoIdempotency();

  @override
  IdempotencyKey? mint(OperationDraft draft) => null;
}

/// Converts between your model and a JSON-compatible value.
abstract interface class Codec<T> {
  Object? encode(T value);

  T decode(Object? json);
}

/// A [Codec] from two functions.
final class JsonCodecOf<T> implements Codec<T> {
  const JsonCodecOf(this._fromJson, this._toJson);

  final T Function(Object? json) _fromJson;
  final Object? Function(T value) _toJson;

  @override
  T decode(Object? json) => _fromJson(json);

  @override
  Object? encode(T value) => _toJson(value);
}
