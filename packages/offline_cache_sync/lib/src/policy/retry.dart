import 'dart:math';

import '../model/operation.dart';

sealed class RetryDecision {
  const RetryDecision();
}

final class RetryAt extends RetryDecision {
  const RetryAt(this.delay);

  final Duration delay;
}

final class GiveUp extends RetryDecision {
  const GiveUp();
}

/// Decides what happens after a retryable failure.
abstract interface class RetryPolicy {
  /// [attempts] is the number of attempts already made (>= 1).
  RetryDecision decide({required int attempts, required ErrorInfo error, Duration? retryAfter});
}

/// Exponential backoff with "full jitter": delay = random(0, min(cap, base * 2^n)).
/// Honours a server `Retry-After` as a lower bound.
final class ExponentialBackoff implements RetryPolicy {
  ExponentialBackoff({
    this.base = const Duration(seconds: 1),
    this.cap = const Duration(minutes: 5),
    this.maxAttempts = 10,
    Random? random,
  }) : _random = random ?? Random();

  final Duration base;
  final Duration cap;

  /// After this many attempts the op is dead-lettered.
  final int maxAttempts;
  final Random _random;

  /// Upper bound of the jitter window for [attempts].
  Duration ceilingFor(int attempts) {
    final exp = min(attempts - 1, 30);
    final micros = base.inMicroseconds * pow(2, exp);
    return Duration(microseconds: min(micros.toInt(), cap.inMicroseconds));
  }

  @override
  RetryDecision decide({required int attempts, required ErrorInfo error, Duration? retryAfter}) {
    if (attempts >= maxAttempts) return const GiveUp();
    final ceiling = ceilingFor(attempts);
    var delay = Duration(microseconds: (_random.nextDouble() * ceiling.inMicroseconds).round());
    if (retryAfter != null && retryAfter > delay) delay = retryAfter;
    return RetryAt(delay);
  }
}
