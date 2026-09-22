import 'dart:async';

/// Minimal async mutex: callers run one at a time, in arrival order.
final class Mutex {
  Future<void> _last = Future<void>.value();

  Future<R> protect<R>(Future<R> Function() body) {
    final previous = _last;
    final done = Completer<void>();
    _last = done.future;
    return previous.then((_) => body()).whenComplete(done.complete);
  }
}
