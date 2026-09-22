import 'package:offline_cache_sync/offline_cache_sync.dart';

/// Deterministic time: implements both [Clock] and [Scheduler].
///
/// Timers fire synchronously, in time order, during [advance].
final class FakeClock implements Clock, Scheduler {
  FakeClock([DateTime? start]) : _now = start ?? DateTime.utc(2026, 1, 1, 12);

  DateTime _now;
  final List<_FakeTimer> _timers = [];
  int _seq = 0;

  @override
  DateTime now() => _now;

  @override
  Cancelable schedule(Duration delay, void Function() callback) {
    final t = _FakeTimer(_now.add(delay.isNegative ? Duration.zero : delay), _seq++, callback);
    _timers.add(t);
    return t;
  }

  /// Number of timers that have not fired or been cancelled.
  int get pendingTimers => _timers.where((t) => !t.cancelled).length;

  /// Moves time forward by [by], firing every timer that becomes due.
  void advance(Duration by) {
    final target = _now.add(by);
    while (true) {
      final due = _timers.where((t) => !t.cancelled && !t.at.isAfter(target)).toList()
        ..sort((a, b) {
          final c = a.at.compareTo(b.at);
          return c != 0 ? c : a.seq.compareTo(b.seq);
        });
      if (due.isEmpty) break;
      final t = due.first;
      _timers.remove(t);
      if (t.at.isAfter(_now)) _now = t.at;
      t.callback();
    }
    _timers.removeWhere((t) => t.cancelled);
    _now = target;
  }
}

final class _FakeTimer implements Cancelable {
  _FakeTimer(this.at, this.seq, this.callback);

  final DateTime at;
  final int seq;
  final void Function() callback;
  bool cancelled = false;

  @override
  void cancel() => cancelled = true;
}
