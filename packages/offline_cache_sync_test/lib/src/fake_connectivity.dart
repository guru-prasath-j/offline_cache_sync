import 'dart:async';

import 'package:offline_cache_sync/offline_cache_sync.dart';

/// Scriptable [ConnectivityProvider].
final class FakeConnectivity implements ConnectivityProvider {
  FakeConnectivity([this._current = Reachability.online]);

  Reachability _current;
  final StreamController<Reachability> _changes = StreamController<Reachability>.broadcast(sync: true);

  @override
  Reachability get current => _current;

  @override
  Stream<Reachability> get changes => _changes.stream;

  void set(Reachability r) {
    if (r == _current) return;
    _current = r;
    _changes.add(r);
  }

  void goOffline() => set(Reachability.offline);

  void goOnline() => set(Reachability.online);
}
