// Runnable with `dart run example/example.dart`.
//
// EngineHarness wires an OfflineEngine to deterministic fakes, so you can
// script offline edits, crashes and restarts in tests without a real server.
import 'package:offline_cache_sync/offline_cache_sync.dart';
import 'package:offline_cache_sync_test/offline_cache_sync_test.dart';

Future<void> main() async {
  final h = await EngineHarness.create();
  h.server.seed('1', {'title': 'Write docs', 'done': false});

  print('read:      ${(await h.items.get('1')).value}');

  h.connectivity.goOffline();
  await h.items.mutate('1', const SetFields({'done': true}));
  print('offline:   ${(await h.items.peek('1')).value} (pending writes: ${(await h.items.peek('1')).pendingWrites})');

  await h.crashAndRestart();
  print('restarted: pending writes: ${(await h.items.peek('1')).pendingWrites}');

  h.connectivity.goOnline();
  await h.advance(const Duration(minutes: 10));
  print('server:    ${h.server.valueOf('1')}');

  await h.dispose();
}
