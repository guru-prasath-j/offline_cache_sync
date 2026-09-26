// Runnable with `dart run example/example.dart`.
//
// Shows the point of a durable store: an edit made while offline is still
// queued after the app is killed, and syncs on the next launch.
import 'dart:io';

import 'package:offline_cache_sync/offline_cache_sync.dart';
import 'package:offline_cache_sync_sqlite/offline_cache_sync_sqlite.dart';
import 'package:sqlite3/sqlite3.dart';

/// A toy backend. Replace with your http/Dio calls.
final class TodoApi implements RemoteSource<String>, MutationSender {
  final Map<String, Map<String, Object?>> _rows = {
    '1': {'title': 'Write docs', 'done': false},
  };

  @override
  Future<FetchResult> fetch(String id, FetchContext context) async {
    final row = _rows[id];
    return row == null ? const Gone() : Fetched(Map<String, Object?>.of(row));
  }

  @override
  Future<SendResult> send(Operation op, SendContext context) async {
    final row = Map<String, Object?>.of(_rows[op.entity.id] ?? const {});
    row.addAll(Map<String, Object?>.from(op.payload! as Map));
    _rows[op.entity.id] = row;
    return Accepted.withValue(Map<String, Object?>.of(row));
  }
}

Future<void> main() async {
  final api = TodoApi();
  final todos = Resource<String, Map<String, Object?>>(
    type: 'todo',
    codec: JsonCodecOf((j) => Map<String, Object?>.from(j! as Map), (v) => v),
    remote: api,
    sender: api,
  );
  final dir = Directory.systemTemp.createTempSync('offline_cache_sync_example_');
  final path = '${dir.path}/offline.db';

  // First launch: read, then edit without syncing (autoDrain: false stands in
  // for "no network"), then the app is killed.
  var store = SqliteLocalStore(sqlite3.open(path));
  var engine = await OfflineEngine.open(store: store, resources: [todos], autoDrain: false);
  var repo = engine.repository(todos);
  print('read:      ${(await repo.get('1')).value}');
  await repo.mutate('1', const SetFields({'done': true}));
  print('edited:    ${(await repo.peek('1')).value} (pending writes: ${(await repo.peek('1')).pendingWrites})');
  await engine.close();
  await store.close();

  // Second launch: the queued edit was read back from SQLite and syncs.
  store = SqliteLocalStore(sqlite3.open(path));
  engine = await OfflineEngine.open(store: store, resources: [todos]);
  repo = engine.repository(todos);
  print('relaunch:  pending writes: ${(await repo.peek('1')).pendingWrites}');
  await engine.drain();
  print('synced:    ${(await repo.peek('1')).value} (pending writes: ${(await repo.peek('1')).pendingWrites})');
  await engine.close();
  await store.close();
  dir.deleteSync(recursive: true);
}
