// Runnable with `dart run example/example.dart`: an in-memory "server" stands
// in for your REST API so the example has no dependencies.
import 'package:offline_cache_sync/offline_cache_sync.dart';

/// A toy backend. Replace with your http/Dio calls.
final class TodoApi implements RemoteSource<String>, MutationSender {
  final Map<String, Map<String, Object?>> _rows = {
    '1': {'title': 'Write docs', 'done': false},
  };
  var online = true;

  @override
  Future<FetchResult> fetch(String id, FetchContext context) async {
    if (!online) throw StateError('network down');
    final row = _rows[id];
    return row == null ? const Gone() : Fetched(Map<String, Object?>.of(row));
  }

  @override
  Future<SendResult> send(Operation op, SendContext context) async {
    if (!online) throw StateError('network down'); // ambiguous => retried with the same key
    final id = op.entity.id;
    switch (op.kind) {
      case r'$set':
        final row = Map<String, Object?>.of(_rows[id] ?? const {});
        row.addAll(Map<String, Object?>.from(op.payload! as Map));
        _rows[id] = row;
        return Accepted.withValue(Map<String, Object?>.of(row));
      case r'$delete':
        _rows.remove(id);
        return const Accepted(deleted: true);
      default:
        return Rejected(ErrorInfo(code: 'unsupported ${op.kind}'));
    }
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
  final engine = await OfflineEngine.open(
    store: InMemoryLocalStore(),
    resources: [todos],
    retry: ExponentialBackoff(base: const Duration(milliseconds: 50)),
  );
  final repo = engine.repository(todos);

  print('read:      ${(await repo.get('1')).value}');

  api.online = false;
  final op = await repo.mutate('1', const SetFields({'done': true}));
  final local = await repo.peek('1');
  print('offline:   ${local.value} (pending writes: ${local.pendingWrites})');

  api.online = true;
  print('outcome:   ${await op.done}');
  print('synced:    ${(await repo.peek('1')).value} (pending writes: ${(await repo.peek('1')).pendingWrites})');
  print('metrics:   ${engine.metrics.toJson()}');
  await engine.close();
}
