# offline_cache_sync

Offline-first data engine for Dart and Flutter. It keeps a **durable outbox** and a
**stale-while-revalidate cache** consistent with each other:

- **Pending edits survive restarts.** Optimistic state is derived from a durable op log (`view = fold(serverBase, pendingOps)`), not from in-memory closures.
- **Refreshes never clobber pending edits.** A background refresh replaces the server base; your unsynced writes stay folded on top.
- **Eviction never loses them.** LRU/TTL/size eviction skips entities with pending, failed or parked ops, pinned records, and records a screen is watching.
- **Retries never duplicate them** (when your backend honours `Idempotency-Key`). The key is minted once, persisted with the op, and reused on every retry and after a crash.

The core is pure Dart with **zero runtime dependencies**: no Flutter, Dio, Hive, Isar, Drift or SQLite. Storage and transport are ports you plug in.

> Status: 0.1.0, the API may still change before 1.0. The SQLite/Drift stores, http/Dio helpers and Flutter widgets ship as separate `offline_cache_sync_*` packages.

## When NOT to use this

- **You control the backend and can run a sync service:** use [PowerSync](https://pub.dev/packages/powersync).
- **GraphQL:** use [graphql](https://pub.dev/packages/graphql) or [ferry](https://pub.dev/packages/ferry). They have normalized caches.
- **Plain HTTP response caching only:** use [dio_cache_interceptor](https://pub.dev/packages/dio_cache_interceptor) or [http_cache_client](https://pub.dev/packages/http_cache_client).

`offline_cache_sync` is for REST/RPC backends you do **not** control, in an app where offline edits must stay durable and cached reads must agree with them.

## Quick start

```dart
import 'package:offline_cache_sync/offline_cache_sync.dart';

// 1. Describe the entity: key, codec, how to fetch it, how to send ops.
final todos = Resource<String, Map<String, Object?>>(
  type: 'todo',
  codec: JsonCodecOf((j) => Map<String, Object?>.from(j! as Map), (v) => v),
  remote: MyTodoApi(),   // implements RemoteSource<String>
  sender: MyTodoApi(),   // implements MutationSender
  freshness: const FreshnessWindows(
    staleAfter: Duration(minutes: 1),  // fresh: served with no request
    expireAfter: Duration(days: 1),    // stale: served + revalidated in the background
  ),
  conflicts: ConflictStrategies.fieldMerge(),
);

// 2. One engine per account. InMemoryLocalStore is for tests; use a persistent store in apps.
final engine = await OfflineEngine.open(store: InMemoryLocalStore(), resources: [todos]);
final repo = engine.repository(todos);

// 3. Read: fresh / stale-while-revalidate / fetch / stale-if-error / stale-if-offline.
final snap = await repo.get('42');
print('${snap.value} ${snap.freshness} pending=${snap.pendingWrites}');

// 4. Write: atomically enqueued, visible immediately, sent when possible.
final op = await repo.mutate('42', const SetFields({'done': true}));
print(await op.done); // OpOutcome.succeeded, conflicted, failed, ...

// 5. Watch: one stream carries optimistic writes, acknowledgements and refreshes.
repo.watch('42').listen((s) => print(s.value));
```

### Typed mutations

A mutation is a small class with a **pure reducer**. The reducer *is* the optimistic update, so it survives restarts:

```dart
final class Rename extends Mutation<User> {
  const Rename(this.name);
  factory Rename.fromJson(Object? j) => Rename((j! as Map)['name'] as String);
  final String name;

  @override String get kind => 'user.rename';
  @override Object? toJson() => {'name': name};
  @override User? apply(User? u) => u?.copyWith(name: name);
}

// Register the decoder so the op can be rebuilt after a restart:
Resource<String, User>(..., mutations: {'user.rename': Rename.fromJson});
```

Built-ins: `SetFields` (for `Map<String, Object?>` entities), `Replace<T>` and `Delete<T>`.

### Implementing the transport

```dart
final class MyTodoApi implements RemoteSource<String>, MutationSender {
  @override
  Future<FetchResult> fetch(String id, FetchContext ctx) async {
    final res = await http.get(uri(id), headers: {
      if (ctx.validators?.etag != null) 'If-None-Match': ctx.validators!.etag!,
    });
    if (res.statusCode == 304) return NotModified();
    if (res.statusCode == 404) return const Gone();
    if (res.statusCode >= 500) throw HttpException('${res.statusCode}'); // throw => stale-if-error
    return Fetched(jsonDecode(res.body),
        version: Version(etag: res.headers['etag']),
        validators: Validators(etag: res.headers['etag']));
  }

  @override
  Future<SendResult> send(Operation op, SendContext ctx) async {
    final res = await http.patch(uri(op.entity.id), body: jsonEncode(op.payload), headers: {
      if (ctx.idempotencyKey != null) 'Idempotency-Key': ctx.idempotencyKey!.value,
      if (ctx.baseVersion?.etag != null) 'If-Match': ctx.baseVersion!.etag!,
    });
    return switch (res.statusCode) {
      >= 200 && < 300 => Accepted.withValue(jsonDecode(res.body), version: Version(etag: res.headers['etag'])),
      409 || 412 => Conflicted.withValue(jsonDecode(res.body)),
      429 || 503 => Retryable(ErrorInfo(code: 'http_${res.statusCode}', httpStatus: res.statusCode)),
      _ => Rejected(ErrorInfo(code: 'http_${res.statusCode}', httpStatus: res.statusCode)),
    };
  }
}
```

Throwing from `send` means "ambiguous": the engine retries with the same idempotency key, or, for `Delivery.atMostOnce` without keys, parks the op as `failed`.

## Freshness

| State | Condition (age since the last 200/304) | Behaviour |
| --- | --- | --- |
| fresh | `< staleAfter` | served, no request |
| stale | `< expireAfter` | served immediately, revalidated in the background (deduplicated) |
| expired | `>= expireAfter` | blocking fetch |
| stale-if-error | expired, fetch threw, `< expireAfter + staleIfError` | expired value served, `servedOnError` |
| stale-if-offline | expired, offline, `< expireAfter + staleIfOffline` (default unlimited) | expired value served, `servedOffline`, no request |

## Guarantees and how they are tested

`offline_cache_sync_test` runs randomized command sequences with injected network failures and **crashes at random transactions**, restarting the engine on the same store. After every step it checks these invariants:

1. No acknowledged write is ever lost.
2. At most one op per entity is in flight.
3. Each entity's ops reach the server in enqueue order, each applied once.
4. Every attempt of an op carries the same idempotency key.
5. Read-your-writes: until an op is acknowledged, reads reflect it.
6. At the end, the server and the local view converge to a reference model.

Known limitation: after an *ambiguous* send (timeout or crash in flight), a refresh may already contain the op's effect while the op is still pending. Until the resend is acknowledged, a non-idempotent reducer (for example an increment) can briefly show the effect twice. Idempotent writes (set, replace, delete) are unaffected.

## Crash recovery

Ops are marked `inFlight` in a committed transaction *before* they are sent. On startup, any `inFlight` op left by a dead process is resent with its original idempotency key. Without a key, it follows `Delivery`: resent for `atLeastOnce`, or parked as `failed(ambiguous)` for `atMostOnce`. Ops of a kind the app no longer knows are dead-lettered, never dropped.

## Storage adapters

Implement `LocalStore` from `package:offline_cache_sync/adapter_api.dart` and run the conformance suite:

```dart
import 'package:offline_cache_sync_test/offline_cache_sync_test.dart';

void main() => runLocalStoreConformance('my store', () async => MyStore.inMemory());
```

## License

MIT
