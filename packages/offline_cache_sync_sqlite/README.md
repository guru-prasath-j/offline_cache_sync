# offline_cache_sync_sqlite

SQLite storage for [`offline_cache_sync`](https://pub.dev/packages/offline_cache_sync). Records, the outbox and engine metadata live in one SQLite database, so an edit queued while offline is still there after the app is killed, and every engine transaction ("write locally and enqueue") commits or rolls back as a whole.

Built on [`package:sqlite3`](https://pub.dev/packages/sqlite3), which bundles SQLite through build hooks: no `sqlite3_flutter_libs`, no native setup.

## Install

```shell
dart pub add offline_cache_sync offline_cache_sync_sqlite sqlite3
```

## Use

```dart
import 'package:offline_cache_sync/offline_cache_sync.dart';
import 'package:offline_cache_sync_sqlite/offline_cache_sync_sqlite.dart';
import 'package:sqlite3/sqlite3.dart';

final store = SqliteLocalStore(sqlite3.open('${appSupportDir.path}/offline.db'));
final engine = await OfflineEngine.open(store: store, resources: [todos]);

// On shutdown. The engine does not close the store.
await engine.close();
await store.close();
```

In Flutter, get a writable directory from [`path_provider`](https://pub.dev/packages/path_provider)'s `getApplicationSupportDirectory()`.

The store works with any `CommonDatabase`, including WebAssembly databases from `package:sqlite3/wasm.dart` on the web.

## Behaviour

- **Atomic.** Each `LocalStore.transaction` is one `BEGIN IMMEDIATE ... COMMIT`. If the body throws, it rolls back and nothing it wrote is visible.
- **Serialized.** Transactions run one at a time. Don't start a transaction on the same store from inside a transaction body: it waits for itself.
- **Durable by default.** For file databases the store enables WAL journaling with `synchronous = FULL`. Pass `configure: false` to keep your own pragmas.
- **Shares your database.** Tables are prefixed `ocs_`, so you can pass a database that also holds your own tables. Pass `closeDatabase: false` if you close it yourself.
- **Versioned schema.** The schema version is kept in `PRAGMA user_version`. Opening a file written by a newer version of this package throws instead of corrupting it.
- **JSON values.** Payloads, versions and metadata are stored as JSON text. Timestamps are stored as microseconds and read back as UTC `DateTime`s.

## Tested

The package runs the `LocalStore` conformance suite from [`offline_cache_sync_test`](https://pub.dev/packages/offline_cache_sync_test) (atomicity, serialization, op ordering, eviction rules), plus round trips of every field and persistence across reopening a file database.
