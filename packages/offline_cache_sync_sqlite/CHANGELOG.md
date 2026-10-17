## 0.1.1

- Published as `flutter_offline_cache_sync_sqlite`; depends on `flutter_offline_cache_sync: ^0.1.3`. README uses the new package names. No code changes.
- Add `package:flutter_offline_cache_sync_sqlite/flutter_offline_cache_sync_sqlite.dart`, a library named after the package (same exports).

## 0.1.0

- First release: `SqliteLocalStore`, a `LocalStore` for `offline_cache_sync` on `package:sqlite3`.
- Records, the op log and engine metadata share one database, so each engine transaction is one SQLite transaction.
- WAL journaling with `synchronous = FULL` by default, schema versioning through `PRAGMA user_version`.
- Passes the `offline_cache_sync_test` LocalStore conformance suite.
