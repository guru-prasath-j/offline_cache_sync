## 0.1.0

- First release: `SqliteLocalStore`, a `LocalStore` for `offline_cache_sync` on `package:sqlite3`.
- Records, the op log and engine metadata share one database, so each engine transaction is one SQLite transaction.
- WAL journaling with `synchronous = FULL` by default, schema versioning through `PRAGMA user_version`.
- Passes the `offline_cache_sync_test` LocalStore conformance suite.
