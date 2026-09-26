/// A SQLite [LocalStore] for `offline_cache_sync`, built on `package:sqlite3`.
///
/// ```dart
/// import 'package:offline_cache_sync/offline_cache_sync.dart';
/// import 'package:offline_cache_sync_sqlite/offline_cache_sync_sqlite.dart';
/// import 'package:sqlite3/sqlite3.dart';
///
/// final engine = await OfflineEngine.open(
///   store: SqliteLocalStore(sqlite3.open('offline.db')),
///   resources: [todos],
/// );
/// ```
library;

export 'src/sqlite_local_store.dart' show SqliteLocalStore;
