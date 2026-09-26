import 'package:offline_cache_sync_sqlite/offline_cache_sync_sqlite.dart';
import 'package:offline_cache_sync_test/offline_cache_sync_test.dart';
import 'package:sqlite3/sqlite3.dart' show sqlite3;

void main() {
  runLocalStoreConformance('SqliteLocalStore', () async => SqliteLocalStore(sqlite3.openInMemory()));
}
