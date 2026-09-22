/// SPI for storage adapter authors (SQLite, Drift, Hive CE, ...).
///
/// Implement [LocalStore] and run the conformance suite from
/// `package:offline_cache_sync_test/conformance.dart` in your tests.
library;

export 'src/model/keys.dart';
export 'src/model/operation.dart';
export 'src/model/records.dart';
export 'src/ports/store.dart';
export 'src/store/memory_store.dart' show estimateSize;
