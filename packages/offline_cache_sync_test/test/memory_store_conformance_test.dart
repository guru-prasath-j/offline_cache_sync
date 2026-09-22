import 'package:offline_cache_sync/offline_cache_sync.dart';
import 'package:offline_cache_sync_test/offline_cache_sync_test.dart';

void main() {
  runLocalStoreConformance('InMemoryLocalStore', () async => InMemoryLocalStore());
}
