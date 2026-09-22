/// Offline-first data engine for Dart and Flutter.
///
/// Reads return the server-confirmed base with pending local writes folded
/// on top (`fold(base, pendingOps)`); writes go through a durable outbox with
/// per-entity ordering, idempotency keys, retries and crash recovery;
/// refreshes never clobber pending edits; eviction never drops them.
library;

export 'src/engine/engine.dart' show OfflineEngine, Repository, OpHandle, OpOutcome, LeaseMode;
export 'src/engine/events.dart';
export 'src/engine/snapshot.dart';
export 'src/model/keys.dart';
export 'src/model/operation.dart';
export 'src/model/records.dart';
export 'src/policy/freshness.dart';
export 'src/policy/retention.dart';
export 'src/policy/retry.dart';
export 'src/ports/remote.dart';
export 'src/ports/runtime.dart';
export 'src/ports/store.dart';
export 'src/resource/conflicts.dart';
export 'src/resource/mutation.dart';
export 'src/resource/resource.dart';
export 'src/store/memory_store.dart' show InMemoryLocalStore;
