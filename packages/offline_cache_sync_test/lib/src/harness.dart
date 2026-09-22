import 'dart:math';

import 'package:offline_cache_sync/offline_cache_sync.dart';

import 'crashing_store.dart';
import 'fake_clock.dart';
import 'fake_connectivity.dart';
import 'fake_server.dart';

/// Wires an [OfflineEngine] to fakes: in-memory store behind a
/// [CrashingStore], [FakeClock], [FakeConnectivity] and [FakeServer], with one
/// `Map<String, Object?>` resource named `item`.
///
/// ```dart
/// final h = await EngineHarness.create();
/// h.server.seed('1', {'name': 'a'});
/// final s = await h.items.get('1');
/// ```
final class EngineHarness {
  EngineHarness._({
    required this.disk,
    required this.clock,
    required this.connectivity,
    required this.server,
    required this.itemResource,
    required this.seed,
    required this.retention,
    required this.leaseMode,
  });

  static Future<EngineHarness> create({
    FakeServer? server,
    FreshnessWindows freshness = const FreshnessWindows(
      staleAfter: Duration(minutes: 1),
      expireAfter: Duration(hours: 1),
      staleIfError: Duration(days: 1),
    ),
    ConflictResolver<Map<String, Object?>>? conflicts,
    Delivery delivery = Delivery.atLeastOnce,
    bool useIdempotencyKeys = true,
    RetentionPolicy retention = const RetentionPolicy.unbounded(),
    LeaseMode leaseMode = LeaseMode.primary,
    int seed = 1,
  }) async {
    final s = server ?? FakeServer();
    final resource = Resource<String, Map<String, Object?>>(
      type: 'item',
      codec: mapCodec,
      remote: s,
      sender: s,
      mutations: {Increment.kindName: Increment.fromJson},
      freshness: freshness,
      conflicts: conflicts,
      delivery: delivery,
      useIdempotencyKeys: useIdempotencyKeys,
    );
    final h = EngineHarness._(
      disk: InMemoryLocalStore(),
      clock: FakeClock(),
      connectivity: FakeConnectivity(),
      server: s,
      itemResource: resource,
      seed: seed,
      retention: retention,
      leaseMode: leaseMode,
    );
    await h._open();
    return h;
  }

  /// The "disk": survives crashes and restarts.
  final InMemoryLocalStore disk;
  final FakeClock clock;
  final FakeConnectivity connectivity;
  final FakeServer server;
  final Resource<String, Map<String, Object?>> itemResource;
  final int seed;
  final RetentionPolicy retention;
  final LeaseMode leaseMode;

  late CrashingStore store;
  late OfflineEngine engine;
  late Repository<String, Map<String, Object?>> items;
  /// Every event of every engine generation, including crash recovery at open.
  final List<SyncEvent> events = [];
  late final EventSink _sink = _ListSink(events);
  int _generation = 0;

  Future<void> _open() async {
    _generation++;
    store = CrashingStore(disk);
    engine = await OfflineEngine.open(
      store: store,
      resources: [itemResource],
      clock: clock,
      scheduler: clock,
      connectivity: connectivity,
      retry: ExponentialBackoff(random: Random(seed + _generation)),
      retention: retention,
      leaseMode: leaseMode,
      runnerId: 'runner-$_generation',
      uuid: UuidV4(Random(seed * 1000 + _generation)),
      eventSink: _sink,
    );
    items = engine.repository(itemResource);
  }

  /// Lets every started fetch/drain finish.
  Future<void> settle() => engine.settle();

  /// Advances fake time (firing retry timers) and settles.
  Future<void> advance(Duration d) async {
    clock.advance(d);
    await settle();
  }

  /// Kills the current process (store) and starts a fresh engine on the same
  /// disk, as after an app restart.
  ///
  /// The old engine is abandoned, not closed: a dead process runs no cleanup.
  /// Its store is dead, so anything it still attempts fails harmlessly.
  Future<void> crashAndRestart() async {
    store.kill();
    await _open();
    await settle();
  }

  /// Graceful restart (engine closed normally).
  Future<void> restart() async {
    await engine.close();
    await _open();
    await settle();
  }

  /// All ops still in the store (any status).
  List<Operation> get storedOps => disk.debugOps.values.toList()..sort((a, b) => a.seq.compareTo(b.seq));

  Future<void> dispose() async {
    try {
      await engine.close();
    } on Object {
      // ignore
    }
  }
}

final class _ListSink implements EventSink {
  _ListSink(this._events);

  final List<SyncEvent> _events;

  @override
  void emit(SyncEvent event) => _events.add(event);
}
