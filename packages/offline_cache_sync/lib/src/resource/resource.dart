import '../model/keys.dart';
import '../model/operation.dart';
import '../model/records.dart';
import '../policy/freshness.dart';
import '../ports/remote.dart';
import '../ports/runtime.dart';
import 'conflicts.dart';
import 'mutation.dart';

/// What to do when a send's outcome is ambiguous (timeout after the request
/// was written, or a crash while the op was in flight) and the op carries NO
/// idempotency key. With a key, the engine always resends: the server
/// deduplicates.
enum Delivery {
  /// Resend. May duplicate the effect if the backend is not idempotent.
  atLeastOnce,

  /// Do not resend. Mark the op `failed` with an `ambiguous` error and let the
  /// app/user verify.
  atMostOnce,
}

/// Definition of one kind of entity: key, codec, remote access, mutations and
/// policies. Create one per entity type and register it with the engine.
final class Resource<K, T> {
  Resource({
    required this.type,
    required this.codec,
    required this.remote,
    required this.sender,
    Map<String, MutationDecoder<T>> mutations = const {},
    this.freshness = const FreshnessWindows(),
    ConflictResolver<T>? conflicts,
    this.delivery = Delivery.atLeastOnce,
    this.useIdempotencyKeys = true,
    String Function(K id)? idToString,
    K Function(String id)? idFromString,
  })  : conflicts = conflicts ?? ConflictStrategies.park<T>(),
        _idToString = idToString,
        _idFromString = idFromString {
    if (type.isEmpty || type.contains(':')) {
      throw ArgumentError.value(type, 'type', 'must be non-empty and not contain ":"');
    }
    for (final k in mutations.keys) {
      if (k.startsWith(r'$')) {
        throw ArgumentError.value(k, 'mutations', r'kinds starting with "$" are reserved');
      }
    }
    _decoders = {
      ...mutations,
      Replace.kindName: (json) => Replace<T>.fromJson(json, codec),
      Delete.kindName: (_) => Delete<T>(),
      if (T == _JsonMap) SetFields.kindName: (json) => SetFields.fromJson(json) as Mutation<T>,
    };
  }

  /// Resource type used in keys (`user` in `user:123`). No `:` allowed.
  final String type;
  final Codec<T> codec;
  final RemoteSource<K> remote;
  final MutationSender sender;
  final FreshnessWindows freshness;
  final ConflictResolver<T> conflicts;
  final Delivery delivery;

  /// Mint an idempotency key for every op (recommended).
  final bool useIdempotencyKeys;

  final String Function(K id)? _idToString;
  final K Function(String id)? _idFromString;
  late final Map<String, MutationDecoder<T>> _decoders;

  RecordKey keyOf(K id) => RecordKey(type, _idToString?.call(id) ?? id.toString());

  K idOf(RecordKey key) {
    final f = _idFromString;
    if (f != null) return f(key.id);
    if (key.id is K) return key.id as K;
    throw StateError('Resource "$type" needs idFromString to parse ids of type $K');
  }

  bool knowsKind(String kind) => _decoders.containsKey(kind);

  /// Rebuilds a mutation from its frozen form. Throws if [kind] is unknown.
  Mutation<T> decodeMutation(String kind, Object? json) {
    final d = _decoders[kind];
    if (d == null) throw StateError('Unknown mutation kind "$kind" for resource "$type"');
    return d(json);
  }

  T? decode(Object? json) => json == null ? null : codec.decode(json);

  // ---- engine helpers ------------------------------------------------------
  // The engine holds resources as Resource<Object?, Object?>. Anything that
  // must CONSTRUCT a generic object with the real T (a Conflict<T>, a
  // Replace<T>) happens here, where T is the resource's real type.

  /// Folds the ops `(kind, payload)` over an encoded base. Returns the new
  /// encoded value, or null when the entity ends up deleted.
  Object? foldEncoded(Object? basePayload, Iterable<(String, Object?)> ops) {
    T? value = decode(basePayload);
    for (final (kind, payload) in ops) {
      value = decodeMutation(kind, payload).apply(value);
    }
    return value == null ? null : codec.encode(value);
  }

  /// Builds a typed [Conflict] and asks [conflicts] to resolve it.
  Future<Resolution<T>> resolveConflict({
    required Operation op,
    required Object? basePayload,
    required Object? serverPayload,
    required bool serverDeleted,
    Version? serverVersion,
  }) async {
    final base = decode(basePayload);
    final local = decodeMutation(op.kind, op.payload).apply(base);
    final server = serverDeleted ? null : decode(serverPayload);
    return conflicts.resolve(Conflict<T>(
      op: op,
      base: base,
      local: local,
      server: server,
      serverDeleted: serverDeleted,
      serverVersion: serverVersion,
    ));
  }

  /// Encodes a value produced by [MergeWith] for this resource.
  Object? encodeUntyped(Object? value) => codec.encode(value as T);

  /// Fetches by key (engine use).
  Future<FetchResult> fetchByKey(RecordKey key, FetchContext context) => remote.fetch(idOf(key), context);
}

typedef _JsonMap = Map<String, Object?>;
