import '../ports/runtime.dart';

/// A typed, durable intent to change one entity.
///
/// [apply] is the pure local reducer: it IS the optimistic update. The engine
/// re-folds `apply` over the server base on every read, so optimistic state
/// survives restarts and "rollback" is just dropping the op. No closures are
/// kept in memory.
///
/// Rules for implementers:
/// * [apply] must be pure and deterministic (no clocks, no randomness).
/// * [toJson] must be stable: the engine freezes it at enqueue time and
///   rebuilds the mutation from it after a restart via the resource's
///   registered decoder for [kind].
abstract class Mutation<T> {
  const Mutation();

  /// Registered kind, e.g. `user.rename`. Kinds starting with `$` are reserved.
  String get kind;

  /// JSON-compatible arguments.
  Object? toJson();

  /// Returns the new local value. Return null to delete the entity.
  T? apply(T? current);
}

/// Rebuilds a mutation from its frozen JSON.
typedef MutationDecoder<T> = Mutation<T> Function(Object? json);

/// Built-in: replace the whole entity. Used by conflict merges and handy for
/// "save form" style writes. Senders receive it with kind [kindName].
final class Replace<T> extends Mutation<T> {
  Replace(T value, Codec<T> codec)
      : _encoded = codec.encode(value),
        _codec = codec;

  Replace.fromJson(Object? json, Codec<T> codec)
      : _encoded = json,
        _codec = codec;

  static const kindName = r'$replace';

  final Object? _encoded;
  final Codec<T> _codec;

  @override
  String get kind => kindName;

  @override
  Object? toJson() => _encoded;

  @override
  T? apply(T? current) => _codec.decode(_encoded);
}

/// Built-in: delete the entity. Senders receive it with kind [kindName].
final class Delete<T> extends Mutation<T> {
  const Delete();

  static const kindName = r'$delete';

  @override
  String get kind => kindName;

  @override
  Object? toJson() => null;

  @override
  T? apply(T? current) => null;
}

/// Built-in for `Map<String, Object?>` entities: shallow-merge [fields]
/// (a null value removes the field). Senders receive it with kind [kindName]
/// and can map it to a PATCH.
final class SetFields extends Mutation<Map<String, Object?>> {
  const SetFields(this.fields);

  factory SetFields.fromJson(Object? json) => SetFields(Map<String, Object?>.from(json! as Map));

  static const kindName = r'$set';

  final Map<String, Object?> fields;

  @override
  String get kind => kindName;

  @override
  Object? toJson() => fields;

  @override
  Map<String, Object?>? apply(Map<String, Object?>? current) {
    final next = <String, Object?>{...?current};
    for (final MapEntry(:key, :value) in fields.entries) {
      if (value == null) {
        next.remove(key);
      } else {
        next[key] = value;
      }
    }
    return next;
  }
}
