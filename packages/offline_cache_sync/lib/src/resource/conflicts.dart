import 'dart:async';

import '../model/operation.dart';
import '../model/records.dart';

/// Everything a resolver needs. Conflicts are detected by the SERVER
/// (409 / 412); the client never guesses.
final class Conflict<T> {
  const Conflict({
    required this.op,
    required this.base,
    required this.local,
    required this.server,
    required this.serverDeleted,
    this.serverVersion,
  });

  final Operation op;

  /// The value the user edited against (the stored base before this op).
  final T? base;

  /// `op.apply(base)`: what the user wants.
  final T? local;

  /// The current server value (null if unknown or deleted).
  final T? server;

  final bool serverDeleted;
  final Version? serverVersion;
}

sealed class Resolution<T> {
  const Resolution();
}

/// Drop the op; the server value becomes the base.
final class TakeServer<T> extends Resolution<T> {
  const TakeServer();
}

/// Rebase the op on the server version and send it again ("client wins").
final class ResendLocal<T> extends Resolution<T> {
  const ResendLocal();
}

/// Replace the op by a full-entity write of [value] against the server
/// version. The replacement gets a NEW idempotency key (different payload).
final class MergeWith<T> extends Resolution<T> {
  const MergeWith(this.value);

  final T value;
}

/// Keep the op in `conflicted` and let the app/user decide.
final class Park<T> extends Resolution<T> {
  const Park();
}

abstract interface class ConflictResolver<T> {
  FutureOr<Resolution<T>> resolve(Conflict<T> conflict);
}

final class _FnResolver<T> implements ConflictResolver<T> {
  const _FnResolver(this._fn);

  final FutureOr<Resolution<T>> Function(Conflict<T>) _fn;

  @override
  FutureOr<Resolution<T>> resolve(Conflict<T> conflict) => _fn(conflict);
}

/// Built-in strategies.
abstract final class ConflictStrategies {
  /// The server value wins; the local op is dropped.
  static ConflictResolver<T> serverWins<T>() => _FnResolver<T>((_) => TakeServer<T>());

  /// The local op is re-sent against the new server version. Silently
  /// overwrites other writers: opt in only when that is truly intended.
  static ConflictResolver<T> clientWins<T>() => _FnResolver<T>((_) => ResendLocal<T>());

  /// Surface every conflict to the app.
  static ConflictResolver<T> park<T>() => _FnResolver<T>((_) => Park<T>());

  static ConflictResolver<T> custom<T>(FutureOr<Resolution<T>> Function(Conflict<T> conflict) fn) =>
      _FnResolver<T>(fn);

  /// Three-way merge for `Map<String, Object?>` entities.
  ///
  /// For every field: changed only locally -> local value; changed only on
  /// the server -> server value; changed on both -> [onBoth] (server by
  /// default). A server-side delete wins.
  static ConflictResolver<Map<String, Object?>> fieldMerge({
    Object? Function(String field, Object? local, Object? server)? onBoth,
  }) {
    return _FnResolver<Map<String, Object?>>((c) {
      if (c.serverDeleted) return const TakeServer();
      final base = c.base ?? const <String, Object?>{};
      final local = c.local;
      final server = c.server;
      if (local == null) return const ResendLocal(); // local delete: re-apply
      if (server == null) return const ResendLocal();
      final merged = <String, Object?>{};
      final fields = {...base.keys, ...local.keys, ...server.keys};
      for (final f in fields) {
        final b = base[f], l = local[f], s = server[f];
        final localChanged = !jsonEquals(b, l);
        final serverChanged = !jsonEquals(b, s);
        final Object? v;
        if (localChanged && !serverChanged) {
          v = l;
        } else if (!localChanged) {
          v = s;
        } else {
          v = onBoth != null ? onBoth(f, l, s) : s;
        }
        final present = localChanged && !serverChanged ? local.containsKey(f) : server.containsKey(f);
        if (present || v != null) merged[f] = v;
      }
      return MergeWith(merged);
    });
  }
}

/// Deep equality for JSON-compatible values.
bool jsonEquals(Object? a, Object? b) {
  if (identical(a, b)) return true;
  if (a is Map && b is Map) {
    if (a.length != b.length) return false;
    for (final k in a.keys) {
      if (!b.containsKey(k) || !jsonEquals(a[k], b[k])) return false;
    }
    return true;
  }
  if (a is List && b is List) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (!jsonEquals(a[i], b[i])) return false;
    }
    return true;
  }
  return a == b;
}
