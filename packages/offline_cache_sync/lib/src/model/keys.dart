import 'dart:math';

/// Identifies one entity: `<type>:<id>`, e.g. `user:123`.
///
/// Extension type over [String], so it is free at runtime and has value
/// equality (usable as a map key).
extension type const RecordKey._(String value) {
  /// Builds a key from a resource [type] and an entity [id].
  ///
  /// [type] must not contain `:`; ids may.
  factory RecordKey(String type, String id) {
    if (type.isEmpty || type.contains(':')) {
      throw ArgumentError.value(type, 'type', 'must be non-empty and not contain ":"');
    }
    return RecordKey._('$type:$id');
  }

  /// Parses a stored key back. Throws on malformed input.
  factory RecordKey.parse(String raw) {
    final i = raw.indexOf(':');
    if (i <= 0) throw FormatException('Not a RecordKey', raw);
    return RecordKey._(raw);
  }

  /// The resource type part.
  String get type => value.substring(0, value.indexOf(':'));

  /// The entity id part.
  String get id => value.substring(value.indexOf(':') + 1);
}

/// Local identity of one queued operation.
extension type const OpId(String value) {}

/// Key sent as the `Idempotency-Key` header. Minted once per operation.
extension type const IdempotencyKey(String value) {}

/// Generates RFC 4122 version-4 UUIDs without any package dependency.
final class UuidV4 {
  UuidV4([Random? random]) : _random = random ?? Random.secure();

  final Random _random;

  String next() {
    final b = List<int>.generate(16, (_) => _random.nextInt(256));
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    final h = b.map((x) => x.toRadixString(16).padLeft(2, '0')).join();
    return '${h.substring(0, 8)}-${h.substring(8, 12)}-${h.substring(12, 16)}-'
        '${h.substring(16, 20)}-${h.substring(20)}';
  }
}
