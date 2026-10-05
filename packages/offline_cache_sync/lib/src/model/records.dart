import 'keys.dart';

/// Opaque server version of an entity. Prefer [revision] or a strong [etag];
/// [updatedAt] is the weakest signal (clock skew, millisecond collisions).
final class Version {
  /// Creates a version. Supply whichever signals your server provides.
  const Version({this.etag, this.revision, this.updatedAt});

  /// Entity tag, compared for equality only.
  final String? etag;
  /// Monotonic revision number, the strongest ordering signal.
  final int? revision;
  /// Server modification time, the weakest ordering signal.
  final DateTime? updatedAt;

  /// Returns true when [other] is known to be strictly older than this.
  /// Returns false when the versions cannot be ordered (e.g. ETags only).
  bool isNewerThan(Version? other) {
    if (other == null) return false;
    final r = revision, o = other.revision;
    if (r != null && o != null) return r > o;
    final u = updatedAt, ou = other.updatedAt;
    if (u != null && ou != null) return u.isAfter(ou);
    return false;
  }

  /// Returns true when [other] is known to be strictly newer than this.
  bool isOlderThan(Version? other) => other != null && other.isNewerThan(this);

  /// Converts the version to a JSON-compatible map.
  Map<String, Object?> toJson() => {
        if (etag != null) 'etag': etag,
        if (revision != null) 'revision': revision,
        if (updatedAt != null) 'updatedAt': updatedAt!.toUtc().toIso8601String(),
      };

  /// Reads a version written by [toJson]; null for anything else.
  static Version? fromJson(Object? json) {
    if (json is! Map) return null;
    final u = json['updatedAt'];
    return Version(
      etag: json['etag'] as String?,
      revision: json['revision'] as int?,
      updatedAt: u is String ? DateTime.parse(u) : null,
    );
  }

  @override
  bool operator ==(Object other) =>
      other is Version && other.etag == etag && other.revision == revision && other.updatedAt == updatedAt;

  @override
  int get hashCode => Object.hash(etag, revision, updatedAt);

  @override
  String toString() => 'Version(${toJson()})';
}

/// HTTP validators used for conditional requests (304 Not Modified).
final class Validators {
  /// Creates validators.
  const Validators({this.etag, this.lastModified});

  /// Value of the `ETag` response header, sent back as `If-None-Match`.
  final String? etag;
  /// Value of the `Last-Modified` header, sent back as `If-Modified-Since`.
  final String? lastModified;

  /// Whether neither validator is present.
  bool get isEmpty => etag == null && lastModified == null;

  /// Converts the validators to a JSON-compatible map.
  Map<String, Object?> toJson() => {
        if (etag != null) 'etag': etag,
        if (lastModified != null) 'lastModified': lastModified,
      };

  /// Reads validators written by [toJson]; null for anything else.
  static Validators? fromJson(Object? json) {
    if (json is! Map) return null;
    return Validators(etag: json['etag'] as String?, lastModified: json['lastModified'] as String?);
  }
}

/// A server-confirmed base value plus the metadata the engine needs for
/// freshness and retention. Immutable.
final class StoredRecord {
  /// Creates a record.
  const StoredRecord({
    required this.key,
    required this.payload,
    required this.fetchedAt,
    required this.lastAccessedAt,
    this.tombstone = false,
    this.invalidated = false,
    this.serverVersion,
    this.validators,
    this.httpMaxAge,
    this.accessCount = 0,
    this.sizeBytes = 0,
    this.priority = 0,
    this.pinned = false,
  });

  /// Which entity this is.
  final RecordKey key;

  /// JSON-compatible encoded value (the output of `Codec.encode`).
  /// Null together with [tombstone] means "deleted on the server".
  final Object? payload;

  /// True when the server says the entity does not exist (404/410) or it was
  /// deleted by an accepted operation.
  final bool tombstone;

  /// True when the value was derived locally (e.g. an accepted write without a
  /// response body) and should be revalidated on the next read.
  final bool invalidated;

  /// Server version of [payload], if known.
  final Version? serverVersion;
  /// Validators for the next conditional fetch.
  final Validators? validators;

  /// Last time the server confirmed this value (200 or 304).
  final DateTime fetchedAt;

  /// `Cache-Control: max-age` sent by the server, if any.
  final Duration? httpMaxAge;

  /// Last time the app read this record; drives LRU eviction.
  final DateTime lastAccessedAt;
  /// How many times the app read this record.
  final int accessCount;
  /// Approximate encoded size, counted against `RetentionPolicy.maxBytes`.
  final int sizeBytes;

  /// Retention priority: higher values are kept longer.
  final int priority;

  /// Explicitly pinned by the app ("available offline"). Never evicted.
  final bool pinned;

  /// Returns a copy with the given fields replaced. Nullable fields can be
  /// set to null explicitly.
  StoredRecord copyWith({
    Object? payload = _unset,
    bool? tombstone,
    bool? invalidated,
    Object? serverVersion = _unset,
    Object? validators = _unset,
    DateTime? fetchedAt,
    Object? httpMaxAge = _unset,
    DateTime? lastAccessedAt,
    int? accessCount,
    int? sizeBytes,
    int? priority,
    bool? pinned,
  }) {
    return StoredRecord(
      key: key,
      payload: identical(payload, _unset) ? this.payload : payload,
      tombstone: tombstone ?? this.tombstone,
      invalidated: invalidated ?? this.invalidated,
      serverVersion: identical(serverVersion, _unset) ? this.serverVersion : serverVersion as Version?,
      validators: identical(validators, _unset) ? this.validators : validators as Validators?,
      fetchedAt: fetchedAt ?? this.fetchedAt,
      httpMaxAge: identical(httpMaxAge, _unset) ? this.httpMaxAge : httpMaxAge as Duration?,
      lastAccessedAt: lastAccessedAt ?? this.lastAccessedAt,
      accessCount: accessCount ?? this.accessCount,
      sizeBytes: sizeBytes ?? this.sizeBytes,
      priority: priority ?? this.priority,
      pinned: pinned ?? this.pinned,
    );
  }
}

const Object _unset = Object();
