import 'dart:convert';

import 'package:offline_cache_sync/adapter_api.dart';
import 'package:sqlite3/common.dart' show Row;

// Timestamps are stored as microseconds since the epoch and read back as UTC
// DateTimes: the same instant, full precision, and sortable in SQL.

int encodeTime(DateTime t) => t.microsecondsSinceEpoch;

DateTime decodeTime(Object? v) => DateTime.fromMicrosecondsSinceEpoch(v! as int, isUtc: true);

DateTime? decodeTimeOrNull(Object? v) => v == null ? null : decodeTime(v);

String? encodeJson(Object? value) => value == null ? null : jsonEncode(value);

Object? decodeJson(Object? v) => v == null ? null : jsonDecode(v as String);

int encodeBool(bool b) => b ? 1 : 0;

bool decodeBool(Object? v) => v == 1;

const String recordColumns = 'key, payload, tombstone, invalidated, server_version, validators, fetched_at, '
    'http_max_age, last_accessed_at, access_count, size_bytes, priority, pinned';

List<Object?> recordValues(StoredRecord r) => [
      r.key.value,
      encodeJson(r.payload),
      encodeBool(r.tombstone),
      encodeBool(r.invalidated),
      encodeJson(r.serverVersion?.toJson()),
      encodeJson(r.validators?.toJson()),
      encodeTime(r.fetchedAt),
      r.httpMaxAge?.inMicroseconds,
      encodeTime(r.lastAccessedAt),
      r.accessCount,
      r.sizeBytes,
      r.priority,
      encodeBool(r.pinned),
    ];

StoredRecord recordFromRow(Row row) {
  final maxAge = row['http_max_age'] as int?;
  return StoredRecord(
    key: RecordKey.parse(row['key'] as String),
    payload: decodeJson(row['payload']),
    tombstone: decodeBool(row['tombstone']),
    invalidated: decodeBool(row['invalidated']),
    serverVersion: Version.fromJson(decodeJson(row['server_version'])),
    validators: Validators.fromJson(decodeJson(row['validators'])),
    fetchedAt: decodeTime(row['fetched_at']),
    httpMaxAge: maxAge == null ? null : Duration(microseconds: maxAge),
    lastAccessedAt: decodeTime(row['last_accessed_at']),
    accessCount: row['access_count'] as int,
    sizeBytes: row['size_bytes'] as int,
    priority: row['priority'] as int,
    pinned: decodeBool(row['pinned']),
  );
}

const String opColumns = 'id, seq, entity, kind, payload, idempotency_key, base_version, local_version, '
    'depends_on, status, attempts, ever_sent, created_at, updated_at, next_attempt_at, lease_owner, '
    'in_flight_since, last_error, server_version';

List<Object?> opValues(Operation op) => [
      op.id.value,
      op.seq,
      op.entity.value,
      op.kind,
      encodeJson(op.payload),
      op.idempotencyKey?.value,
      encodeJson(op.baseVersion?.toJson()),
      op.localVersion,
      jsonEncode([for (final d in op.dependsOn) d.value]),
      op.status.name,
      op.attempts,
      encodeBool(op.everSent),
      encodeTime(op.createdAt),
      encodeTime(op.updatedAt),
      op.nextAttemptAt == null ? null : encodeTime(op.nextAttemptAt!),
      op.leaseOwner,
      op.inFlightSince == null ? null : encodeTime(op.inFlightSince!),
      encodeJson(op.lastError?.toJson()),
      encodeJson(op.serverVersion?.toJson()),
    ];

Operation opFromRow(Row row) {
  final idem = row['idempotency_key'] as String?;
  return Operation(
    id: OpId(row['id'] as String),
    seq: row['seq'] as int,
    entity: RecordKey.parse(row['entity'] as String),
    kind: row['kind'] as String,
    payload: decodeJson(row['payload']),
    idempotencyKey: idem == null ? null : IdempotencyKey(idem),
    baseVersion: Version.fromJson(decodeJson(row['base_version'])),
    localVersion: row['local_version'] as int,
    dependsOn: [for (final d in decodeJson(row['depends_on'])! as List) OpId(d as String)],
    status: OpStatus.values.byName(row['status'] as String),
    attempts: row['attempts'] as int,
    everSent: decodeBool(row['ever_sent']),
    createdAt: decodeTime(row['created_at']),
    updatedAt: decodeTime(row['updated_at']),
    nextAttemptAt: decodeTimeOrNull(row['next_attempt_at']),
    leaseOwner: row['lease_owner'] as String?,
    inFlightSince: decodeTimeOrNull(row['in_flight_since']),
    lastError: ErrorInfo.fromJson(decodeJson(row['last_error'])),
    serverVersion: Version.fromJson(decodeJson(row['server_version'])),
  );
}

/// `(?, ?, ?)` with [n] placeholders.
String placeholders(int n) => '(${List.filled(n, '?').join(', ')})';
