import 'package:sqlite3/common.dart' show CommonDatabase;

/// Current schema version, stored in `PRAGMA user_version`.
const int schemaVersion = 1;

/// Statuses that end an op's lifecycle ([OpStatus.isTerminal]). Every other
/// status blocks later ops of the same entity ([OpStatus.blocksEntity]).
const String terminalStatuses = "('succeeded', 'cancelled')";

/// Creates or upgrades the tables. Throws if the file was written by a newer
/// version of this package.
void migrate(CommonDatabase db) {
  final current = db.userVersion;
  if (current == schemaVersion) return;
  if (current > schemaVersion) {
    throw StateError(
      'Database schema version $current is newer than this version of '
      'offline_cache_sync_sqlite supports ($schemaVersion).',
    );
  }
  db.execute('BEGIN IMMEDIATE');
  try {
    if (current < 1) _createV1(db);
    db.userVersion = schemaVersion;
    db.execute('COMMIT');
  } catch (_) {
    if (!db.autocommit) db.execute('ROLLBACK');
    rethrow;
  }
}

void _createV1(CommonDatabase db) {
  db.execute('''
    CREATE TABLE ocs_records (
      key TEXT NOT NULL PRIMARY KEY,
      payload TEXT,
      tombstone INTEGER NOT NULL,
      invalidated INTEGER NOT NULL,
      server_version TEXT,
      validators TEXT,
      fetched_at INTEGER NOT NULL,
      http_max_age INTEGER,
      last_accessed_at INTEGER NOT NULL,
      access_count INTEGER NOT NULL,
      size_bytes INTEGER NOT NULL,
      priority INTEGER NOT NULL,
      pinned INTEGER NOT NULL
    )''');
  db.execute('CREATE INDEX ocs_records_lru ON ocs_records (pinned, last_accessed_at)');
  db.execute('''
    CREATE TABLE ocs_ops (
      id TEXT NOT NULL PRIMARY KEY,
      seq INTEGER NOT NULL,
      entity TEXT NOT NULL,
      kind TEXT NOT NULL,
      payload TEXT,
      idempotency_key TEXT,
      base_version TEXT,
      local_version INTEGER NOT NULL,
      depends_on TEXT NOT NULL,
      status TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      ever_sent INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      next_attempt_at INTEGER,
      lease_owner TEXT,
      in_flight_since INTEGER,
      last_error TEXT,
      server_version TEXT
    )''');
  db.execute('CREATE INDEX ocs_ops_entity ON ocs_ops (entity, seq)');
  db.execute('CREATE INDEX ocs_ops_status ON ocs_ops (status, seq)');
  db.execute('CREATE INDEX ocs_ops_seq ON ocs_ops (seq)');
  db.execute('''
    CREATE TABLE ocs_meta (
      key TEXT NOT NULL PRIMARY KEY,
      value TEXT NOT NULL
    )''');
}
