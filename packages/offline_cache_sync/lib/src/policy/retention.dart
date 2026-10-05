import '../model/records.dart';

/// Budgets and ordering for eviction. Freshness decides whether a value may be
/// SERVED; retention decides whether the row should still EXIST. They are
/// separate on purpose: an expired record is exactly what stale-if-offline
/// needs.
///
/// Pins are enforced by the engine and store, not by this policy: records with
/// blocking ops, pinned records and watched records never reach [score].
final class RetentionPolicy {
  /// Creates a policy. With no arguments nothing is evicted.
  const RetentionPolicy({
    this.maxEntries,
    this.maxBytes,
    this.retainFor,
    this.lowWatermark = 0.9,
    this.chunkSize = 200,
  });

  /// No limits: nothing is ever evicted automatically.
  const RetentionPolicy.unbounded() : this();

  /// Maximum number of records, or null for no limit.
  final int? maxEntries;
  /// Maximum total `StoredRecord.sizeBytes`, or null for no limit.
  final int? maxBytes;

  /// Records not accessed for longer than this are removed (subject to pins).
  final Duration? retainFor;

  /// When a budget is exceeded, evict down to this fraction of it.
  final double lowWatermark;

  /// Max records examined per eviction transaction.
  final int chunkSize;

  /// Whether any limit is set.
  bool get isBounded => maxEntries != null || maxBytes != null || retainFor != null;

  /// Whether [records] or [bytes] exceed a budget.
  bool overBudget(int records, int bytes) =>
      (maxEntries != null && records > maxEntries!) || (maxBytes != null && bytes > maxBytes!);

  /// Whether eviction can stop: both counts are at or below the low watermark.
  bool underLowWatermark(int records, int bytes) =>
      (maxEntries == null || records <= (maxEntries! * lowWatermark).floor()) &&
      (maxBytes == null || bytes <= (maxBytes! * lowWatermark).floor());

  /// Lower score = evicted first. Default: priority tier, then LRU.
  double score(StoredRecord r) =>
      r.priority * 1e15 + r.lastAccessedAt.millisecondsSinceEpoch.toDouble();
}
