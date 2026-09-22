import '../model/records.dart';

/// How fresh the server-confirmed base is. Pending local writes are layered
/// on top regardless of freshness.
enum Freshness {
  /// Serve without asking the server.
  fresh,

  /// Serve immediately and revalidate in the background (SWR).
  stale,

  /// Do not serve as a normal result; fetch first. May still be served under
  /// stale-if-error / stale-if-offline.
  expired,

  /// Nothing cached.
  missing,
}

/// How to treat HTTP `Cache-Control` hints stored on a record.
enum HttpDirectiveMode {
  /// Use `max-age` only when the app did not configure [FreshnessWindows.staleAfter].
  advisory,

  /// `max-age` always overrides [FreshnessWindows.staleAfter].
  strict,

  /// Ignore server hints entirely.
  ignore,
}

/// The four freshness windows of section 6.1, measured from `fetchedAt`
/// (the last 200 or 304).
final class FreshnessWindows {
  const FreshnessWindows({
    this.staleAfter = const Duration(seconds: 30),
    this.expireAfter = const Duration(hours: 24),
    this.staleIfError = const Duration(days: 7),
    this.staleIfOffline,
    this.httpMode = HttpDirectiveMode.advisory,
  });

  final Duration staleAfter;
  final Duration expireAfter;

  /// Extra window after [expireAfter] during which an expired value may be
  /// served when the fetch fails with a transport/5xx error.
  final Duration staleIfError;

  /// Extra window after [expireAfter] during which an expired value may be
  /// served while offline. Null means unlimited (the offline-first default).
  final Duration? staleIfOffline;

  final HttpDirectiveMode httpMode;

  FreshnessWindows copyWith({
    Duration? staleAfter,
    Duration? expireAfter,
    Duration? staleIfError,
    Duration? staleIfOffline,
    HttpDirectiveMode? httpMode,
  }) =>
      FreshnessWindows(
        staleAfter: staleAfter ?? this.staleAfter,
        expireAfter: expireAfter ?? this.expireAfter,
        staleIfError: staleIfError ?? this.staleIfError,
        staleIfOffline: staleIfOffline ?? this.staleIfOffline,
        httpMode: httpMode ?? this.httpMode,
      );

  Duration _effectiveStaleAfter(StoredRecord r) {
    final maxAge = r.httpMaxAge;
    if (maxAge == null) return staleAfter;
    return switch (httpMode) {
      HttpDirectiveMode.strict => maxAge,
      // Advisory: a server hint may shorten freshness, never extend it.
      HttpDirectiveMode.advisory => maxAge < staleAfter ? maxAge : staleAfter,
      HttpDirectiveMode.ignore => staleAfter,
    };
  }

  /// Classifies [record] at [now].
  Freshness classify(StoredRecord? record, DateTime now) {
    if (record == null) return Freshness.missing;
    if (record.invalidated) {
      // Derived locally: usable, but must be revalidated.
      return Freshness.stale;
    }
    final age = now.difference(record.fetchedAt);
    if (age >= expireAfter) return Freshness.expired;
    if (age >= _effectiveStaleAfter(record)) return Freshness.stale;
    return Freshness.fresh;
  }

  /// Whether an expired [record] may still be served after a failed fetch.
  bool usableOnError(StoredRecord record, DateTime now) =>
      now.difference(record.fetchedAt) < expireAfter + staleIfError;

  /// Whether an expired [record] may still be served while offline.
  bool usableOffline(StoredRecord record, DateTime now) {
    final extra = staleIfOffline;
    if (extra == null) return true;
    return now.difference(record.fetchedAt) < expireAfter + extra;
  }
}
