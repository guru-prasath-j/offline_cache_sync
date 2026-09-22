import 'dart:math';

import 'package:offline_cache_sync/offline_cache_sync.dart';
import 'package:test/test.dart';

void main() {
  final t0 = DateTime.utc(2026, 1, 1);

  StoredRecord rec({DateTime? fetchedAt, Duration? maxAge, bool invalidated = false}) => StoredRecord(
        key: RecordKey('item', '1'),
        payload: const {'a': 1},
        fetchedAt: fetchedAt ?? t0,
        lastAccessedAt: t0,
        httpMaxAge: maxAge,
        invalidated: invalidated,
      );

  group('RecordKey', () {
    test('round-trips type and id (ids may contain ":")', () {
      final k = RecordKey('user', 'a:b');
      expect(k.type, 'user');
      expect(k.id, 'a:b');
      expect(RecordKey.parse(k.value), k);
    });

    test('rejects bad types', () {
      expect(() => RecordKey('', '1'), throwsArgumentError);
      expect(() => RecordKey('a:b', '1'), throwsArgumentError);
    });
  });

  group('UuidV4', () {
    test('produces RFC 4122 v4 strings', () {
      final u = UuidV4(Random(1));
      final re = RegExp(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');
      for (var i = 0; i < 100; i++) {
        expect(u.next(), matches(re));
      }
    });
  });

  group('Version', () {
    test('orders by revision, then updatedAt; ETags are unordered', () {
      expect(const Version(revision: 3).isNewerThan(const Version(revision: 2)), isTrue);
      expect(const Version(revision: 2).isNewerThan(const Version(revision: 3)), isFalse);
      expect(Version(updatedAt: t0.add(const Duration(seconds: 1))).isNewerThan(Version(updatedAt: t0)), isTrue);
      expect(const Version(etag: '"b"').isNewerThan(const Version(etag: '"a"')), isFalse);
      expect(const Version(revision: 1).isNewerThan(null), isFalse);
    });

    test('json round trip', () {
      final v = Version(etag: '"x"', revision: 7, updatedAt: t0);
      expect(Version.fromJson(v.toJson()), v);
    });
  });

  group('FreshnessWindows', () {
    const w = FreshnessWindows(
      staleAfter: Duration(minutes: 1),
      expireAfter: Duration(hours: 1),
      staleIfError: Duration(hours: 2),
      staleIfOffline: Duration(days: 1),
    );

    test('classifies fresh / stale / expired / missing', () {
      expect(w.classify(null, t0), Freshness.missing);
      expect(w.classify(rec(), t0.add(const Duration(seconds: 30))), Freshness.fresh);
      expect(w.classify(rec(), t0.add(const Duration(minutes: 5))), Freshness.stale);
      expect(w.classify(rec(), t0.add(const Duration(hours: 1))), Freshness.expired);
    });

    test('an invalidated record is stale even if recent', () {
      expect(w.classify(rec(invalidated: true), t0), Freshness.stale);
    });

    test('stale-if-error and stale-if-offline windows', () {
      expect(w.usableOnError(rec(), t0.add(const Duration(hours: 2, minutes: 59))), isTrue);
      expect(w.usableOnError(rec(), t0.add(const Duration(hours: 3))), isFalse);
      expect(w.usableOffline(rec(), t0.add(const Duration(hours: 24))), isTrue);
      expect(w.usableOffline(rec(), t0.add(const Duration(days: 2))), isFalse);
      expect(const FreshnessWindows().usableOffline(rec(), t0.add(const Duration(days: 999))), isTrue);
    });

    test('HTTP max-age: advisory can only shorten, strict overrides, ignore ignores', () {
      final r = rec(maxAge: const Duration(seconds: 10));
      final at = t0.add(const Duration(seconds: 20));
      expect(w.classify(r, at), Freshness.stale); // advisory: 10s < 1min
      expect(w.copyWith(httpMode: HttpDirectiveMode.ignore).classify(r, at), Freshness.fresh);
      final long = rec(maxAge: const Duration(minutes: 30));
      final at2 = t0.add(const Duration(minutes: 5));
      expect(w.classify(long, at2), Freshness.stale); // advisory never extends
      expect(w.copyWith(httpMode: HttpDirectiveMode.strict).classify(long, at2), Freshness.fresh);
    });
  });

  group('ExponentialBackoff', () {
    test('full jitter stays under the doubling ceiling and the cap', () {
      final b = ExponentialBackoff(
        base: const Duration(seconds: 1),
        cap: const Duration(seconds: 30),
        maxAttempts: 20,
        random: Random(7),
      );
      for (var attempt = 1; attempt < 15; attempt++) {
        final d = b.decide(attempts: attempt, error: const ErrorInfo(code: 'x'));
        expect(d, isA<RetryAt>());
        final delay = (d as RetryAt).delay;
        expect(delay, lessThanOrEqualTo(b.ceilingFor(attempt)));
        expect(delay, lessThanOrEqualTo(const Duration(seconds: 30)));
      }
      expect(b.ceilingFor(1), const Duration(seconds: 1));
      expect(b.ceilingFor(3), const Duration(seconds: 4));
    });

    test('honours Retry-After as a lower bound and gives up at maxAttempts', () {
      final b = ExponentialBackoff(maxAttempts: 3, random: Random(1));
      final d = b.decide(attempts: 1, error: const ErrorInfo(code: 'x'), retryAfter: const Duration(minutes: 2));
      expect((d as RetryAt).delay, greaterThanOrEqualTo(const Duration(minutes: 2)));
      expect(b.decide(attempts: 3, error: const ErrorInfo(code: 'x')), isA<GiveUp>());
    });
  });

  group('built-in mutations', () {
    test('SetFields merges and removes', () {
      const m = SetFields({'a': 2, 'b': null, 'c': 3});
      expect(m.apply({'a': 1, 'b': 1}), {'a': 2, 'c': 3});
      expect(m.apply(null), {'a': 2, 'c': 3});
      expect(SetFields.fromJson(m.toJson()).fields, m.fields);
    });

    test('Replace and Delete', () {
      final codec = JsonCodecOf<Map<String, Object?>>((j) => Map<String, Object?>.from(j! as Map), (v) => v);
      expect(Replace<Map<String, Object?>>({'x': 1}, codec).apply({'y': 2}), {'x': 1});
      expect(const Delete<Map<String, Object?>>().apply({'y': 2}), isNull);
    });
  });

  group('fieldMerge', () {
    final resolver = ConflictStrategies.fieldMerge();
    Operation op() => Operation(
          id: const OpId('o'),
          seq: 1,
          entity: RecordKey('item', '1'),
          kind: r'$set',
          payload: const {},
          localVersion: 1,
          createdAt: t0,
          updatedAt: t0,
        );

    test('takes local-only changes, server-only changes, and server on overlap', () async {
      final r = await resolver.resolve(Conflict(
        op: op(),
        base: const {'name': 'a', 'age': 1, 'city': 'x'},
        local: const {'name': 'b', 'age': 1, 'city': 'y'},
        server: const {'name': 'a', 'age': 2, 'city': 'z'},
        serverDeleted: false,
      ));
      expect(r, isA<MergeWith<Map<String, Object?>>>());
      expect((r as MergeWith<Map<String, Object?>>).value, {'name': 'b', 'age': 2, 'city': 'z'});
    });

    test('a server delete wins', () async {
      final r = await resolver.resolve(Conflict(
        op: op(),
        base: const {'a': 1},
        local: const {'a': 2},
        server: null,
        serverDeleted: true,
      ));
      expect(r, isA<TakeServer<Map<String, Object?>>>());
    });
  });

  group('jsonEquals', () {
    test('deep equality', () {
      expect(jsonEquals({'a': [1, {'b': 2}]}, {'a': [1, {'b': 2}]}), isTrue);
      expect(jsonEquals({'a': [1, {'b': 2}]}, {'a': [1, {'b': 3}]}), isFalse);
      expect(jsonEquals(null, null), isTrue);
      expect(jsonEquals({'a': null}, <String, Object?>{}), isFalse);
    });
  });
}
