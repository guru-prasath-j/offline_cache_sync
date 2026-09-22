# offline_cache_sync_test

Test kit for [offline_cache_sync](https://pub.dev/packages/offline_cache_sync).

- **`FakeClock`**: a deterministic clock and scheduler. Retry timers fire in `advance()`.
- **`FakeConnectivity`**: flip online/offline from a test.
- **`FakeServer`**: an in-memory REST-like backend. It has revisions and ETags, honours `Idempotency-Key` (replay, or a 422 on payload mismatch), detects conflicts, and can script failures (`lostBeforeServer`, `lostAfterApply`, `unavailable`, `reject`).
- **`CrashingStore`**: kills the "process" before or after the commit of a chosen transaction.
- **`EngineHarness`**: all of the above, wired together.
- **`runLocalStoreConformance`**: the suite every `LocalStore` adapter must pass.

```dart
test('a lost response is not applied twice', () async {
  final h = await EngineHarness.create();
  h.server.seed('1', {'n': 0});
  await h.items.get('1');
  h.server.failNextSends(1, SendFailure.lostAfterApply);
  await h.items.mutate('1', const Increment('n', 1));
  for (var i = 0; i < 10 && h.storedOps.isNotEmpty; i++) {
    await h.advance(const Duration(minutes: 10));
  }
  expect(h.server.valueOf('1'), {'n': 1});
});
```
