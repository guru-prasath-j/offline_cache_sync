## 0.1.1

- Shorten the pubspec description to fit the pub.dev 60-180 character guideline. No code changes.

## 0.1.0

First release: the pure-Dart core engine.

- `OfflineEngine`, `Repository<K, T>` and `Resource<K, T>`, with typed `Mutation<T>` reducers (`SetFields`, `Replace`, `Delete` built in).
- Reads: fresh, stale-while-revalidate, expired fetch, stale-if-error and stale-if-offline windows; read coalescing; ETag/304 revalidation; `Cache-Control: max-age` in advisory or strict mode; a fetch-epoch and version guard against out-of-order responses.
- Writes: an atomic enqueue and a durable outbox; per-entity serial sends, parallel across entities; `dependsOn`; exponential backoff with full jitter and `Retry-After`; dead-lettering; idempotency keys minted once and reused on every retry; `Delivery.atLeastOnce` / `atMostOnce`.
- Crash recovery: `inFlight` is marked write-ahead, a single-writer lease is held (`LeaseMode.primary` / `secondary`), ambiguous ops are recovered on startup, and ops of unknown kinds are dead-lettered.
- Conflicts: `serverWins`, `clientWins`, three-way `fieldMerge`, `park` and `custom`.
- Retention: LRU + retention TTL + priority + count/byte budgets. Entities with pending ops, pinned records and watched records are never evicted.
- Observability: typed `SyncEvent`s, `SyncMetrics` and an `EventSink` port.
- An `InMemoryLocalStore` plus the `LocalStore` adapter SPI (`adapter_api.dart`).
