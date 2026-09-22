#!/usr/bin/env bash
# Verifies the workspace and (with --publish) publishes to pub.dev.
# Nothing is published unless every step before it passed.
#   ./tool/release.sh            # checks + dry runs only
#   ./tool/release.sh --publish  # checks, then publish core, then the test kit
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== pub get";            dart pub get
echo "== core import guard";  dart run tool/check_core_imports.dart
echo "== analyze";            dart analyze
echo "== core tests";         (cd packages/offline_cache_sync && dart test)
echo "== test kit + engine";  (cd packages/offline_cache_sync_test && dart test)
echo "== invariants x500";    (cd packages/offline_cache_sync_test && INVARIANT_RUNS=500 dart test -t invariants)
echo "== example";            (cd packages/offline_cache_sync && dart run example/example.dart)
echo "== dry run (core)";     (cd packages/offline_cache_sync && dart pub publish --dry-run)

if [[ "${1:-}" == "--publish" ]]; then
  echo "== publish offline_cache_sync"
  (cd packages/offline_cache_sync && dart pub publish)
  echo "== dry run + publish offline_cache_sync_test"
  (cd packages/offline_cache_sync_test && dart pub publish --dry-run && dart pub publish)
else
  echo "All checks passed. Re-run with --publish to release."
fi
