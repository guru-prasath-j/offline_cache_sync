// Fails if the core package imports anything but the allowed dart: libraries
// or its own files. Run from the repo root: dart run tool/check_core_imports.dart
import 'dart:io';

const allowed = {'dart:async', 'dart:collection', 'dart:convert', 'dart:math', 'dart:typed_data'};

void main() {
  final dir = Directory('packages/offline_cache_sync/lib');
  final bad = <String>[];
  final importRe = RegExp(r'''^\s*(?:import|export)\s+['"]([^'"]+)['"]''', multiLine: true);
  for (final f in dir.listSync(recursive: true).whereType<File>().where((f) => f.path.endsWith('.dart'))) {
    for (final m in importRe.allMatches(f.readAsStringSync())) {
      final uri = m.group(1)!;
      final ok = allowed.contains(uri) || (!uri.startsWith('dart:') && !uri.startsWith('package:'));
      if (!ok) bad.add('${f.path}: $uri');
    }
  }
  if (bad.isEmpty) {
    stdout.writeln('core imports OK');
  } else {
    stderr.writeln('Forbidden imports in the core:\n${bad.join('\n')}');
    exitCode = 1;
  }
}
