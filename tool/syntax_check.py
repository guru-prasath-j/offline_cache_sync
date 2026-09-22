"""Parse every .dart file with tree-sitter and report syntax errors (no type checking)."""
import pathlib, sys
from tree_sitter_language_pack import get_parser
p = get_parser('dart')
bad = 0
for f in sorted(pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else '.').rglob('*.dart')):
    tree = p.parse(f.read_bytes())
    errs = []
    def walk(n):
        if n.type == 'ERROR' or n.is_missing:
            errs.append((n.start_point[0] + 1, n.start_point[1] + 1, n.type))
            return
        for c in n.children:
            walk(c)
    walk(tree.root_node)
    if errs:
        bad += 1
        print(f, errs[:5])
print('files with errors:', bad)
sys.exit(1 if bad else 0)
