from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
steps = [
    ('py_compile', [sys.executable, str(ROOT / 'checks' / 'check_py_compile.py')]),
    ('templates', [sys.executable, str(ROOT / 'checks' / 'check_templates.py')]),
    ('app_import', [sys.executable, str(ROOT / 'checks' / 'check_app_import.py')]),
    ('smoke', [sys.executable, str(ROOT / 'checks' / 'smoke_suite.py')]),
    ('deps', [sys.executable, str(ROOT / 'checks' / 'check_dependencies.py')]),
]
results = []
for name, cmd in steps:
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    results.append({
        'step': name,
        'returncode': proc.returncode,
        'stdout': proc.stdout.strip(),
        'stderr': proc.stderr.strip(),
    })
    if proc.returncode != 0:
        print(json.dumps(results, ensure_ascii=False, indent=2))
        raise SystemExit(proc.returncode)
print(json.dumps(results, ensure_ascii=False, indent=2))
