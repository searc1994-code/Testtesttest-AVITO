from __future__ import annotations

import py_compile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
errors: list[str] = []
compiled = 0
for path in sorted(ROOT.rglob('*.py')):
    if '__pycache__' in path.parts:
        continue
    py_compile.compile(str(path), doraise=True)
    compiled += 1
print(f'PY_COMPILE_OK {compiled}')
