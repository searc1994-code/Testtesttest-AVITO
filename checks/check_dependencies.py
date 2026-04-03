from __future__ import annotations

import importlib.metadata as md
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
req_path = ROOT / 'requirements.txt'
missing = []
mismatched = []
checked = 0
for raw in req_path.read_text(encoding='utf-8').splitlines():
    line = raw.strip()
    if not line or line.startswith('#'):
        continue
    if '==' not in line:
        continue
    name, version = line.split('==', 1)
    name = name.strip()
    version = version.strip()
    checked += 1
    try:
        installed = md.version(name)
    except Exception:
        missing.append(name)
        continue
    if installed != version:
        mismatched.append((name, version, installed))
print({'checked': checked, 'missing': missing, 'mismatched': mismatched})
