from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Tuple

DEFAULT_EXCLUDES = {
    '.git',
    '__pycache__',
    '.pytest_cache',
    '.mypy_cache',
    '.venv',
    'node_modules',
    'dist',
    'build',
    'safe_logs',
}
SKIP_SUFFIXES = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.pdf', '.zip', '.gz', '.sqlite', '.sqlite3', '.db', '.pyc'}

# Purposefully tuned to detect literal credential leaks, not ordinary variable names in source code.
PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    ('openai_api_key', re.compile(r'\bsk-[A-Za-z0-9]{20,}\b')),
    ('avito_client_secret_literal', re.compile(r'(?i)(client_secret|avito_client_secret)\s*[:=]\s*["\']([A-Za-z0-9_\-]{8,})["\']')),
    ('oauth_token_literal', re.compile(r'(?i)(access_token|refresh_token|webhook_secret)\s*[:=]\s*["\']([A-Za-z0-9_\-]{8,})["\']')),
    ('bearer_token', re.compile(r'(?i)authorization\s*[:=]\s*["\']bearer\s+[A-Za-z0-9._\-]{16,}["\']')),
    ('generic_secret_literal', re.compile(r'(?i)(api_key|secret_key|token)\s*[:=]\s*["\']([A-Za-z0-9_\-]{16,})["\']')),
]
PATH_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    ('tracked_auth_state', re.compile(r'(^|/)(auth)/(avito_state|avito_secrets|wb_state)\.json$', re.I)),
    ('tracked_env_file', re.compile(r'(^|/)\.env(\.|$)', re.I)),
]
ALLOW_SUBSTRINGS = {'example', 'placeholder', 'dummy', 'test_', 'your_'}


def iter_files(root: Path) -> Iterable[Path]:
    for path in root.rglob('*'):
        if path.is_dir():
            continue
        parts = set(path.parts)
        if parts & DEFAULT_EXCLUDES:
            continue
        if path.suffix.lower() in SKIP_SUFFIXES:
            continue
        yield path



def should_skip_match(rel: str, line: str) -> bool:
    lower_rel = rel.lower()
    lower_line = line.lower()
    if lower_rel.endswith('_test.py') or lower_rel.endswith('smoke_test_avito_module.py'):
        return True
    return any(token in lower_line for token in ALLOW_SUBSTRINGS)



def scan_path(path: Path, root: Path) -> List[str]:
    findings: List[str] = []
    rel = path.relative_to(root).as_posix()
    for label, pattern in PATH_PATTERNS:
        if pattern.search(rel):
            findings.append(f'{label}: path={rel}')
    try:
        text = path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return findings
    for idx, line in enumerate(text.splitlines(), start=1):
        if should_skip_match(rel, line):
            continue
        for label, pattern in PATTERNS:
            if pattern.search(line):
                findings.append(f'{label}: {rel}:{idx}: {line[:220]}')
    return findings



def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Simple secret scanner for Avito/runtime credentials')
    parser.add_argument('root', nargs='?', default='.', help='repository root')
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    findings: List[str] = []
    for path in iter_files(root):
        findings.extend(scan_path(path, root))
    if findings:
        print('Secret scan findings:')
        for item in findings:
            print('-', item)
        return 1
    print('Secret scan: no findings')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
