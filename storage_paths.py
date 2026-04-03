from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterable, List, Sequence


def _dedupe_paths(paths: Iterable[Path]) -> List[Path]:
    result: List[Path] = []
    seen: set[str] = set()
    for path in paths:
        candidate = Path(path).expanduser()
        key = str(candidate)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result


SECURITY_FILES = ('admin_auth.json', 'flask_secret.txt')


def root_state_score(root: Path) -> int:
    root = Path(root).expanduser()
    score = 0
    try:
        if (root / 'tenants.json').exists():
            score += 100
        tenants_dir = root / 'tenants'
        if tenants_dir.is_dir() and any(tenants_dir.iterdir()):
            score += 90
        if (root / 'security' / 'admin_auth.json').exists():
            score += 15
        if (root / 'security' / 'flask_secret.txt').exists():
            score += 10
        for dirname in ('shared', 'logs', 'auth', 'data'):
            directory = root / dirname
            if directory.is_dir() and any(directory.iterdir()):
                score += 3
    except Exception:
        return 0
    return score


def root_has_state(root: Path) -> bool:
    return root_state_score(root) > 0


def candidate_private_roots(
    *,
    env_value: str | None = None,
    os_name: str | None = None,
    home: Path | None = None,
    windows_legacy_root: Path | None = None,
) -> List[Path]:
    env_value = os.getenv('WB_PRIVATE_DIR', '') if env_value is None else str(env_value or '')
    if env_value.strip():
        return [Path(env_value).expanduser()]

    os_name = os_name or os.name
    home = Path(home).expanduser() if home is not None else Path.home()
    home_root = home / 'wb-ai-private'

    if os_name == 'nt':
        if windows_legacy_root is not None:
            legacy_root = Path(windows_legacy_root).expanduser()
        else:
            system_drive = (os.getenv('SystemDrive') or 'C:').rstrip('\\/') or 'C:'
            legacy_root = Path(f'{system_drive}/wb-ai-private')
        return _dedupe_paths([legacy_root, home_root])

    return _dedupe_paths([home_root])


def resolve_private_root(
    *,
    env_value: str | None = None,
    os_name: str | None = None,
    home: Path | None = None,
    windows_legacy_root: Path | None = None,
) -> Path:
    candidates = candidate_private_roots(
        env_value=env_value,
        os_name=os_name,
        home=home,
        windows_legacy_root=windows_legacy_root,
    )
    best_candidate = candidates[0]
    best_score = root_state_score(best_candidate)
    for candidate in candidates[1:]:
        score = root_state_score(candidate)
        if score > best_score:
            best_candidate = candidate
            best_score = score
    return best_candidate


def sibling_private_roots(
    selected_root: Path,
    *,
    env_value: str | None = None,
    os_name: str | None = None,
    home: Path | None = None,
    windows_legacy_root: Path | None = None,
) -> List[Path]:
    selected_root = Path(selected_root).expanduser()
    siblings: List[Path] = []
    for candidate in candidate_private_roots(
        env_value=env_value,
        os_name=os_name,
        home=home,
        windows_legacy_root=windows_legacy_root,
    ):
        try:
            if candidate.resolve() == selected_root.resolve():
                continue
        except Exception:
            if str(candidate) == str(selected_root):
                continue
        siblings.append(candidate)
    return siblings


def hydrate_security_files(primary_root: Path, alternate_roots: Sequence[Path]) -> List[Path]:
    primary_root = Path(primary_root).expanduser()
    primary_security_dir = primary_root / 'security'
    primary_security_dir.mkdir(parents=True, exist_ok=True)
    copied: List[Path] = []

    for filename in SECURITY_FILES:
        destination = primary_security_dir / filename
        if destination.exists():
            continue
        for root in alternate_roots:
            source = Path(root).expanduser() / 'security' / filename
            if not source.exists():
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            copied.append(destination)
            break
    return copied
