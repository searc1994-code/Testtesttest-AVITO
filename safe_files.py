import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

LOCK_SUFFIX = ".lock"
DEFAULT_LOCK_TIMEOUT = 20.0
DEFAULT_LOCK_POLL = 0.05
DEFAULT_STALE_LOCK_SECONDS = 300.0


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split())


def _lock_path(path: Path) -> Path:
    path = Path(path)
    return path.with_name(path.name + LOCK_SUFFIX)


@contextmanager
def file_lock(path: Path, timeout: float = DEFAULT_LOCK_TIMEOUT, poll_interval: float = DEFAULT_LOCK_POLL, stale_seconds: float = DEFAULT_STALE_LOCK_SECONDS) -> Iterator[Path]:
    target = Path(path)
    lock_path = _lock_path(target)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.1, float(timeout or DEFAULT_LOCK_TIMEOUT))
    payload = {
        "pid": os.getpid(),
        "target": str(target),
        "locked_at": time.time(),
    }

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
            finally:
                os.close(fd)
            break
        except FileExistsError:
            stale = False
            try:
                meta = json.loads(lock_path.read_text(encoding="utf-8"))
                locked_at = float(meta.get("locked_at") or 0.0)
                stale = locked_at > 0 and (time.time() - locked_at) > stale_seconds
            except Exception:
                try:
                    stale = (time.time() - lock_path.stat().st_mtime) > stale_seconds
                except Exception:
                    stale = False
            if stale:
                try:
                    lock_path.unlink()
                    continue
                except Exception:
                    pass
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Не удалось получить файловую блокировку: {target}")
            time.sleep(poll_interval)

    try:
        yield lock_path
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            pass


def read_json(path: Path, default: Any) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def write_json(path: Path, data: Any, *, ensure_ascii: bool = False, indent: int = 2, timeout: float = DEFAULT_LOCK_TIMEOUT) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(path, timeout=timeout):
        fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=ensure_ascii, indent=indent)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass


def write_text(path: Path, text: str, *, encoding: str = "utf-8", timeout: float = DEFAULT_LOCK_TIMEOUT) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(path, timeout=timeout):
        fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding=encoding) as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass


def append_text(path: Path, text: str, *, encoding: str = "utf-8", timeout: float = DEFAULT_LOCK_TIMEOUT) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(path, timeout=timeout):
        with path.open("a", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())


def truncate_text(path: Path, text: str = "", *, encoding: str = "utf-8", timeout: float = DEFAULT_LOCK_TIMEOUT) -> None:
    write_text(path, text, encoding=encoding, timeout=timeout)


def append_jsonl(path: Path, row: Any, *, ensure_ascii: bool = False, timeout: float = DEFAULT_LOCK_TIMEOUT) -> None:
    payload = json.dumps(row, ensure_ascii=ensure_ascii) + "\n"
    append_text(path, payload, timeout=timeout)
