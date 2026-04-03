from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import config
import safe_files

SAFE_BASE_DIR = Path(getattr(config, 'WB_PRIVATE_DIR', Path(__file__).resolve().parent / 'wb-private')).expanduser() / 'shared' / 'safe_logs'
SAFE_BASE_DIR.mkdir(parents=True, exist_ok=True)

SENSITIVE_KEYS = {
    'api_key', 'wb_api_key', 'openai_api_key', 'authorization', 'auth', 'cookie', 'cookies',
    'password', 'secret', 'token', 'bearer', 'phone', 'telephone', 'wb_state', 'storage_state',
}

MAX_STRING_LEN = 1200
_CORRELATION_FIELDS = ('request_id', 'job_id', 'run_id', 'correlation_id')
_AUDIT_CHANNELS = {'security', 'auth', 'audit'}
_FORENSICS_CHANNELS = {'forensics', 'ui_forensics'}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any) -> str:
    return re.sub(r'\s+', ' ', str(value or '')).strip()


def _mask_phone(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        digits = re.sub(r'\D', '', match.group(0))
        if len(digits) < 6:
            return '[PHONE]'
        return f'[PHONE:{digits[:2]}***{digits[-2:]}]'
    return re.sub(r'(?:\+?7|8)[\s\-()]*\d(?:[\s\-()]*\d){9,10}', repl, text)


def _redact_text(text: str) -> str:
    text = _clean(text)
    if not text:
        return text
    text = re.sub(r'(?i)(authorization\s*[:=]\s*bearer\s+)[A-Za-z0-9._\-]+', r'\1[REDACTED]', text)
    text = re.sub(r'(?i)((?:api|token|secret|password|cookie|authorization|wb_api_key|openai_api_key)[\w\- ]*[:=]\s*)[^,;\s]+', r'\1[REDACTED]', text)
    text = re.sub(r'\bsk-[A-Za-z0-9\-_]{10,}\b', '[OPENAI_KEY_REDACTED]', text)
    text = re.sub(r'\b[A-Za-z0-9_\-]{30,}\b', lambda match: '[LONG_TOKEN_REDACTED]' if any(ch.isdigit() for ch in match.group(0)) else match.group(0), text)
    text = _mask_phone(text)
    if len(text) > MAX_STRING_LEN:
        text = text[:MAX_STRING_LEN] + '…'
    return text


def _looks_sensitive_key(key: str) -> bool:
    lowered = _clean(key).lower()
    return any(part in lowered for part in SENSITIVE_KEYS)


def sanitize(data: Any, key_hint: str = '') -> Any:
    if _looks_sensitive_key(key_hint):
        return '[REDACTED]'
    if data is None:
        return None
    if isinstance(data, (int, float, bool)):
        return data
    if isinstance(data, str):
        return _redact_text(data)
    if isinstance(data, Path):
        return _redact_text(str(data))
    if isinstance(data, dict):
        out: Dict[str, Any] = {}
        for key, value in data.items():
            key_text = _clean(key)
            out[key_text] = sanitize(value, key_hint=key_text)
        return out
    if isinstance(data, (list, tuple, set)):
        return [sanitize(item, key_hint=key_hint) for item in list(data)]
    return _redact_text(repr(data))


def _tenant_dir(tenant_id: Optional[str]) -> Path:
    tenant = _clean(tenant_id) or '_system'
    path = SAFE_BASE_DIR / tenant
    path.mkdir(parents=True, exist_ok=True)
    return path


def _bucket_dir(tenant_id: Optional[str]) -> Path:
    path = _tenant_dir(tenant_id) / 'buckets'
    path.mkdir(parents=True, exist_ok=True)
    return path


def _infer_bucket(channel: str, event: str, data: Dict[str, Any]) -> str:
    channel = _clean(channel).lower()
    event = _clean(event).lower()
    if channel in _AUDIT_CHANNELS or event.startswith('login') or event.startswith('logout'):
        return 'audit'
    if channel in _FORENSICS_CHANNELS:
        return 'forensics'
    forensic_markers = {'screenshot_path', 'html_path', 'meta_path', 'manifest_path', 'trace_path', 'video_path', 'forensics_path', 'run_dir', 'summary_path', 'events_path', 'inputs_dir', 'outputs_dir'}
    if any(key in data for key in forensic_markers):
        return 'forensics'
    return 'ops'


def log_event(channel: str, event: str, tenant_id: Optional[str] = None, level: str = 'info', bucket: str = '', **data: Any) -> None:
    try:
        channel = _clean(channel) or 'app'
        event = _clean(event) or 'event'
        level = _clean(level) or 'info'
        sanitized_data = sanitize(data)
        row: Dict[str, Any] = {
            'ts': utc_now_iso(),
            'tenant_id': _clean(tenant_id),
            'channel': channel,
            'event': event,
            'level': level,
            'bucket': _clean(bucket) or _infer_bucket(channel, event, sanitized_data if isinstance(sanitized_data, dict) else {}),
            'data': sanitized_data,
        }
        if isinstance(sanitized_data, dict):
            for field in _CORRELATION_FIELDS:
                value = _clean(sanitized_data.get(field))
                if value:
                    row[field] = value
        channel_path = _tenant_dir(tenant_id) / f'{channel}.jsonl'
        safe_files.append_jsonl(channel_path, row, ensure_ascii=False)
        bucket_name = _clean(row.get('bucket'))
        if bucket_name:
            bucket_path = _bucket_dir(tenant_id) / f'{bucket_name}.jsonl'
            safe_files.append_jsonl(bucket_path, row, ensure_ascii=False)
    except Exception:
        return


def list_channels(tenant_id: Optional[str] = None) -> List[str]:
    tenants = []
    if tenant_id and tenant_id != 'all':
        tenants = [_tenant_dir(tenant_id)]
    else:
        tenants = [path for path in SAFE_BASE_DIR.iterdir() if path.is_dir()] if SAFE_BASE_DIR.exists() else []
    names = set()
    for tenant_dir in tenants:
        for file_path in tenant_dir.glob('*.jsonl'):
            names.add(file_path.stem)
    return sorted(names)


def list_tenants() -> List[str]:
    if not SAFE_BASE_DIR.exists():
        return []
    return sorted([path.name for path in SAFE_BASE_DIR.iterdir() if path.is_dir()])


def read_events(tenant_id: Optional[str] = None, channel: str = 'all', limit: int = 300) -> List[Dict[str, Any]]:
    files: List[Path] = []
    if tenant_id and tenant_id != 'all':
        base = _tenant_dir(tenant_id)
        if channel and channel != 'all':
            files = [base / f'{channel}.jsonl']
        else:
            files = list(base.glob('*.jsonl'))
    else:
        for tenant_dir in [path for path in SAFE_BASE_DIR.iterdir() if path.is_dir()] if SAFE_BASE_DIR.exists() else []:
            if channel and channel != 'all':
                files.append(tenant_dir / f'{channel}.jsonl')
            else:
                files.extend(tenant_dir.glob('*.jsonl'))
    rows: List[Dict[str, Any]] = []
    for file_path in files:
        if not file_path.exists():
            continue
        try:
            with file_path.open('r', encoding='utf-8') as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        rows.append(row)
        except Exception:
            continue
    rows.sort(key=lambda row: _clean(row.get('ts')))
    return rows[-limit:]


def stats(tenant_id: Optional[str] = None, channel: str = 'all', limit: int = 1000) -> Dict[str, Any]:
    rows = read_events(tenant_id=tenant_id, channel=channel, limit=limit)
    by_channel: Counter[str] = Counter()
    by_level: Counter[str] = Counter()
    by_event: Counter[str] = Counter()
    by_bucket: Counter[str] = Counter()
    for row in rows:
        by_channel[_clean(row.get('channel')) or 'app'] += 1
        by_level[_clean(row.get('level')) or 'info'] += 1
        by_event[_clean(row.get('event')) or 'event'] += 1
        by_bucket[_clean(row.get('bucket')) or 'ops'] += 1
    return {
        'total': len(rows),
        'by_channel': dict(by_channel),
        'by_level': dict(by_level),
        'by_bucket': dict(by_bucket),
        'top_events': by_event.most_common(20),
    }


def safe_log_event(channel: str, tenant_id: Optional[str], level: str, message: str, **data: Any) -> None:
    log_event(channel, message, tenant_id=tenant_id, level=level, **data)


def safe_log_exception(channel: str, tenant_id: Optional[str], exc: Exception, message: str = '', **data: Any) -> None:
    payload = dict(data)
    if message:
        payload['message'] = message
    payload['error'] = _clean(exc)
    log_event(channel, 'exception', tenant_id=tenant_id, level='error', **payload)
