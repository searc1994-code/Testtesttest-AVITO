from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import config
import safe_files
from safe_logs import sanitize, log_event


BASE_DIR = Path(getattr(config, 'WB_PRIVATE_DIR', Path(__file__).resolve().parent / 'wb-private')).expanduser() / 'shared' / 'module_runs'
BASE_DIR.mkdir(parents=True, exist_ok=True)


def _clean(value: Any) -> str:
    return ' '.join(str(value or '').split())


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return _utc_now().isoformat()


def _slug(value: Any, fallback: str = 'run') -> str:
    raw = _clean(value).lower()
    if not raw:
        return fallback
    out = []
    for ch in raw:
        if ch.isalnum() or ch in {'-', '_'}:
            out.append(ch)
        elif ch in {' ', '.', '/'}:
            out.append('-')
    text = ''.join(out).strip('-_')
    while '--' in text:
        text = text.replace('--', '-')
    return text or fallback


class ModuleRunLogger:
    def __init__(
        self,
        module: str,
        operation: str,
        *,
        tenant_id: str = '',
        request_id: str = '',
        job_id: str = '',
        run_id: str = '',
        correlation_id: str = '',
        actor: str = '',
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.module = _slug(module, 'module')
        self.operation = _slug(operation, 'operation')
        self.tenant_id = _clean(tenant_id)
        self.request_id = _clean(request_id)
        self.job_id = _clean(job_id)
        self.correlation_id = _clean(correlation_id) or self.job_id or self.request_id
        self.actor = _clean(actor)
        now = _utc_now()
        if run_id:
            self.run_id = _clean(run_id)
        else:
            self.run_id = f"{now.strftime('%H%M%S')}_{self.module}_{self.operation}_{uuid.uuid4().hex[:8]}"
        self.date_key = now.strftime('%Y%m%d')
        self.run_dir = BASE_DIR / self.module / self.date_key / self.run_id
        self.inputs_dir = self.run_dir / 'inputs'
        self.outputs_dir = self.run_dir / 'outputs'
        self.forensics_dir = self.run_dir / 'forensics'
        for path in (self.run_dir, self.inputs_dir, self.outputs_dir, self.forensics_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.events_path = self.run_dir / 'events.jsonl'
        self.summary_path = self.run_dir / 'summary.json'
        self.manifest_path = self.run_dir / 'manifest.json'
        self._seq = 0
        self._summary: Dict[str, Any] = {
            'module': self.module,
            'operation': self.operation,
            'tenant_id': self.tenant_id,
            'request_id': self.request_id,
            'job_id': self.job_id,
            'run_id': self.run_id,
            'correlation_id': self.correlation_id,
            'actor': self.actor,
            'status': 'running',
            'started_at': utc_now_iso(),
            'finished_at': '',
            'events_count': 0,
            'inputs': {},
            'outputs': {},
            'meta': sanitize(meta or {}),
        }
        self._write_manifest()
        self.event('run_started', stage='start', message=f'Начат run {self.module}/{self.operation}', run_dir=str(self.run_dir))

    def _write_manifest(self) -> None:
        manifest = {
            'module': self.module,
            'operation': self.operation,
            'tenant_id': self.tenant_id,
            'request_id': self.request_id,
            'job_id': self.job_id,
            'run_id': self.run_id,
            'correlation_id': self.correlation_id,
            'run_dir': str(self.run_dir),
            'events_path': str(self.events_path),
            'summary_path': str(self.summary_path),
            'inputs_dir': str(self.inputs_dir),
            'outputs_dir': str(self.outputs_dir),
            'forensics_dir': str(self.forensics_dir),
        }
        safe_files.write_json(self.manifest_path, manifest, ensure_ascii=False, indent=2)

    def _base_row(self, event: str, level: str, stage: str = '', message: str = '') -> Dict[str, Any]:
        self._seq += 1
        return {
            'ts': utc_now_iso(),
            'seq': self._seq,
            'module': self.module,
            'operation': self.operation,
            'tenant_id': self.tenant_id,
            'request_id': self.request_id,
            'job_id': self.job_id,
            'run_id': self.run_id,
            'correlation_id': self.correlation_id,
            'actor': self.actor,
            'event': _clean(event) or 'event',
            'level': _clean(level) or 'info',
            'stage': _clean(stage),
            'message': _clean(message),
        }

    def event(self, event: str, *, level: str = 'info', stage: str = '', message: str = '', **data: Any) -> Dict[str, Any]:
        row = self._base_row(event, level, stage=stage, message=message)
        payload = sanitize(data)
        if payload:
            row['data'] = payload
        safe_files.append_jsonl(self.events_path, row, ensure_ascii=False)
        self._summary['events_count'] = int(self._summary.get('events_count') or 0) + 1
        safe_files.write_json(self.summary_path, self._summary, ensure_ascii=False, indent=2)
        log_event(
            self.module,
            event,
            tenant_id=self.tenant_id,
            level=level,
            bucket='forensics',
            request_id=self.request_id,
            job_id=self.job_id,
            run_id=self.run_id,
            correlation_id=self.correlation_id,
            run_dir=str(self.run_dir),
            stage=stage,
            message=message,
        )
        return row

    def write_input(self, name: str, payload: Any) -> Path:
        file_path = self.inputs_dir / f'{_slug(name, "input")}.json'
        safe_files.write_json(file_path, sanitize(payload), ensure_ascii=False, indent=2)
        inputs = self._summary.get('inputs') if isinstance(self._summary.get('inputs'), dict) else {}
        inputs[_clean(name) or 'input'] = str(file_path)
        self._summary['inputs'] = inputs
        safe_files.write_json(self.summary_path, self._summary, ensure_ascii=False, indent=2)
        return file_path

    def write_output(self, name: str, payload: Any) -> Path:
        file_path = self.outputs_dir / f'{_slug(name, "output")}.json'
        safe_files.write_json(file_path, sanitize(payload), ensure_ascii=False, indent=2)
        outputs = self._summary.get('outputs') if isinstance(self._summary.get('outputs'), dict) else {}
        outputs[_clean(name) or 'output'] = str(file_path)
        self._summary['outputs'] = outputs
        safe_files.write_json(self.summary_path, self._summary, ensure_ascii=False, indent=2)
        return file_path

    def attach_artifact(self, name: str, path: Any) -> None:
        artifacts = self._summary.get('artifacts') if isinstance(self._summary.get('artifacts'), dict) else {}
        artifacts[_clean(name) or 'artifact'] = _clean(path)
        self._summary['artifacts'] = artifacts
        safe_files.write_json(self.summary_path, self._summary, ensure_ascii=False, indent=2)

    def update_summary(self, **fields: Any) -> None:
        for key, value in fields.items():
            self._summary[_clean(key) or key] = sanitize(value)
        safe_files.write_json(self.summary_path, self._summary, ensure_ascii=False, indent=2)

    def finalize(self, status: str = 'completed', *, message: str = '', **summary: Any) -> Dict[str, Any]:
        self._summary['status'] = _clean(status) or 'completed'
        self._summary['finished_at'] = utc_now_iso()
        self._summary['message'] = _clean(message)
        for key, value in summary.items():
            self._summary[_clean(key) or key] = sanitize(value)
        self.event('run_finished', stage='finish', message=message or 'Run завершён', status=self._summary['status'])
        safe_files.write_json(self.summary_path, self._summary, ensure_ascii=False, indent=2)
        return dict(self._summary)


__all__ = ['BASE_DIR', 'ModuleRunLogger', 'utc_now_iso']
