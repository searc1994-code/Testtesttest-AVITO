from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMP_ROOT = Path(tempfile.mkdtemp(prefix='wb-merged-smoke-'))
os.environ['WB_PRIVATE_DIR'] = str(TEMP_ROOT / 'private')
os.environ.pop('APP_ADMIN_PASSWORD', None)
os.environ.pop('WB_API_KEY', None)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Fresh imports after env bootstrap.
for mod_name in [
    'config', 'safe_files', 'tenant_manager', 'common', 'background_jobs',
    'history_sync_worker', 'history_service', 'complaint_core', 'question_core',
    'auth_core', 'browser_bot', 'storage_paths', 'promo_executor'
]:
    if mod_name in sys.modules:
        del sys.modules[mod_name]

import auth_core
import background_jobs
import browser_bot
import common
import complaint_core
import history_service
import question_core
import safe_files
import storage_paths
import tenant_manager
import promo_executor


def _wait_job(job_id: str, timeout: float = 8.0) -> dict:
    started = time.time()
    while time.time() - started < timeout:
        current = background_jobs.get_job(job_id) or {}
        if current.get('status') in {'completed', 'error', 'abandoned'}:
            return current
        time.sleep(0.1)
    return background_jobs.get_job(job_id) or {}


# 0. Legacy private-root autodiscovery should prefer old C-drive-like storage and reuse security files.
legacy_root = TEMP_ROOT / 'legacy-root'
legacy_home = TEMP_ROOT / 'home-root'
legacy_home_root = legacy_home / 'wb-ai-private'
legacy_root.mkdir(parents=True, exist_ok=True)
legacy_home_root.mkdir(parents=True, exist_ok=True)
safe_files.write_json(legacy_root / 'tenants.json', [{'id': 'legacy', 'name': 'Legacy tenant'}])
safe_files.write_text(legacy_home_root / 'security' / 'admin_auth.json', json.dumps({'username': 'admin'}), encoding='utf-8')
resolved_root = storage_paths.resolve_private_root(env_value='', os_name='nt', home=legacy_home, windows_legacy_root=legacy_root)
assert resolved_root == legacy_root
copied_security = storage_paths.hydrate_security_files(resolved_root, storage_paths.sibling_private_roots(resolved_root, env_value='', os_name='nt', home=legacy_home, windows_legacy_root=legacy_root))
assert (resolved_root / 'security' / 'admin_auth.json').exists()
assert copied_security

# 1. Auth bootstrap / first run password setup.
assert auth_core.needs_bootstrap() is True
record = auth_core.bootstrap_admin_password('VeryStrongPass123', confirm_password='VeryStrongPass123')
assert record['username'] == 'admin'
assert auth_core.needs_bootstrap() is False
assert auth_core.verify_credentials('admin', 'VeryStrongPass123') is True
assert auth_core.verify_credentials('admin', 'WrongPass') is False

# 2. Tenant isolation + malicious tenant id rejection.
alpha = tenant_manager.create_tenant('Alpha LLC', '+79990000001', 'token-alpha', tenant_slug='alpha')
beta = tenant_manager.create_tenant('Beta LLC', '+79990000002', 'token-beta', tenant_slug='beta')
for bad_tenant in ('../../evil', '<script>alert(1)</script>', 'a' * 81):
    try:
        tenant_manager.ensure_tenant_dirs(bad_tenant)
        raise AssertionError(f'Invalid tenant id was accepted: {bad_tenant!r}')
    except ValueError:
        pass

for tenant_id, text in [('alpha', 'draft-alpha'), ('beta', 'draft-beta')]:
    tenant = tenant_manager.get_tenant(tenant_id)
    paths = tenant_manager.ensure_tenant_dirs(tenant_id)
    tokens = common.bind_tenant_context(tenant_id, tenant=tenant, paths=paths)
    try:
        common.write_json(common.DRAFTS_FILE, {'row': {'reply': text}})
        common.write_json(common.REPLY_QUEUE_FILE, [{'id': tenant_id, 'status': 'queued'}])
    finally:
        common.reset_tenant_context(tokens)

for tenant_id, expected in [('alpha', 'draft-alpha'), ('beta', 'draft-beta')]:
    tenant = tenant_manager.get_tenant(tenant_id)
    paths = tenant_manager.ensure_tenant_dirs(tenant_id)
    tokens = common.bind_tenant_context(tenant_id, tenant=tenant, paths=paths)
    try:
        data = common.read_json(common.DRAFTS_FILE, {})
        assert data['row']['reply'] == expected
    finally:
        common.reset_tenant_context(tokens)

# 3. Safe file writes under concurrency.
atomic_target = TEMP_ROOT / 'atomic.json'


def _atomic_writer(name: str, rounds: int = 20):
    for index in range(rounds):
        safe_files.write_json(atomic_target, {'worker': name, 'round': index})


threads = [threading.Thread(target=_atomic_writer, args=('alpha',)), threading.Thread(target=_atomic_writer, args=('beta',))]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join(timeout=5)
final_atomic = safe_files.read_json(atomic_target, {})
assert final_atomic.get('worker') in {'alpha', 'beta'}
assert isinstance(final_atomic.get('round'), int)

# 4. Background jobs: dedupe, completion, failure, restart recovery.

def slow_job(delay: float = 0.2):
    time.sleep(delay)
    return {'ok': True}


job1, created1 = background_jobs.submit_job(kind='questions_process', tenant_id='alpha', label='Questions process', target=slow_job, kwargs={'delay': 0.5}, unique_key='questions_process')
job2, created2 = background_jobs.submit_job(kind='questions_process', tenant_id='alpha', label='Questions process', target=slow_job, kwargs={'delay': 0.5}, unique_key='questions_process')
assert created1 is True
assert created2 is False
assert job1['job_id'] == job2['job_id']
assert _wait_job(job1['job_id']).get('status') == 'completed'


def bad_job():
    raise RuntimeError('boom')


job_err, created_err = background_jobs.submit_job(kind='complaints_process', tenant_id='alpha', label='Complaints process', target=bad_job, unique_key='complaints_process')
assert created_err is True
assert _wait_job(job_err['job_id']).get('status') == 'error'

recent_ts = common.utc_now_iso()
stale_jobs_path = tenant_manager.ensure_tenant_dirs('alpha')['jobs_file']
stale_jobs = tenant_manager.read_json(stale_jobs_path, [])
stale_jobs.append({
    'job_id': 'restart-job-1',
    'tenant_id': 'alpha',
    'kind': 'replies_process',
    'label': 'Restarted running job',
    'unique_key': 'replies_process',
    'singleton_key': 'replies_process::alpha::replies_process',
    'status': 'running',
    'created_at': recent_ts,
    'started_at': recent_ts,
    'updated_at': recent_ts,
    'last_heartbeat_at': recent_ts,
    'last_message': 'running before restart',
    'result': None,
    'error': '',
    'progress': {},
})
tenant_manager.write_json(stale_jobs_path, stale_jobs)
assert any(job.get('job_id') == 'restart-job-1' and job.get('status') == 'abandoned' for job in background_jobs.list_jobs('alpha', limit=50))

# 5. Concurrent background jobs across tenants should not mix context.

def tenant_write_job(label: str, delay: float = 0.2):
    common.write_json(common.DRAFTS_FILE, {'row': {'reply': label}})
    time.sleep(delay)
    return {'reply': label}


alpha_job, alpha_created = background_jobs.submit_job(kind='tenant_write', tenant_id='alpha', label='Alpha write', target=tenant_write_job, kwargs={'label': 'tenant-alpha', 'delay': 0.25}, unique_key='tenant_write')
beta_job, beta_created = background_jobs.submit_job(kind='tenant_write', tenant_id='beta', label='Beta write', target=tenant_write_job, kwargs={'label': 'tenant-beta', 'delay': 0.25}, unique_key='tenant_write')
assert alpha_created is True and beta_created is True
assert _wait_job(alpha_job['job_id']).get('status') == 'completed'
assert _wait_job(beta_job['job_id']).get('status') == 'completed'

for tenant_id, expected in [('alpha', 'tenant-alpha'), ('beta', 'tenant-beta')]:
    tenant = tenant_manager.get_tenant(tenant_id)
    paths = tenant_manager.ensure_tenant_dirs(tenant_id)
    tokens = common.bind_tenant_context(tenant_id, tenant=tenant, paths=paths)
    try:
        data = common.read_json(common.DRAFTS_FILE, {})
        assert data['row']['reply'] == expected
    finally:
        common.reset_tenant_context(tokens)

# 6. History sync database lifecycle.
history_service.ensure_db('alpha')
snapshot = {
    'fetched_at': common.utc_now_iso(),
    'count_unanswered': 1,
    'feedbacks': [
        {
            'id': 'review-1',
            'text': 'Товар хороший',
            'pros': 'качество',
            'cons': '',
            'productValuation': 5,
            'createdDate': '2025-01-01T00:00:00+00:00',
            'userName': 'Иван',
            'subjectName': 'Категория',
            'productDetails': {'productName': 'Товар', 'supplierArticle': 'ART-1', 'brandName': 'Brand', 'nmId': 101},
        }
    ],
}
inserted = history_service.upsert_active_snapshot(snapshot, 'alpha')
assert inserted == 1
counts = history_service.get_counts('alpha')
assert counts['total'] == 1 and counts['needs_reply'] == 1
history_service.mark_replied('review-1', 'Спасибо за отзыв', 'alpha')
counts = history_service.get_counts('alpha')
assert counts['needs_reply'] == 0

# 7. Complaints results analytics.
paths = tenant_manager.ensure_tenant_dirs('alpha')
tokens = common.bind_tenant_context('alpha', tenant=tenant_manager.get_tenant('alpha'), paths=paths)
try:
    complaint_core.append_result({'review_id': 'c1', 'status': 'accepted', 'category': 'Другое', 'review': {'product_name': 'Товар'}})
    complaint_core.append_result({'review_id': 'c2', 'status': 'rejected', 'category': 'Другое', 'review': {'product_name': 'Товар'}})
    complaint_core.append_result({'review_id': 'c3', 'outcome': 'pending', 'status': 'submitted', 'category': 'Другое', 'review': {'product_name': 'Товар'}})
    metrics = complaint_core.build_complaint_effectiveness(limit=100)
    assert metrics['accepted'] == 1
    assert metrics['rejected'] == 1
    assert metrics['pending'] >= 1
finally:
    common.reset_tenant_context(tokens)

# 8. Question queue prepare/process without live WB network.
paths = tenant_manager.ensure_tenant_dirs('alpha')
tokens = common.bind_tenant_context('alpha', tenant=tenant_manager.get_tenant('alpha'), paths=paths)
try:
    question = {
        'id': 'q-1',
        'text': 'Подойдет ли для зимы?',
        'createdDate': '2025-01-01T00:00:00+00:00',
        'state': 'none',
        'subjectName': 'Категория',
        'answer': None,
        'productDetails': {'productName': 'Куртка', 'supplierArticle': 'JACKET-1', 'brandName': 'Brand', 'nmId': 777, 'size': 'M', 'imtId': 555, 'supplierName': 'Seller'},
        'wasViewed': False,
        'isWarned': False,
    }
    common.write_json(common.QUESTION_SNAPSHOT_FILE, {'questions': [question], 'count_unanswered': 1})
    common.write_json(common.QUESTION_DRAFTS_FILE, {
        'q-1': {
            'reply': 'Да, модель рассчитана на зимний сезон и хорошо сохраняет тепло.',
            'action': 'answer',
            'manual_action': 'answer',
            'manager_comment': '',
            'confidence': 0.95,
            'auto_ready': True,
            'cluster_key': 'other',
            'cluster_title': 'Прочее',
            'signature': question_core.question_signature(common.normalize_question(question)),
            'source': 'manual',
            'needs_regeneration': False,
        }
    })

    class Form:
        def getlist(self, name: str):
            if name == 'selected_ids':
                return ['q-1']
            return []

        def get(self, name: str, default=None):
            values = {
                'action__q-1': 'answer',
                'reply__q-1': 'Да, модель рассчитана на зимний сезон и хорошо сохраняет тепло.',
                'manager_comment__q-1': '',
            }
            return values.get(name, default)

    original_get_snapshot = question_core.get_question_snapshot
    original_refresh_snapshot = question_core.refresh_question_snapshot
    original_patch = common.patch_question
    question_core.get_question_snapshot = lambda force_refresh=False: common.read_json(common.QUESTION_SNAPSHOT_FILE, {})
    question_core.refresh_question_snapshot = lambda: common.read_json(common.QUESTION_SNAPSHOT_FILE, {})
    added, notes = question_core.queue_questions_from_form(Form())
    assert added == 1
    sent_payloads = []
    common.patch_question = lambda payload: sent_payloads.append(payload) or {'ok': True}
    try:
        result = question_core.process_question_queue(max_items=10, auto_only=False)
    finally:
        question_core.get_question_snapshot = original_get_snapshot
        question_core.refresh_question_snapshot = original_refresh_snapshot
        common.patch_question = original_patch
    assert result['sent'] == 1
    assert sent_payloads and sent_payloads[0]['id'] == 'q-1'
finally:
    common.reset_tenant_context(tokens)

# 9. Promo checkbox DOM fallback helpers.
class _FakePromoPage:
    def __init__(self):
        self.calls = []

    def evaluate(self, script, arg):
        self.calls.append((script, arg))
        return {"ok": True, "mode": "dom_dispatch"}

fake_promo_page = _FakePromoPage()
assert promo_executor._force_confirm_checkbox_via_dom(fake_promo_page, promo_executor.DEFAULT_PROMO_PROFILE) is True
assert fake_promo_page.calls and "selectors" in fake_promo_page.calls[0][1]

class _FakeCheckbox:
    def __init__(self, checked: bool = False):
        self._checked = checked

    def is_checked(self):
        return self._checked

class _FakePromoPageUnchecked:
    def evaluate(self, script, arg):
        return False

    def wait_for_timeout(self, timeout):
        return None

class _FakePromoPageChecked:
    def evaluate(self, script, arg):
        return True

    def wait_for_timeout(self, timeout):
        return None

assert promo_executor._wait_confirmation_applied(_FakePromoPageUnchecked(), _FakeCheckbox(False), promo_executor.DEFAULT_PROMO_PROFILE, timeout_ms=50) is False
assert promo_executor._wait_confirmation_applied(_FakePromoPageChecked(), _FakeCheckbox(False), promo_executor.DEFAULT_PROMO_PROFILE, timeout_ms=50) is True

# 10. Browser security helpers.
assert browser_bot._is_allowed_navigation_url('https://seller.wildberries.ru/reviews') is True
assert browser_bot._is_allowed_navigation_url('https://example.com') is False
assert browser_bot._is_allowed_request_url('https://static-basket-01.wb.ru') is True
assert browser_bot._is_allowed_request_url('https://malicious.example.com/evil.js') is False
try:
    browser_bot._assert_allowed_navigation_url('https://example.com/phishing')
    raise AssertionError('Disallowed navigation URL was accepted')
except browser_bot.BrowserBotError:
    pass

print(json.dumps({
    'status': 'SMOKE_OK',
    'private_root': str(TEMP_ROOT),
    'auth_bootstrap': True,
    'tenant_isolation': True,
    'safe_file_writes': True,
    'background_jobs': True,
    'concurrency': True,
    'history_sync': True,
    'complaints': True,
    'questions': True,
    'browser_security': True,
    'promo_checkbox_dom_force': True,
}, ensure_ascii=False))

shutil.rmtree(TEMP_ROOT, ignore_errors=True)
