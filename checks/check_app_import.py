from __future__ import annotations

import os
import shutil
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMP_ROOT = Path(tempfile.mkdtemp(prefix='wb-app-import-'))
os.environ['WB_PRIVATE_DIR'] = str(TEMP_ROOT / 'private')
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

flask_stub = types.ModuleType('flask')


class DummyFlask:
    def __init__(self, name: str):
        self.name = name
        self.routes: list[dict] = []
        self.before_request_funcs = []
        self.after_request_funcs = []
        self.teardown_request_funcs = []
        self.context_processors = []
        self.config = {}
        self.secret_key = ''

    def route(self, rule: str, methods=None, **kwargs):
        methods = set(methods or ['GET'])
        def decorator(func):
            self.routes.append({'rule': rule, 'methods': methods, 'endpoint': func.__name__})
            return func
        return decorator

    def before_request(self, func):
        self.before_request_funcs.append(func)
        return func

    def after_request(self, func):
        self.after_request_funcs.append(func)
        return func

    def teardown_request(self, func):
        self.teardown_request_funcs.append(func)
        return func

    def context_processor(self, func):
        self.context_processors.append(func)
        return func

    def run(self, *args, **kwargs):
        return None


flask_stub.Flask = DummyFlask
flask_stub.flash = lambda *args, **kwargs: None
flask_stub.g = types.SimpleNamespace()
flask_stub.jsonify = lambda payload=None, **kwargs: payload if payload is not None else kwargs
flask_stub.redirect = lambda url: ('redirect', url)
flask_stub.render_template = lambda template_name, **context: {'template': template_name, 'context': context}
flask_stub.request = types.SimpleNamespace(method='GET', args={}, form={}, values={}, headers={}, endpoint='', host_url='http://localhost/', full_path='/', path='/', query_string=b'')
flask_stub.send_file = lambda *args, **kwargs: {'send_file': True}
flask_stub.session = {}
flask_stub.url_for = lambda endpoint, **values: f'/{endpoint}'

sys.modules['flask'] = flask_stub
for mod_name in ['config', 'safe_files', 'tenant_manager', 'common', 'background_jobs', 'history_service', 'auth_core', 'app']:
    if mod_name in sys.modules:
        del sys.modules[mod_name]

import app as app_module  # noqa: E402

app = app_module.app
rules = {(item['rule'], tuple(sorted(item['methods']))) for item in app.routes}
assert hasattr(app_module, 'setup_admin')
assert hasattr(app_module, 'login')
assert hasattr(app_module, 'logout')
assert any(rule == '/setup-admin' for rule, _ in rules)
assert any(rule == '/login' for rule, _ in rules)
assert any(rule == '/logout' for rule, _ in rules)
assert app.config.get('DEBUG') is False
assert bool(app.secret_key)
assert len(app.before_request_funcs) >= 1
assert len(app.after_request_funcs) >= 1
assert len(app.teardown_request_funcs) >= 1
print('APP_IMPORT_OK', len(app.routes))

shutil.rmtree(TEMP_ROOT, ignore_errors=True)
