"""Avito channel module for the WB assistant build.

The module is intentionally resilient to partial environments: core service, API,
storage and AI pieces can be imported without Flask, while the Blueprint helpers
become available when Flask exists in the host application environment.
"""

from .config import AvitoModuleConfig
from .service import AvitoService

try:  # Flask may be absent in isolated test environments.
    from .blueprint import avito_bp, register_avito_module, create_standalone_app
except Exception:  # pragma: no cover - optional import guard.
    avito_bp = None
    register_avito_module = None
    create_standalone_app = None

__all__ = [
    "avito_bp",
    "register_avito_module",
    "create_standalone_app",
    "AvitoService",
    "AvitoModuleConfig",
]
