"""Mock heavy dependencies so main.py can be imported without them installed.

Import this module BEFORE importing main.py in any test file.
"""
import sys
import types


def _make_mock_module(name: str) -> types.ModuleType:
    """Create a mock module with a flexible __getattr__."""
    mod = types.ModuleType(name)
    mod.__getattr__ = lambda self_name: type(self_name, (), {"__init__": lambda *a, **k: None})
    return mod


# Modules to mock
_MOCK_MODULES = [
    # Databricks SDK
    "databricks", "databricks.sdk", "databricks.sdk.service",
    "databricks.sdk.service.sql",
    # FastAPI
    "fastapi", "fastapi.responses", "fastapi.staticfiles",
    # SQLAlchemy
    "sqlalchemy", "sqlalchemy.event", "sqlalchemy.ext", "sqlalchemy.ext.asyncio",
]

for mod_name in _MOCK_MODULES:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = _make_mock_module(mod_name)

# Provide specific attributes that main.py accesses at import time
sys.modules["databricks.sdk"].WorkspaceClient = type("WorkspaceClient", (), {})
sys.modules["databricks.sdk.service.sql"].StatementState = type("StatementState", (), {})

# FastAPI mocks
_FastAPI = type("FastAPI", (), {
    "__init__": lambda self, *a, **k: None,
    "get": lambda self, *a, **k: (lambda fn: fn),
    "post": lambda self, *a, **k: (lambda fn: fn),
    "mount": lambda self, *a, **k: None,
    "exception_handler": lambda self, *a, **k: (lambda fn: fn),
    "middleware": lambda self, *a, **k: (lambda fn: fn),
})
sys.modules["fastapi"].FastAPI = _FastAPI
sys.modules["fastapi"].HTTPException = type("HTTPException", (Exception,), {
    "__init__": lambda self, status_code=500, detail="": None,
})
sys.modules["fastapi"].Request = type("Request", (), {})
sys.modules["fastapi.responses"].HTMLResponse = type("HTMLResponse", (), {})
sys.modules["fastapi.responses"].JSONResponse = type("JSONResponse", (), {})
sys.modules["fastapi.staticfiles"].StaticFiles = type("StaticFiles", (), {
    "__init__": lambda self, *a, **k: None,
})

# SQLAlchemy mocks
sys.modules["sqlalchemy"].event = types.ModuleType("sqlalchemy.event")
sys.modules["sqlalchemy"].event.listens_for = lambda *a, **k: (lambda fn: fn)
sys.modules["sqlalchemy"].text = lambda x: x
sys.modules["sqlalchemy.ext.asyncio"].AsyncSession = type("AsyncSession", (), {})
sys.modules["sqlalchemy.ext.asyncio"].async_sessionmaker = lambda *a, **k: None
sys.modules["sqlalchemy.ext.asyncio"].create_async_engine = lambda *a, **k: None
