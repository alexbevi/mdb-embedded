# Submodule imports trigger @_register decorators that populate _HANDLERS.
# Do NOT remove these -- they look unused but the side effect is load-bearing.
from . import admin as _admin  # noqa: F401
from . import aggregation as _aggregation  # noqa: F401
from . import crud as _crud  # noqa: F401
from . import diagnostic as _diagnostic  # noqa: F401
from . import handshake as _handshake  # noqa: F401
from . import indexes as _indexes  # noqa: F401
from . import sessions as _sessions  # noqa: F401
from . import users as _users  # noqa: F401
from ._registry import (
    _HANDLERS,
    _HELP,
    MAX_BSON_OBJECT_SIZE,
    MAX_MESSAGE_SIZE,
    MAX_WRITE_BATCH_SIZE,
    dispatch,
)

__all__ = [
    "MAX_BSON_OBJECT_SIZE",
    "MAX_MESSAGE_SIZE",
    "MAX_WRITE_BATCH_SIZE",
    "_HANDLERS",
    "_HELP",
    "dispatch",
]
