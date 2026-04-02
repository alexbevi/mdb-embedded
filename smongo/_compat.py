"""WiredTiger import compatibility shim.

Provides ``wt`` (the module or ``None``) and ``WTError`` (the real
``WiredTigerError`` when the C extension is available, otherwise a
benign stand-in ``Exception`` subclass).
"""

from __future__ import annotations

from typing import Any

wt: Any
WTError: type[Exception]

try:
    import wiredtiger as _wt

    wt = _wt
    WTError = _wt.WiredTigerError
except (ImportError, AttributeError):
    wt = None

    class WTError(Exception):  # type: ignore[no-redef]
        pass
