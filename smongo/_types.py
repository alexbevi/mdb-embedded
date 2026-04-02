"""Shared type aliases for the smongo package."""

from collections.abc import Callable
from typing import Any

Document = dict[str, Any]
Filter = dict[str, Any]
UpdateSpec = dict[str, Any]
Pipeline = list[dict[str, Any]]
Projection = dict[str, Any] | list[str]
IndexKeys = str | list[tuple[str, int]]
SortSpec = list[tuple[str, int]]
Predicate = Callable[[Document], bool]
CollectionGetter = Callable[[str], Any]
