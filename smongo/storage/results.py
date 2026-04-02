from __future__ import annotations

from typing import Any


class InsertResult:
    """Result of an insert_one or insert_many operation."""

    def __init__(self, inserted_ids: list[Any]) -> None:
        self.inserted_ids = inserted_ids


class UpdateResult:
    """Result of an update_one, update_many, or replace operation."""

    def __init__(self, matched_count: int, modified_count: int, upserted_id: Any = None) -> None:
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.upserted_id = upserted_id


class DeleteResult:
    """Result of a delete_one or delete_many operation."""

    def __init__(self, deleted_count: int) -> None:
        self.deleted_count = deleted_count
