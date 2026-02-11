import pytest

from schema_registry.schema_registry import (
    CORRUPT_RECORD_COLUMN,
    schema_with_corrupt_record,
)
from schema_registry.shipments_schema import SCHEMA_RAW_SHIPMENTS


class TestSchemaRegistry:
    """Tests for schema registry helpers."""

    def test_schema_with_corrupt_record_adds_column(self) -> None:
        """schema_with_corrupt_record adds _corrupt_record when not present."""
        result = schema_with_corrupt_record(SCHEMA_RAW_SHIPMENTS)

        assert CORRUPT_RECORD_COLUMN in [f.name for f in result.fields]
        assert len(result.fields) == len(SCHEMA_RAW_SHIPMENTS.fields) + 1

    def test_schema_with_corrupt_record_idempotent(self) -> None:
        """schema_with_corrupt_record does not add duplicate when already present."""
        once = schema_with_corrupt_record(SCHEMA_RAW_SHIPMENTS)
        twice = schema_with_corrupt_record(once)

        assert len(twice.fields) == len(once.fields)
        assert sum(1 for f in twice.fields if f.name == CORRUPT_RECORD_COLUMN) == 1
