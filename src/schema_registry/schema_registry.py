from pyspark.sql.types import StringType, StructField, StructType

from schema_registry.routes_schema import SCHEMA_RAW_ROUTES
from schema_registry.shipments_schema import SCHEMA_RAW_SHIPMENTS
from schema_registry.vehicles_schema import SCHEMA_RAW_VEHICLES
from schema_registry.weather_schema import SCHEMA_RAW_WEATHER

CORRUPT_RECORD_COLUMN = "_corrupt_record"


def schema_with_corrupt_record(schema: StructType) -> StructType:
    """Add _corrupt_record field to schema for columnNameOfCorruptRecord support."""
    if any(f.name == CORRUPT_RECORD_COLUMN for f in schema.fields):
        return schema
    return StructType(
        list(schema.fields) + [StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True)]
    )


SCHEMAS = {
    "routes": SCHEMA_RAW_ROUTES,
    "shipments": SCHEMA_RAW_SHIPMENTS,
    "vehicles": SCHEMA_RAW_VEHICLES,
    "weather": SCHEMA_RAW_WEATHER,
}
