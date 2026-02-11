from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Data schema for RAW source data (CSV)
SCHEMA_RAW_VEHICLES = StructType(
    [
        StructField("vehicle_id", StringType(), nullable=False),
        StructField("vehicle_type", StringType(), nullable=True),
        StructField("fuel_type", StringType(), nullable=True),
        StructField("emission_kg_per_km", DoubleType(), nullable=True),
        StructField("capacity_kg", DoubleType(), nullable=True),
        StructField("capacity_m3", DoubleType(), nullable=True),
    ]
)
