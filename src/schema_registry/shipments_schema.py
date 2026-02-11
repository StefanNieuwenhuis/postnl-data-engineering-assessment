from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Data schema for RAW source data (CSV)
SCHEMA_RAW_SHIPMENTS = StructType(
    [
        StructField("shipment_id", StringType(), nullable=False),
        StructField("route_id", StringType(), nullable=True),
        StructField("vehicle_id", StringType(), nullable=True),
        StructField("carrier_id", StringType(), nullable=True),
        StructField("origin", StringType(), nullable=True),
        StructField("destination", StringType(), nullable=True),
        StructField("ship_date", StringType(), nullable=True),
        StructField("planned_arrival", StringType(), nullable=True),
        StructField("actual_arrival", StringType(), nullable=True),
        StructField("weight_kg", DoubleType(), nullable=True),
        StructField("volume_m3", DoubleType(), nullable=True),
    ]
)
