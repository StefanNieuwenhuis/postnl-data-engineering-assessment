from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Data schema for RAW source data
SCHEMA_RAW_ROUTES = StructType(
    [
        StructField("route_id", StringType(), nullable=False),
        StructField("origin_region", StringType(), nullable=True),
        StructField("destination_region", StringType(), nullable=True),
        StructField("origin_city", StringType(), nullable=True),
        StructField("destination_city", StringType(), nullable=True),
        StructField("distance_km", DoubleType(), nullable=True),
        StructField("avg_speed_kmh", DoubleType(), nullable=True),
        StructField("road_type", StringType(), nullable=True),
        StructField("toll_eur", DoubleType(), nullable=True),
    ]
)
