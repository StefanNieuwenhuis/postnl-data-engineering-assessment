from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Data schema for RAW source data
SCHEMA_RAW_WEATHER = StructType(
    [
        StructField("date", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("condition", StringType(), nullable=True),
        StructField("temperature_c", DoubleType(), nullable=True),
        StructField("wind_kph", DoubleType(), nullable=True),
        StructField("precipitation_mm", DoubleType(), nullable=True),
    ]
)
