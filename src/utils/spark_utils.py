from pyspark.sql import DataFrame, SparkSession


def read_dataframe(spark: SparkSession, path_dir: str, data_format="delta") -> DataFrame:
    """
    Read from a data source

    :param spark: Active SparkSession
    :param path_dir: Source path
    :param data_format: Data format
    :return: DataFrame from the source data
    """
    return spark.read.format(data_format).load(path_dir)


def read_stream(spark: SparkSession, path_dir: str, data_format="delta") -> DataFrame:
    return spark.readStream.load(path_dir, format=data_format)