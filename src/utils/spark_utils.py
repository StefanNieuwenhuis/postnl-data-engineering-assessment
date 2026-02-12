import logging
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class SparkUtils:

    @staticmethod
    def add_metadata(
        df: DataFrame, run_id: str, source: str, source_file: Optional[str] = None
    ) -> DataFrame:
        """
        Add metadata columns to a DataFrame

        :param df: DataFrame to add metadata columns
            :param run_id: The current pipeline run ID
            :param source: Source system / dataset name
            :param source_file: full path of the ingested file or directory (for streaming)
            :return: DataFrame enriched with layer metadata columns
        """

        # batch-df only: return empty DataFrame unaltered
        if not df.isStreaming and df.isEmpty():
            return df

        result = (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("ingestion_date", F.current_date())
            .withColumn("run_id", F.lit(run_id))
            .withColumn("source_system", F.lit(source))
        )

        if source_file is not None:
            result = result.withColumn("source_file", F.lit(source_file))
        else:
            result = result.withColumn("source_file", F.input_file_name())

        return result
