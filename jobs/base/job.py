import os
from abc import abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


class Job:
    def __init__(self, config: dict[str, str] = None):
        self.config = config or {}

    @property
    def ref_date(self) -> str:
        return self.config.get('ref_date')

    @property
    def spark(self) -> SparkSession:
        builder = SparkSession.builder.appName(self.__class__.__name__)

        if os.environ.get('ENV').lower() != 'dev':
            return builder.getOrCreate()

        return (
            builder.master('local[*]')
            .config(
                'spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262',
            )
            .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')
            .config('spark.hadoop.fs.s3a.access.key', os.getenv('AWS_ACCESS_KEY'))
            .config('spark.hadoop.fs.s3a.secret.key', os.getenv('AWS_SECRET_KEY'))
            .config(
                'spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem'
            )
            .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')
            .config('spark.driver.memory', '6g')
            .getOrCreate()
        )

    def generate_partition_columns(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn('YEAR', lit(self.ref_date[:4]))
            .withColumn('MONTH', lit(self.ref_date[5:7]))
            .withColumn('DAY', lit(self.ref_date[-2:]))
        )

    def filter_by_ref_date(self, df: DataFrame) -> DataFrame:
        year, month, day = (
            self.ref_date[:4],
            self.ref_date[5:7],
            self.ref_date[-2:],
        )

        return df.filter(
            (col('YEAR') == year) & (col('MONTH') == month) & (col('DAY') == day)
        )

    @abstractmethod
    def run(self) -> None:
        pass
