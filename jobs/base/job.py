import os
from abc import abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


class Job:
    def __init__(
        self,
        spark: SparkSession = None,
        config: dict[str, str] = None,
        is_test: bool = False,
    ):
        self.config = config or {}
        self._spark = spark
        self._is_test = is_test

    @property
    def ref_date(self) -> str:
        return self.config.get('ref_date')

    @property
    def is_test(self) -> bool:
        return self._is_test

    @property
    def aditional_packages(self) -> str:
        return ','.join(
            [
                'org.apache.hadoop:hadoop-aws:3.3.2',
                'com.amazonaws:aws-java-sdk-bundle:1.12.262',
                'io.delta:delta-spark_2.12:3.1.0',
            ]
        )

    @property
    def spark(self) -> SparkSession:
        if self._spark is not None:
            return self._spark

        builder = SparkSession.builder.appName(self.__class__.__name__)

        if os.environ.get('ENV', '').lower() != 'dev':
            return builder.getOrCreate()

        return (
            builder.master('local[*]')
            .config('spark.jars.packages', self.aditional_packages)
            .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')
            .config(
                'spark.hadoop.fs.s3a.access.key',
                os.getenv('AWS_ACCESS_KEY_ID'),
            )
            .config(
                'spark.hadoop.fs.s3a.secret.key',
                os.getenv('AWS_SECRET_ACCESS_KEY'),
            )
            .config(
                'spark.hadoop.fs.s3a.impl',
                'org.apache.hadoop.fs.s3a.S3AFileSystem',
            )
            .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')
            .config(
                'spark.sql.extensions',
                'io.delta.sql.DeltaSparkSessionExtension',
            )
            .config(
                'spark.sql.catalog.spark_catalog',
                'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            )
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
            (col('YEAR') == year)
            & (col('MONTH') == month)
            & (col('DAY') == day)
        )

    @abstractmethod
    def run(self) -> None:
        pass

    def save(
        self,
        df: DataFrame,
        target: str,
        mode: str = 'overwrite',
        file_format: str = 'delta',
    ) -> DataFrame | None:
        if self.is_test:
            return df
        df.write.mode(mode).format(file_format).save(target)
