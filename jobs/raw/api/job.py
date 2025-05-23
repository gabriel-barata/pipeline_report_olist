from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

from jobs.base.job import Job
from jobs.base.variables import LANDING_BUCKET, RAW_BUCKET


class ApiJob(Job):
    source = LANDING_BUCKET + 'api_CNAE/'
    target = RAW_BUCKET + 'api_CNAE/'

    def run(self) -> None:
        data = self.spark.read.json(self.source, schema=self.schema)
        data = self.filter_by_ref_date(data)
        data = self.flatten_dataframe(data)

        (
            data.write.mode('overwrite')
            .partitionBy('YEAR', 'MONTH', 'DAY')
            .parquet(self.target)
        )

    @property
    def schema(self) -> StructType:
        return StructType(
            [
                StructField('id', StringType(), True),
                StructField('descricao', StringType(), True),
                StructField('observacoes', StringType(), True),
                StructField(
                    'grupo',
                    StructType(
                        [
                            StructField('id', StringType(), True),
                            StructField('descricao', StringType(), True),
                            StructField('observacoes', StringType(), True),
                            StructField(
                                'divisao',
                                StructType(
                                    [
                                        StructField('id', StringType(), True),
                                        StructField(
                                            'descricao', StringType(), True
                                        ),
                                        StructField(
                                            'observacoes', StringType(), True
                                        ),
                                        StructField(
                                            'secao',
                                            StructType(
                                                [
                                                    StructField(
                                                        'id',
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        'descricao',
                                                        StringType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        'observacoes',
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
            ]
        )

    @staticmethod
    def flatten_dataframe(df: DataFrame) -> DataFrame:
        return df.select(
            col('id'),
            col('descricao'),
            col('observacoes'),
            col('grupo.id').alias('grupo_id'),
            col('grupo.descricao').alias('grupo_descricao'),
            col('grupo.observacoes').alias('grupo_observacoes'),
            col('grupo.divisao.id').alias('divisao_id'),
            col('grupo.divisao.descricao').alias('divisao_descricao'),
            col('grupo.divisao.observacoes').alias('divisao_observacoes'),
            col('grupo.divisao.secao.id').alias('secao_id'),
            col('grupo.divisao.secao.descricao').alias('secao_descricao'),
            col('grupo.divisao.secao.observacoes').alias('secao_observacoes'),
            col('YEAR'),
            col('MONTH'),
            col('DAY'),
        )
