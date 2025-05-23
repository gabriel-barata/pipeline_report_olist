import requests
from pyspark.sql.types import StringType, StructField, StructType

from jobs.base.job import Job
from jobs.base.variables import LANDING_BUCKET


class ApiToS3Job(Job):
    target = LANDING_BUCKET + 'api_CNAE/'

    def run(self) -> None:
        data = self.spark.createDataFrame(
            self.fetch_api_data(), schema=self.schema
        )

        data = self.generate_partition_columns(data)

        (
            data.write.mode('overwrite')
            .partitionBy('YEAR', 'MONTH', 'DAY')
            .json(self.target)
        )

    def fetch_api_data(self) -> dict:
        response = requests.get(self.url)
        response.raise_for_status()

        return response.json()

    @property
    def url(self) -> str:
        return 'https://servicodados.ibge.gov.br/api/v2/cnae/classes'

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
