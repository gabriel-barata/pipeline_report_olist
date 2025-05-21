from pathlib import Path

from jobs.base.job import Job

BASE_LOCAL_PATH = (
    Path(__file__).parent.parent.parent.parent / 'resources' / 'sample_data'
)
BASE_S3_PATH = 's3a://olist-datalake-prd-us-east-1-269012942764-landing/kaggle/'

FILES = {
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'products': 'olist_products_dataset.csv',
}


class KaggleToS3Job(Job):
    def run(self) -> None:
        for table_name, file_name in FILES.items():
            file_path = str(BASE_LOCAL_PATH / file_name)
            output_path = BASE_S3_PATH + table_name

            data = self.spark.read.csv(
                file_path, sep=',', header=True, inferSchema=True
            )

            data = self.generate_partition_columns(data)

            (
                data.write.mode('overwrite')
                .partitionBy('YEAR', 'MONTH', 'DAY')
                .parquet(output_path)
            )
