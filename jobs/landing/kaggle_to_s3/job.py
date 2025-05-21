from jobs.base.job import Job
from jobs.base.variables import KAGGLE_FILES, LANDING_BUCKET, SAMPLE_DATA_PATH


class KaggleToS3Job(Job):
    target = LANDING_BUCKET + 'kaggle/'

    def run(self) -> None:
        for table_name, file_name in KAGGLE_FILES.items():
            file_path = str(SAMPLE_DATA_PATH / file_name)
            output_path = self.target + table_name

            data = self.spark.read.csv(
                file_path, sep=',', header=True, inferSchema=True
            )

            data = self.generate_partition_columns(data)

            (
                data.write.mode('overwrite')
                .partitionBy('YEAR', 'MONTH', 'DAY')
                .csv(output_path, header=True, sep=',')
            )
