from jobs.base.job import Job
from jobs.base.variables import KAGGLE_FILES, LANDING_BUCKET, RAW_BUCKET


class KaggleJob(Job):
    source = LANDING_BUCKET + 'kaggle/'
    target = RAW_BUCKET + 'kaggle/'

    def run(self) -> None:
        for file in KAGGLE_FILES.keys():
            source = self.source + file
            target = self.target + file

            data = self.spark.read.csv(source, header=True, inferSchema=True, sep=',')

            data = self.filter_by_ref_date(data)

            (
                data.write.mode('overwrite')
                .partitionBy('YEAR', 'MONTH', 'DAY')
                .parquet(target)
            )
