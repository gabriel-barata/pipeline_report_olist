from jobs.base.job import Job
from jobs.base.variables import CURATED_BUCKET, RAW_BUCKET


class SellersJob(Job):
    sources = {'': ...}
    target = ''

    def run(self) -> None:
        ...
