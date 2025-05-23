from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
)

from airflow import Dataset

DAG_ID = 'olist_report_pipeline'
APPLICATION_ID = Variable.get('OLIST_REPORT_EMR_APP_NAME')
EXECUTION_ROLE_ARN = Variable.get('OLIST_REPORT_EMR_APP_ROLE_ARN')
REF_DATE_DELAY = -1
REF_DATE = '{{ ti.xcom_pull(task_ids="get_ref_date") }}'
DATE_FORMAT = '%Y-%m-%d'

CURATED_BUCKET = Variable.get('CURATED_BUCKET')
JOB_CODE_PATH = (
    's3://olist-datalake-prd-us-east-1-269012942764-source-code'
    + 'olist_report_pipeline/'
)


def get_emr_serverless_start_job_operator(
    task_id: str, job: str
) -> EmrServerlessStartJobOperator:
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        execution_role_arn=EXECUTION_ROLE_ARN,
        application_id=APPLICATION_ID,
        wait_for_completion=True,
        aws_conn_id='aws_default',
        job_driver={
            'sparkSubmit': {
                'entryPoint': f'{JOB_CODE_PATH}/jobs.zip',
                'entryPointArguments': [
                    job,
                    REF_DATE,
                ],
                'sparkSubmitParameters': (
                    f'--conf spark.archives={JOB_CODE_PATH}/jobs/jobs.zip'
                    f'--py-files {JOB_CODE_PATH}/jobs/jobs.zip'
                ),
            }
        },
    )


@task(outlets=[Dataset(CURATED_BUCKET)])
def register_outlet() -> None:
    """
    Dummy task used just to register an outlet
    """
    return None


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['AUTOGLASS', 'OLIST', 'REPORT'],
)
def olist_report_pipeline() -> None:
    @task
    def get_ref_date(ds: str) -> str:
        return (
            datetime.strptime(ds, DATE_FORMAT) + timedelta(days=REF_DATE_DELAY)
        ).strftime(DATE_FORMAT)

    @task_group
    def landing() -> None:
        landing_api_job = get_emr_serverless_start_job_operator(
            task_id='api_to_s3', job='landing.api_to_s3'
        )

        landing_api_job

    @task_group
    def raw() -> None:
        api = get_emr_serverless_start_job_operator(
            task_id='raw_api', job='raw.api'
        )

        kaggle = get_emr_serverless_start_job_operator(
            task_id='raw_kaggle', job='raw.kaggle'
        )

        [api, kaggle]

    @task_group
    def curated() -> None:
        sales_per_state = get_emr_serverless_start_job_operator(
            task_id='sales_per_state', job='curated.sales_per_state'
        )

        sales_consolidated = get_emr_serverless_start_job_operator(
            task_id='sales_comnsolidated', job='curated.sales_consolidated'
        )

        products = get_emr_serverless_start_job_operator(
            task_id='products', job='curated.products'
        )

        [sales_per_state, sales_consolidated, products]

    (get_ref_date() >> landing() >> raw() >> curated() >> register_outlet())


olist_report_pipeline()
