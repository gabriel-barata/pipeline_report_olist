from unittest.mock import PropertyMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from jobs.curated.sales_per_state.job import SalesPerStateJob


@pytest.fixture(scope='module')
def sales_consolidated_job(spark: SparkSession) -> SalesPerStateJob:
    return SalesPerStateJob(
        spark=spark, is_test=True, config={'ref_date': '2025-05-22'}
    )


def test_job(
    sales_consolidated_job: SalesPerStateJob,
    source_order_items: DataFrame,
    source_orders: DataFrame,
    source_customers: DataFrame,
    snapshot,
) -> None:

    with (
        patch.object(
            SalesPerStateJob,
            'source_order_items',
            new_callable=PropertyMock,
        ) as mock_order_items,
        patch.object(
            SalesPerStateJob, 'source_orders', new_callable=PropertyMock
        ) as mock_orders,
        patch.object(
            SalesPerStateJob, 'source_customers', new_callable=PropertyMock
        ) as mock_customers,
    ):

        mock_order_items.return_value = source_order_items
        mock_orders.return_value = source_orders
        mock_customers.return_value = source_customers

        output = sales_consolidated_job.run()

        assert output.collect() == snapshot
