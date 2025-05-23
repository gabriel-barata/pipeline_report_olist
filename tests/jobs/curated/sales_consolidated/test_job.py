from unittest.mock import PropertyMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from jobs.curated.sales_consolidated.job import SalesConsolidatedJob


@pytest.fixture(scope='module')
def sales_consolidated_job(spark: SparkSession) -> SalesConsolidatedJob:
    return SalesConsolidatedJob(
        spark=spark, is_test=True, config={'ref_date': '2025-05-22'}
    )


def test_job(
    sales_consolidated_job: SalesConsolidatedJob,
    source_order_items: DataFrame,
    source_products: DataFrame,
    source_orders: DataFrame,
    source_customers: DataFrame,
    snapshot,
) -> None:

    with (
        patch.object(
            SalesConsolidatedJob, 'source_products', new_callable=PropertyMock
        ) as mock_products,
        patch.object(
            SalesConsolidatedJob,
            'source_order_items',
            new_callable=PropertyMock,
        ) as mock_order_items,
        patch.object(
            SalesConsolidatedJob, 'source_orders', new_callable=PropertyMock
        ) as mock_orders,
        patch.object(
            SalesConsolidatedJob, 'source_customers', new_callable=PropertyMock
        ) as mock_customers,
    ):

        mock_order_items.return_value = source_order_items
        mock_products.return_value = source_products
        mock_orders.return_value = source_orders
        mock_customers.return_value = source_customers

        output = sales_consolidated_job.run()

        assert output.collect() == snapshot
