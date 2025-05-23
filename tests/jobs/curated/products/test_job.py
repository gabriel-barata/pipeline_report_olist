from unittest.mock import PropertyMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from jobs.curated.products.job import ProductsJob


@pytest.fixture(scope='module')
def products_job(spark: SparkSession) -> ProductsJob:
    return ProductsJob(
        spark=spark, is_test=True, config={'ref_date': '2025-05-22'}
    )


def test_job(
    products_job: ProductsJob,
    source_order_items: DataFrame,
    source_products: DataFrame,
    snapshot,
) -> None:

    with (
        patch.object(
            ProductsJob, 'source_products', new_callable=PropertyMock
        ) as mock_products,
        patch.object(
            ProductsJob, 'source_order_items', new_callable=PropertyMock
        ) as mock_order_items,
    ):

        mock_order_items.return_value = source_order_items
        mock_products.return_value = source_products

        output = products_job.run()

        assert output.collect() == snapshot
