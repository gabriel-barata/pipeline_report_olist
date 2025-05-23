import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    return (
        SparkSession.builder.master('local[1]')
        .appName('pytest-products-job')
        .config('spark.sql.shuffle.partitions', '1')
        .config('spark.driver.bindAddress', '127.0.0.1')
        .getOrCreate()
    )


@pytest.fixture(scope='session')
def source_order_items(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                '00010242fe8c5a6d1ba2dd792cb16214',
                '1',
                '4244733e06e7ecb4970a6e2683c13e61',
                '48436dade18ac8b2bce089ec2a041202',
                '2017-09-19 09:45:35',
                '58.90',
                '13.29',
                '2025',
                '05',
                '22',
            )
        ],
        [
            'order_id',
            'order_item_id',
            'product_id',
            'seller_id',
            'shipping_limit_date',
            'price',
            'freight_value',
            'YEAR',
            'MONTH',
            'DAY',
        ],
    )


@pytest.fixture(scope='session')
def source_products(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                '4244733e06e7ecb4970a6e2683c13e61',
                'perfumaria',
                40,
                287,
                1,
                225,
                16,
                10,
                14,
                '2025',
                '05',
                '22',
            )
        ],
        [
            'product_id',
            'product_category_name',
            'product_name_lenght',
            'product_description_lenght',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm',
            'YEAR',
            'MONTH',
            'DAY',
        ],
    )
