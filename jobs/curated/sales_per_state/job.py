from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum

from jobs.base.job import Job
from jobs.base.variables import CURATED_BUCKET, RAW_BUCKET

SRC_BASE_PATH = RAW_BUCKET + 'kaggle/'


class SalesPerStateJob(Job):
    sources = {
        'orders': SRC_BASE_PATH + 'orders/',
        'order_items': SRC_BASE_PATH + 'order_items/',
        'customers': SRC_BASE_PATH + 'customers/',
    }
    target = CURATED_BUCKET + '/sales/sales_per_state'

    def run(self) -> None | DataFrame:
        orders = self.filter_by_ref_date(self.source_orders)
        order_items = self.filter_by_ref_date(self.source_order_items)
        customers = self.filter_by_ref_date(self.source_customers)

        data = orders.join(order_items, on='order_id', how='left').join(
            customers, on='customer_id', how='left'
        )

        data = data.groupBy('customer_state').agg(
            sum('price').alias('sales_total')
        )

        data = data.select(
            col('customer_state').alias('sales_customer_state'),
            col('sales_total').cast('decimal(18,2)'),
        )

        return self.save(data, self.target)

    @property
    def source_orders(self) -> DataFrame:
        return (
            self.spark.read.parquet(self.sources['orders'])
            .filter(col('order_status') == 'delivered')
            .select(
                col('order_id'),
                col('customer_id'),
                col('order_status'),
            )
        )

    @property
    def source_order_items(self) -> DataFrame:
        return self.spark.read.parquet(self.sources['order_items']).select(
            col('order_id'),
            col('product_id'),
            col('price'),
        )

    @property
    def source_customers(self) -> DataFrame:
        return self.spark.read.parquet(self.sources['customers']).select(
            col('customer_id'),
            col('customer_state'),
        )
