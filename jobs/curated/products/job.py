from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, when

from jobs.base.job import Job
from jobs.base.variables import CURATED_BUCKET, RAW_BUCKET


class ProductsJob(Job):
    sources = {
        'products': RAW_BUCKET + 'kaggle/products',
        'order_items': RAW_BUCKET + 'kaggle/order_items',
    }
    target = CURATED_BUCKET + 'products/products'

    def run(self) -> None:
        products = self.spark.read.parquet(self.sources['products'])
        products = self.filter_by_ref_date(products)
        order_items = self.source_order_items

        mean_product_value = order_items.groupBy(col('product_id')).agg(
            avg('price').alias('avg_price')
        )

        q1, q2 = mean_product_value.approxQuantile('avg_price', [0.33, 0.66], 0.01)

        mean_product_value = mean_product_value.withColumn(
            'price_category',
            when(col('avg_price') <= q1, 'baixo')
            .when((col('avg_price') > q1) & (col('avg_price') <= q2), 'medio')
            .otherwise('alto'),
        )

        products = products.join(mean_product_value, on='product_id', how='left')

        products = products.select(
            col('product_id'),
            col('product_category_name'),
            col('product_name_lenght'),
            col('product_description_lenght'),
            col('product_photos_qty'),
            col('product_weight_g'),
            col('product_length_cm'),
            col('product_height_cm'),
            col('price_category'),
            col('YEAR'),
            col('MONTH'),
            col('DAY'),
        )

        (products.write.mode('overwrite').format('delta').save(self.target))

    @property
    def source_order_items(self) -> DataFrame:
        return self.spark.read.parquet(self.sources['order_items']).select(
            col('product_id'),
            col('price'),
        )
