from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
SAMPLE_DATA_PATH = ROOT / 'resources' / 'sample_data'

LANDING_BUCKET = 's3a://olist-datalake-prd-us-east-1-269012942764-landing/'
RAW_BUCKET = 's3a://olist-datalake-prd-us-east-1-269012942764-raw/'
CURATED_BUCKET = 's3a://olist-datalake-prd-us-east-1-269012942764-curated/'

KAGGLE_FILES = {
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
}
