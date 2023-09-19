
from dagster import Definitions
from .assets.bronze import raw_customer, raw_product, raw_productcategory, raw_productsubcategory, raw_territory, raw_orders
from .assets.silver import clean_customer, fact_sales
from .assets.gold import sales_by_territory,sales_by_category,Loyal_customer
from .assets.warehouse import dwh_sales_by_category,dwh_sales_by_territory,dwh_Loyal_customer


from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager


MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "database": "adventure_work",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "demo_pg",
    "user": "admin",
    "password": "admin123",
}

defs = Definitions(
    assets=[raw_customer, raw_product, raw_productcategory, raw_productsubcategory, raw_territory, raw_orders,clean_customer,fact_sales,sales_by_territory,sales_by_category,Loyal_customer,dwh_Loyal_customer,dwh_sales_by_category,dwh_sales_by_territory],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)
