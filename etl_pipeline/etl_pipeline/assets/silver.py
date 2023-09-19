import pandas as pd
import numpy as np
from dagster import asset, Output, AssetIn
from datetime import datetime

@asset(
    ins = {
        "raw_customer"  : AssetIn(key_prefix = ["bronze", "ecom"]),
    },
    key_prefix=["silver", "ecom"],
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    group_name="SILVER",
    compute_kind="Pandas"
)
def clean_customer(raw_customer: pd.DataFrame) -> Output[pd.DataFrame]:
    pd_data = raw_customer.copy()
    pd_data["Prefix"] = pd_data["Prefix"].replace('', np.nan).fillna('NA')
    pd_data["Gender"] = pd_data["Gender"].replace('', np.nan).fillna('NA')
    return Output(
        pd_data,
        metadata={
            "table": "customer",
            "records counts": len(pd_data),
        },
    )


@asset(
    ins = {
        "raw_product"  : AssetIn(key_prefix = ["bronze", "ecom"]),
        "raw_orders"  : AssetIn(key_prefix = ["bronze", "ecom"]),
    },
    key_prefix=["silver", "ecom"],
    io_manager_key="minio_io_manager",
    group_name="SILVER",
    compute_kind="Pandas"
)
def fact_sales(raw_product: pd.DataFrame, raw_orders: pd.DataFrame) -> Output[pd.DataFrame]:

    product = raw_product.copy()
    orders = raw_orders.copy()
    pd_data = pd.merge(product,orders,on='ProductKey')
    pd_data["Total_bill"] = pd_data["OrderQuantity"] * pd_data["ProductPrice"]
    pd_data["Profit"] = pd_data["OrderQuantity"]*(pd_data["ProductPrice"] - pd_data["ProductCost"])

    return Output(
        pd_data,
        metadata={
            "table": "fact_sales",
            "records counts": len(pd_data),
        },
    )

