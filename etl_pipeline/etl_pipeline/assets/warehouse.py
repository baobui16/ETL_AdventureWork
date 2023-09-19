import pandas as pd
from dagster import asset, Output, AssetIn, multi_asset, AssetOut

@multi_asset(
    ins={
        "sales_by_category": AssetIn(key_prefix=["gold", "ecom"],)
    },
    outs={
        "sales_by_category": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="WAREHOUSE",
            metadata={
                "primary_keys": [
                    "Date",
                    "CategoryName",
                ],
                "columns": [
                    "Date",
                    "CategoryName",
                    "Total_bill_sum",
                    "Profit_sum",
                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_sales_by_category(sales_by_category) -> Output[pd.DataFrame]:
    return Output(
        sales_by_category,
        metadata={
            "schema": "public",
            "table": "sales_by_category",
            "records counts": len(sales_by_category),
        },
    )

@multi_asset(
    ins={
        "sales_by_territory": AssetIn(key_prefix=["gold", "ecom"],)
    },
    outs={
        "sales_by_territory": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="WAREHOUSE",
            metadata={
                "primary_keys": [
                    "Date",
                    "Region",
                ],
                "columns": [
                    "Date",
                    "Region",
                    "Total_bill_sum",
                    "Profit_sum",
                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_sales_by_territory(sales_by_territory) -> Output[pd.DataFrame]:
    return Output(
        sales_by_territory,
        metadata={
            "schema": "public",
            "table": "sales_by_territory",
            "records counts": len(sales_by_territory),
        },
    )
@multi_asset(
    ins={
        "Loyal_customer": AssetIn(key_prefix=["gold", "ecom"],)
    },
    outs={
        "Loyal_customer": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="WAREHOUSE",
            metadata={
                "primary_keys": [
                    "CustomerKey",
                ],
                "columns": [
                    "CustomerKey",
                    "Prefix",
                    "FirstName",
                    "LastName",
                    "EmailAddress",
                    "Total"
                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_Loyal_customer(Loyal_customer) -> Output[pd.DataFrame]:
    return Output(
        Loyal_customer,
        metadata={
            "schema": "public",
            "table": "Loyal_customer",
            "records counts": len(Loyal_customer),
        },
    )