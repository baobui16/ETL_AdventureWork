import pandas as pd
from dagster import asset, Output, AssetIn, multi_asset, AssetOut
from datetime import datetime

@asset(
    ins={
        "fact_sales": AssetIn(key_prefix=["silver", "ecom"]),
        "raw_productcategory": AssetIn(key_prefix=["bronze", "ecom"]),
        "raw_productsubcategory": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    key_prefix=["gold", "ecom"],
    io_manager_key="minio_io_manager",
    group_name="GOLD",
    compute_kind="Pandas"
)
def sales_by_category(fact_sales: pd.DataFrame,raw_productcategory: pd.DataFrame,raw_productsubcategory: pd.DataFrame) -> Output[pd.DataFrame]:

    factsales = fact_sales.copy()
    cat = raw_productcategory.copy()
    subcat = raw_productsubcategory.copy()

    df = pd.merge(factsales, subcat, on="ProductSubCategoryKey")
    df = df.merge(cat,on= "ProductCategoryKey",how="left")

    df['date'] = pd.to_datetime(df['OrderDate'], format='%Y-%m-%d')
    df1 = df.groupby([df['date'].dt.strftime('%Y-%m'), 'CategoryName'])['Total_bill'].sum().reset_index()
    df1.rename(columns={'date': 'Date', 'Total_bill': 'Total_bill_sum'}, inplace=True)

    df2 = df.groupby([df['date'].dt.strftime('%Y-%m'), 'CategoryName'])['Profit'].sum().reset_index()
    df2.rename(columns={'date': 'Date', 'Profit': 'Profit_sum'}, inplace=True)

    pd_data = df1.merge(df2, on=['Date', 'CategoryName'])

    # Sắp xếp kết quả theo cột "Date" theo thứ tự tăng dần
    pd_data = pd_data.sort_values(by='Date', ascending=True)

    return Output(
        pd_data,
        metadata={
            "table": "sales_by_category",
            "records counts": len(pd_data),
        },
    )


@asset(
    ins = {
        "fact_sales"  : AssetIn(key_prefix = ["silver", "ecom"]),
        "clean_customer"  : AssetIn(key_prefix = ["silver", "ecom"]),
    },
    key_prefix=["gold", "ecom"],
    io_manager_key="minio_io_manager",
    group_name="GOLD",
    compute_kind="Pandas"
)
def Loyal_customer(fact_sales: pd.DataFrame, clean_customer: pd.DataFrame) -> Output[pd.DataFrame]:
    factsales = fact_sales.copy()
    cus = clean_customer.copy()

    y = factsales.groupby('CustomerKey')['Total_bill'].sum().reset_index()
    y.rename(columns={'Total_bill': 'Total'}, inplace=True)
    cus = cus[(cus['Prefix'] != 'NA') & (cus['EmailAddress'] != 'NA')]

    pd_data = cus.merge(y, on='CustomerKey', how='inner')
    pd_data = pd_data[["CustomerKey", "Prefix", "FirstName", "LastName", "EmailAddress", "Total"]]
    return Output(
        pd_data,
        metadata={
            "table": "Loyal_customer",
            "records counts": len(pd_data),
        },
    )


@asset(
    ins={
        "fact_sales": AssetIn(key_prefix=["silver", "ecom"]),
        "raw_territory": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    key_prefix=["gold", "ecom"],
    io_manager_key="minio_io_manager",
    group_name="GOLD",
    compute_kind="Pandas"
)
def sales_by_territory(fact_sales: pd.DataFrame, raw_territory: pd.DataFrame) -> Output[pd.DataFrame]:

    factsales = fact_sales.copy()
    ter = raw_territory.copy()
    df = factsales.merge(ter, left_on='TerritoryKey', right_on='SalesTerritoryKey', how='inner')

    df['Date'] = pd.to_datetime(df['OrderDate'], format='%Y-%m-%d')
    # Nhóm dữ liệu và tính tổng cho cột "Total_bill"
    df1 = df.groupby([df['Date'].dt.strftime('%Y-%m'), 'Region'])['Total_bill'].sum().reset_index()
    df1.rename(columns={'Total_bill': 'Total_bill_sum'}, inplace=True)

    # Nhóm dữ liệu và tính tổng cho cột "Profit"
    df2 = df.groupby([df['Date'].dt.strftime('%Y-%m'), 'Region'])['Profit'].sum().reset_index()
    df2.rename(columns={'Profit': 'Profit_sum'}, inplace=True)

    pd_data = df1.merge(df2, on=['Date', 'Region'], how='inner')

    pd_data = pd_data.sort_values(by='Date')
    return Output(
        pd_data,
        metadata={
            "table": "sales_by_territory",
            "records counts": len(pd_data),
        },
    )