SELECT
    AVG(sa."Total_bill_sum") AS avg_total_bill,
    MIN(sa."Total_bill_sum") AS min_total_bill,
    MAX(sa."Total_bill_sum") AS max_total_bill,
    STDDEV(sa."Total_bill_sum") AS stddev_total_bill,
    AVG(sa."Profit_sum" ) AS avg_profit_sum,
    MIN(sa."Profit_sum") AS min_profit_sum,
    MAX(sa."Profit_sum") AS max_profit_sum,
    STDDEV(sa."Profit_sum") AS stddev_profit_sum
FROM public.sales_by_territory sa