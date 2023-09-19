SELECT
    sa."Date",
    SUM(sa."Total_bill_sum") AS daily_total_bill,
    SUM(sa."Profit_sum") AS daily_profit_sum
FROM public.sales_by_category sa
GROUP BY sa."Date"
ORDER BY sa."Date"