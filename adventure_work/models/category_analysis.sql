SELECT
    sa."CategoryName"  ,
    COUNT(*) AS order_count,
    SUM(sa."Total_bill_sum") AS total_bill,
    SUM(sa."Profit_sum") AS total_profit
FROM public.sales_by_category sa
GROUP BY  sa."CategoryName"
