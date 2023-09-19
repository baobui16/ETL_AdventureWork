SELECT
    lc."CustomerKey" ,
    lc."FirstName" ,
    lc."LastName" ,
    lc."Total"
FROM public."Loyal_customer" lc
ORDER BY  lc."Total"  DESC
LIMIT 10