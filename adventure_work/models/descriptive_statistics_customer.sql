SELECT
    AVG(lc."Total" ) AS avg_total,
    MIN(lc."Total") AS min_total,
    MAX(lc."Total") AS max_total,
    STDDEV(lc."Total") AS stddev_total
from public."Loyal_customer" lc