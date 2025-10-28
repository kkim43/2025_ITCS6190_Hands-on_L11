WITH monthly AS (
    SELECT 
        date_trunc('month', date_parse(replace("date", '"', ''), '%m-%d-%y')) AS month,
        SUM(TRY_CAST(CAST(amount AS VARCHAR) AS DOUBLE)) AS total_sales
    FROM "ecommerce_db_l11_hands_on"."itcs6190_l11_handson_801426261_kkim43"
    WHERE lower(status) = 'shipped'
      AND year(date_parse(replace("date", '"', ''), '%m-%d-%y')) = 2022
    GROUP BY 1
)
SELECT 
    month,
    CAST(total_sales AS BIGINT) AS total_sales,     
    CAST(total_sales AS BIGINT) AS total_profit,     
    ROUND(
        (total_sales - LAG(total_sales) OVER (ORDER BY month)) 
        / NULLIF(LAG(total_sales) OVER (ORDER BY month), 0) * 100, 2
    ) AS sales_growth_pct,
    ROUND(
        (total_sales - LAG(total_sales) OVER (ORDER BY month)) 
        / NULLIF(LAG(total_sales) OVER (ORDER BY month), 0) * 100, 2
    ) AS profit_growth_pct
FROM monthly
WHERE month >= DATE '2022-04-01'   
ORDER BY month
LIMIT 10;
