WITH ranked AS (
    SELECT 
        category,
        style AS product_name,
        SUM(CAST(amount AS DOUBLE)) AS total_sales,
        RANK() OVER (PARTITION BY category ORDER BY SUM(CAST(amount AS DOUBLE)) DESC) AS rank_in_category
    FROM "ecommerce_db_l11_hands_on"."itcs6190_l11_handson_801426261_kkim43"
    WHERE lower(status) = 'shipped'
    GROUP BY category, style
)
SELECT 
    category,
    product_name,
    total_sales,
    rank_in_category
FROM ranked
WHERE rank_in_category <= 3
ORDER BY category ASC, total_sales DESC
LIMIT 10;
