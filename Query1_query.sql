SELECT 
    "date" AS order_date,
    SUM(CAST(amount AS DOUBLE)) 
        OVER (ORDER BY date_parse(replace("date", '"', ''), '%m-%d-%y') 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sales
FROM "ecommerce_db_l11_hands_on"."itcs6190_l11_handson_801426261_kkim43"
WHERE year(date_parse(replace("date", '"', ''), '%m-%d-%y')) = 2022
ORDER BY order_date
LIMIT 10;
