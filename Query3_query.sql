WITH cleaned AS (
  SELECT
      category AS sub_category,
      CASE
        WHEN lower(status) LIKE 'cancelled%' THEN 'cancelled'
        WHEN lower(status) LIKE 'shipped%'   THEN 'shipped'
        WHEN lower(status) LIKE 'pending%'   THEN 'pending'
        ELSE regexp_replace(lower(status), '\\s+', ' ')
      END AS order_status,
      TRY_CAST(CAST(amount AS VARCHAR) AS DOUBLE) AS amount_num
  FROM "ecommerce_db_l11_hands_on"."itcs6190_l11_handson_801426261_kkim43"
)
SELECT
    sub_category,
    order_status,
    ROUND(AVG(amount_num), 2) AS avg_order_value,
    COUNT(amount_num) AS order_count
FROM cleaned
WHERE amount_num IS NOT NULL
GROUP BY 1,2
ORDER BY sub_category ASC, avg_order_value DESC
LIMIT 10;
