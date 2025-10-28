SELECT 
    "ship-state" AS state,
    ROUND(SUM(CAST(amount AS DOUBLE)), 2) AS total_loss_amount
FROM "ecommerce_db_l11_hands_on"."itcs6190_l11_handson_801426261_kkim43"
WHERE lower(status) = 'cancelled'
GROUP BY "ship-state"
ORDER BY total_loss_amount DESC
LIMIT 10;
