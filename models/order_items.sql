WITH cte AS (SELECT "id" AS id,
                "tenders" AS tenders,
                GET("tenders", 0):type::STRING AS payment_type,
                "created_at"::DATETIME AS order_date,
                parse_json(flat_items.value::STRING) AS line_item
             FROM square.airbyte."orders",
                LATERAL FLATTEN(input => square.airbyte."orders"."line_items") AS flat_items)
    SELECT id,
            CASE WHEN payment_type = 'CARD' THEN 
                GET(tenders, 0):card_details.card.card_brand::STRING
            ELSE
                null
            END AS card_type,
            order_date,
            line_item:base_price_money.amount::NUMBER / 100 as amount,
            line_item:name::STRING as item_name,
            line_item:quantity::INTEGER as quantity
    FROM cte