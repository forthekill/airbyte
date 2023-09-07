{{ 
    config(
        schema='dbt_refined',
        post_hook=["ALTER TABLE {{ this }} SET CHANGE_TRACKING=TRUE"]
    ) 
}}

WITH cte AS (SELECT id,
                tenders,
                GET(tenders, 0):type::STRING AS payment_type,
                created_at::DATETIME AS order_date,
                parse_json(flat_items.value::STRING) AS line_item
             FROM SQUARE.AIRBYTE.ORDERS_RAW,
                LATERAL FLATTEN(input => SQUARE.AIRBYTE.ORDERS_RAW.line_items) AS flat_items)
    SELECT id AS order_id,
            payment_type,
            CASE WHEN payment_type = 'CARD' THEN 
                GET(tenders, 0):card_details.card.card_brand::STRING
            ELSE
                null
            END AS card_type,
            order_date,
            line_item:base_price_money.amount::NUMBER / 100 as amount,
            line_item:catalog_object_id::STRING as item_id,
            line_item:name::STRING as item_name,
            line_item:quantity::INTEGER as quantity
    FROM cte
    