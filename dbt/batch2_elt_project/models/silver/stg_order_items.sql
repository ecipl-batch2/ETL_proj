{{
    config(
        materialized='incremental',
        unique_key='ORDER_ID',
        incremental_strategy='merge'
    )
}}

with orders_items_data as (
    SELECT
        ORDER_ID,
        CAST(ORDER_ITEM_ID AS INT) as ORDER_ITEM_ID,
        PRODUCT_ID,
        SELLER_ID,
        CAST(SHIPPING_LIMIT_DATE AS TIMESTAMP) as SHIPPING_LIMIT_DATE,
        CAST(PRICE AS FLOAT) as PRICE,
        CAST(FREIGHT_VALUE AS FLOAT) as FREIGHT_VALUE,
        CAST(OPERATION AS INT) as OPERATION,
        CAST(TRANSACTION_TIME as TIMESTAMP) as TRANSACTION_TIME,
        ROW_NUMBER() OVER(PARTITION BY ORDER_ID ORDER BY TRANSACTION_TIME DESC)AS rn
    FROM 
        {{ source('bronze', 'order_items') }}
),

latest_orders_items_data AS (
    SELECT 
        *
    FROM 
        orders_items_data
    WHERE rn = 1
)


select 
    ORDER_ID,
    ORDER_ITEM_ID,
    PRODUCT_ID,
    SELLER_ID,
    SHIPPING_LIMIT_DATE,
    PRICE,
    FREIGHT_VALUE,
    OPERATION,
    TRANSACTION_TIME
from latest_orders_items_data

{% if is_incremental() %}
  where TRANSACTION_TIME > (select max(TRANSACTION_TIME) from {{ this }})
{% endif %}