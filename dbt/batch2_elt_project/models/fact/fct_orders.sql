with stg_orders_data as
(
    select * from {{ ref ('stg_orders') }}
)

select 
    REPLACE(ORDER_ID,'"', '') AS ORDER_ID,
    REPLACE(CUSTOMER_ID,'"', '') AS CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_PURCHASE_TIMESTAMP,
    ORDER_APPROVED_AT ,
    IfNULL(ORDER_DELIVERED_CARRIER_DATE, CURRENT_TIMESTAMP(2)) as carrier_date,
    IFNULL(ORDER_DELIVERED_CUSTOMER_DATE, CURRENT_TIMESTAMP(2)) as customer_date,
    ORDER_ESTIMATED_DELIVERY_DATE
from 
    stg_orders_data


