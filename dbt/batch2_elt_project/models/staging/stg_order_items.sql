SELECT
      ORDER_ID             ,
      ORDER_ITEM_ID        ,
      PRODUCT_ID           ,
      SELLER_ID            ,
      SHIPPING_LIMIT_DATE  ,
      PRICE                ,
      FREIGHT_VALUE        ,
      INSERTED_AT          
FROM 
      {{ source('bronze', 'order_items') }}

  