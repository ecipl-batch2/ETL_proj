SELECT
   ORDER_ID             ,
   PAYMENT_SEQUENTIAL   ,
   PAYMENT_TYPE         ,
   PAYMENT_INSTALLMENTS ,
   PAYMENT_VALUE        ,
   INSERTED_AT          
FROM 
    {{ source('bronze', 'order_payments') }}

