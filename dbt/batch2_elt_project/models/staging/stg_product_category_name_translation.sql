SELECT
   PRODUCT_CATEGORY_NAME         ,
   PRODUCT_CATEGORY_NAME_ENGLISH ,
   INSERTED_AT                   
FROM 
      {{ source('bronze', 'product_category_name_translation') }}

  