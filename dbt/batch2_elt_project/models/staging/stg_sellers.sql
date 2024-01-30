SELECT 
      SELLER_ID               ,
      SELLER_ZIP_CODE_PREFIX  ,
      SELLER_CITY             ,
      SELLER_STATE            ,
      INSERTED_AT            
FROM
      {{ source('bronze', 'sellers') }}

  