SELECT 
   GEOLOCATION_ZIP_CODE_PREFIX ,
   GEOLOCATION_LAT             ,
   GEOLOCATION_LNG             ,
   GEOLOCATION_CITY            ,
   GEOLOCATION_STATE           ,
   INSERTED_AT 
FROM 
    {{ source('bronze', 'geolocation') }}
  