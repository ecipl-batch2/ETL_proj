{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

with customers_data as (
    SELECT  
        CUSTOMER_ID,
        CUSTOMER_UNIQUE_ID,
        CAST(CUSTOMER_ZIP_CODE_PREFIX AS INT) AS CUSTOMER_ZIP_CODE_PREFIX,
        CAST(CUSTOMER_CITY AS VARCHAR) AS CUSTOMER_CITY,
        CAST(CUSTOMER_STATE AS VARCHAR) AS CUSTOMER_STATE,
        CAST(OPERATION AS INT) AS OPERATION,
        CAST(TRANSACTION_TIME AS TIMESTAMP) AS TRANSACTION_TIME
    FROM
        {{ source('bronze', 'customers') }} 
),

customers_clean_data as (
    select 
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID,
    COALESCE(CAST(CUSTOMER_ZIP_CODE_PREFIX AS TEXT), 'N/A') as CUSTOMER_ZIP_CODE_PREFIX,
    COALESCE(CUSTOMER_CITY, 'N/A') as CUSTOMER_CITY,
    COALESCE(CUSTOMER_STATE, 'N/A') as CUSTOMER_STATE,
    OPERATION,
    TRANSACTION_TIME
    from customers_data
)

SELECT DISTINCT
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID,
    CUSTOMER_ZIP_CODE_PREFIX,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    OPERATION,
    TRANSACTION_TIME
FROM 
    customers_clean_data
{% if is_incremental() %}
  WHERE TRANSACTION_TIME > (SELECT MAX(TRANSACTION_TIME) FROM {{ this }})
{% endif %}
