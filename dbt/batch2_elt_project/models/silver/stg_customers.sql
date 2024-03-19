{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}
WITH customers_data AS (
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

customers_clean_data AS (
    SELECT 
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID,
    COALESCE(CUSTOMER_ZIP_CODE_PREFIX ,0) AS CUSTOMER_ZIP_CODE_PREFIX,
    COALESCE(CUSTOMER_CITY, 'N/A') AS CUSTOMER_CITY,
    COALESCE(CUSTOMER_STATE, 'N/A') AS CUSTOMER_STATE,
    OPERATION,
    TRANSACTION_TIME,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY TRANSACTION_TIME DESC) AS RN
    FROM customers_data
),

latest_customers_data AS (
    SELECT 
        *
    FROM 
        customers_clean_data
    WHERE RN = 1
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
    latest_customers_data

{% if is_incremental() %}
  WHERE TRANSACTION_TIME > (SELECT MAX(TRANSACTION_TIME) FROM {{ this }})
{% endif %}

