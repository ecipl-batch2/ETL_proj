
with stg_product_data as
(
    select * from {{ ref ('stg_products') }}
)

select  
    REPLACE(product_id,'"', '') AS product_id ,
    IFNULL(product_category_name, 'NO NAME ASSIGNED') as product_category_name,
    IFNULL(product_name_length, 0) as product_name_length,
    IFNULL(product_description_length, 0)as product_description_length,
    IFNULL(product_photos_qty, 0) as product_photos_qty,
    IFNULL(product_weight_g, 0) as product_weight_g ,
    IFNULL(product_length_cm, 0) as product_length_cm,
    IFNULL(product_height_cm, 0) as product_height_cm,
    IFNULL(product_width_cm, 0) as product_width_cm,
    INSERTED_AT as inserted_at
from stg_product_data