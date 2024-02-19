SELECT 
    a.product_category_name_english AS product,
    COUNT(b.product_category_name) AS qty 
FROM 
    {{ ref ('stg_product_category_name_translation' )}} a 
INNER JOIN 
    {{ ref("stg_products") }}  b 
ON 
    a.product_category_name = b.product_category_name 
GROUP BY 
    product 
ORDER BY 
    qty DESC 