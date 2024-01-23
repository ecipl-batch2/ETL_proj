
 select  product_id ,
 product_category_name,
 product_name_length,
 cast(product_description_length as int)as product_description_length,
 cast(product_photos_qty as int)as product_photos_qty,
 cast(product_weight_g as int)as product_weight_g,
 cast(product_length_cm as int)as product_length_cm,
 cast(product_height_cm as int)as product_height_cm,
 cast(product_width_cm as int)as product_width_cm,
 INSERTED_AT as inserted_at
 from
  {{    source ('bronze','products')   }} 
