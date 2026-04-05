/*
 * SCD Type 1 Transformation for dim_product
 * 
 * Strategy:
 * Data in the data warehouse final schema will be updated according to attribute of product changes in staging schema. 
 * History is not preserved. If a product in staging does not exist in the final schema yet, insert it.
 */

INSERT INTO final.dim_product (
    product_nk,
    product_category_name,
    product_category_name_english,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT 
    p.product_id,
    p.product_category_name,
    pcnt.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM stg.products p
LEFT JOIN stg.product_category_name_translation pcnt 
    ON p.product_category_name = pcnt.product_category_name
ON CONFLICT (product_nk) 
DO UPDATE SET
    product_category_name = EXCLUDED.product_category_name,
    product_category_name_english = EXCLUDED.product_category_name_english,
    product_weight_g = EXCLUDED.product_weight_g,
    product_length_cm = EXCLUDED.product_length_cm,
    product_height_cm = EXCLUDED.product_height_cm,
    product_width_cm = EXCLUDED.product_width_cm,
    updated_at = CASE WHEN 
        final.dim_product.product_category_name IS DISTINCT FROM EXCLUDED.product_category_name OR
        final.dim_product.product_category_name_english IS DISTINCT FROM EXCLUDED.product_category_name_english OR
        final.dim_product.product_weight_g IS DISTINCT FROM EXCLUDED.product_weight_g OR
        final.dim_product.product_length_cm IS DISTINCT FROM EXCLUDED.product_length_cm OR
        final.dim_product.product_height_cm IS DISTINCT FROM EXCLUDED.product_height_cm OR
        final.dim_product.product_width_cm IS DISTINCT FROM EXCLUDED.product_width_cm
    THEN 
        CURRENT_TIMESTAMP
    ELSE 
        final.dim_product.updated_at
    END;