/*
 * Transform query for fct_order_items
 * 
 * This is a transaction fact table that captures each item purchased in an order.
 */

INSERT INTO final.fct_order_items (
    order_nk,
    order_item_sequence,
    order_status,
    purchase_date_id,
    product_id,
    customer_id,
    seller_id,
    price,
    freight_value
)
SELECT 
    oi.order_id AS order_nk,
    oi.order_item_id AS order_item_sequence,
    o.order_status,
    COALESCE(purchase_date_dim.date_id, -1) AS purchase_date_id,
    dp.product_id,
    dc.customer_id,
    ds.seller_id,
    oi.price,
    oi.freight_value
FROM stg.order_items oi
JOIN stg.orders o ON oi.order_id = o.order_id
JOIN stg.customers c ON o.customer_id = c.customer_id
JOIN final.dim_customer dc ON c.customer_unique_id = dc.customer_nk AND dc.current_flag = 'Current'
JOIN stg.sellers s ON oi.seller_id = s.seller_id
JOIN final.dim_seller ds ON s.seller_id = ds.seller_nk AND ds.current_flag = 'Current'
JOIN stg.products p ON oi.product_id = p.product_id
JOIN final.dim_product dp ON p.product_id = dp.product_nk
-- Date dimension join
LEFT JOIN final.dim_date purchase_date_dim 
    ON purchase_date_dim.date_actual = DATE(TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS'))
ON CONFLICT (order_nk, order_item_sequence) 
DO UPDATE SET
    order_status = EXCLUDED.order_status,
    purchase_date_id = EXCLUDED.purchase_date_id,
    product_id = EXCLUDED.product_id,
    customer_id = EXCLUDED.customer_id,
    seller_id = EXCLUDED.seller_id,
    price = EXCLUDED.price,
    freight_value = EXCLUDED.freight_value,
    updated_at = CURRENT_TIMESTAMP;