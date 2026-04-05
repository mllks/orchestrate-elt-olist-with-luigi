INSERT INTO stg.orders 
    (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date)
SELECT
    order_id, 
    customer_id, 
    order_status, 
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date

FROM public.orders

ON CONFLICT(order_id) 
DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    order_status = EXCLUDED.order_status,
    order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
    order_approved_at = EXCLUDED.order_approved_at,
    order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
    order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
    order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date,
    updated_at = CASE WHEN 
                        stg.orders.customer_id IS DISTINCT FROM EXCLUDED.customer_id
                        OR stg.orders.order_status IS DISTINCT FROM EXCLUDED.order_status
                        OR stg.orders.order_purchase_timestamp IS DISTINCT FROM EXCLUDED.order_purchase_timestamp
                        OR stg.orders.order_approved_at IS DISTINCT FROM EXCLUDED.order_approved_at
                        OR stg.orders.order_delivered_carrier_date IS DISTINCT FROM EXCLUDED.order_delivered_carrier_date
                        OR stg.orders.order_delivered_customer_date IS DISTINCT FROM EXCLUDED.order_delivered_customer_date
                        OR stg.orders.order_estimated_delivery_date IS DISTINCT FROM EXCLUDED.order_estimated_delivery_date
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.orders.updated_at
                END;