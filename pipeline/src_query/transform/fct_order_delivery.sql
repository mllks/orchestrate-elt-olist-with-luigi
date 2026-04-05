/*
 * Transform query for fct_order_delivery
 * 
 * This is an accumulating snapshot fact table that tracks the delivery process
 * for each order through multiple milestones.
 */

INSERT INTO final.fct_order_delivery (
    order_nk,
    order_status,
    purchase_date_id,
    approval_date_id,
    carrier_handoff_date_id,
    estimated_delivery_date_id,
    actual_delivery_date_id,
    customer_id,
    seller_id,
    multi_seller_order,
    days_purchase_to_approval,
    days_approval_to_delivery,
    days_carrier_handoff_to_delivery,
    days_estimated_to_actual,
    delivery_status,
    late_severity
)
SELECT 
    o.order_id AS order_nk,
    o.order_status,
    -- Date dimension foreign keys
    COALESCE(purchase_date_dim.date_id, -1) AS purchase_date_id,
    COALESCE(approval_date_dim.date_id, -1) AS approval_date_id,
    COALESCE(carrier_date_dim.date_id, -1) AS carrier_handoff_date_id,
    COALESCE(estimated_date_dim.date_id, -1) AS estimated_delivery_date_id,
    COALESCE(actual_date_dim.date_id, -1) AS actual_delivery_date_id,
    -- Dimension foreign keys
    dc.customer_id,
    ds.seller_id,
    -- Multi-seller indicator
    CASE 
        WHEN multi_seller.multi_seller_flag > 1 THEN 'Multi Seller'
        ELSE 'Single Seller'
    END AS multi_seller_order,
    -- Calculated measures
    CASE 
        WHEN o.order_approved_at IS NOT NULL AND o.order_purchase_timestamp IS NOT NULL 
        THEN EXTRACT(DAY FROM (TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS') - 
                               TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS')))
        ELSE NULL 
    END AS days_purchase_to_approval,
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_approved_at IS NOT NULL 
        THEN EXTRACT(DAY FROM (TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') - 
                               TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS')))
        ELSE NULL 
    END AS days_approval_to_delivery,
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_delivered_carrier_date IS NOT NULL 
        THEN EXTRACT(DAY FROM (TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') - 
                               TO_TIMESTAMP(o.order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS')))
        ELSE NULL 
    END AS days_carrier_handoff_to_delivery,
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL 
        THEN EXTRACT(DAY FROM (TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') - 
                               TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS')))
        ELSE NULL 
    END AS days_estimated_to_actual,
    -- Delivery status indicators
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL 
             AND TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') <= 
                 TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS')
        THEN 'On Time'
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL 
             AND TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') > 
                 TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS')
        THEN 'Late'
        ELSE NULL
    END AS delivery_status,
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL THEN
            CASE 
                WHEN TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') <= 
                     TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS')
                THEN 'On Time'
                WHEN TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') <= 
                     TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '3 days'
                THEN 'Within Threshold'
                ELSE 'Significantly Late'
            END
        ELSE NULL
    END AS late_severity
FROM stg.orders o
JOIN stg.customers c ON o.customer_id = c.customer_id
JOIN final.dim_customer dc ON c.customer_unique_id = dc.customer_nk AND dc.current_flag = 'Current'
JOIN (
    -- Get the seller for each order (assuming first seller for simplicity)
    SELECT 
        oi.order_id,
        MIN(oi.seller_id) as seller_id,
        COUNT(DISTINCT oi.seller_id) as seller_count
    FROM stg.order_items oi
    GROUP BY oi.order_id
) first_seller ON o.order_id = first_seller.order_id
JOIN stg.sellers s ON first_seller.seller_id = s.seller_id
JOIN final.dim_seller ds ON s.seller_id = ds.seller_nk AND ds.current_flag = 'Current'
-- Join to get multi-seller information
LEFT JOIN (
    SELECT 
        order_id,
        COUNT(DISTINCT seller_id) as multi_seller_flag
    FROM stg.order_items
    GROUP BY order_id
) multi_seller ON o.order_id = multi_seller.order_id
-- Date dimension joins
LEFT JOIN final.dim_date purchase_date_dim 
    ON purchase_date_dim.date_actual = DATE(TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS'))
LEFT JOIN final.dim_date approval_date_dim 
    ON approval_date_dim.date_actual = DATE(TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS'))
LEFT JOIN final.dim_date carrier_date_dim 
    ON carrier_date_dim.date_actual = DATE(TO_TIMESTAMP(o.order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS'))
LEFT JOIN final.dim_date estimated_date_dim 
    ON estimated_date_dim.date_actual = DATE(TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS'))
LEFT JOIN final.dim_date actual_date_dim 
    ON actual_date_dim.date_actual = DATE(TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS'))
ON CONFLICT (order_nk) 
DO UPDATE SET
    order_status = EXCLUDED.order_status,
    purchase_date_id = EXCLUDED.purchase_date_id,
    approval_date_id = EXCLUDED.approval_date_id,
    carrier_handoff_date_id = EXCLUDED.carrier_handoff_date_id,
    estimated_delivery_date_id = EXCLUDED.estimated_delivery_date_id,
    actual_delivery_date_id = EXCLUDED.actual_delivery_date_id,
    customer_id = EXCLUDED.customer_id,
    seller_id = EXCLUDED.seller_id,
    multi_seller_order = EXCLUDED.multi_seller_order,
    days_purchase_to_approval = EXCLUDED.days_purchase_to_approval,
    days_approval_to_delivery = EXCLUDED.days_approval_to_delivery,
    days_carrier_handoff_to_delivery = EXCLUDED.days_carrier_handoff_to_delivery,
    days_estimated_to_actual = EXCLUDED.days_estimated_to_actual,
    delivery_status = EXCLUDED.delivery_status,
    late_severity = EXCLUDED.late_severity,
    updated_at = CURRENT_TIMESTAMP;