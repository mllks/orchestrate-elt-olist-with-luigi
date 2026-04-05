/*
 * Transform query for fct_order_payments
 * 
 * This is a transaction fact table that captures payment information for each order.
 */

INSERT INTO final.fct_order_payments (
    order_nk,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    expected_order_value,
    discount_amount,
    approval_date_id,
    customer_id
)
SELECT 
    op.order_id AS order_nk,
    op.payment_sequential,
    op.payment_type,
    op.payment_installments,
    op.payment_value,
    -- Expected order value (sum of all payments for this order)
    order_totals.total_order_value AS expected_order_value,
    -- Discount calculation (total expected - actual payment)
    (order_totals.total_order_value - op.payment_value) AS discount_amount,
    COALESCE(approval_date_dim.date_id, -1) AS approval_date_id,
    dc.customer_id
FROM stg.order_payments op
JOIN stg.orders o ON op.order_id = o.order_id
JOIN stg.customers c ON o.customer_id = c.customer_id
JOIN final.dim_customer dc ON c.customer_unique_id = dc.customer_nk AND dc.current_flag = 'Current'
-- Get total order value for discount calculation
LEFT JOIN (
    SELECT 
        order_id,
        SUM(payment_value) AS total_order_value
    FROM stg.order_payments
    GROUP BY order_id
) order_totals ON op.order_id = order_totals.order_id
-- Date dimension join
LEFT JOIN final.dim_date approval_date_dim 
    ON approval_date_dim.date_actual = DATE(TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS'))
ON CONFLICT (order_nk, payment_sequential) 
DO UPDATE SET
    payment_type = EXCLUDED.payment_type,
    payment_installments = EXCLUDED.payment_installments,
    payment_value = EXCLUDED.payment_value,
    expected_order_value = EXCLUDED.expected_order_value,
    discount_amount = EXCLUDED.discount_amount,
    approval_date_id = EXCLUDED.approval_date_id,
    customer_id = EXCLUDED.customer_id,
    updated_at = CURRENT_TIMESTAMP;