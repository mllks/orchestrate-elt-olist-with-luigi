/*
 * Transform query for fct_order_reviews
 * 
 * This is a transaction fact table that captures review information for each order.
 */

INSERT INTO final.fct_order_reviews (
    review_nk,
    order_nk,
    order_status,
    review_score,
    sentiment_score,
    review_length_chars,
    review_severity,
    comment_status,
    days_since_purchase,
    review_date_id,
    customer_id
)
SELECT DISTINCT ON (oreview.review_id, oreview.order_id)
    oreview.review_id AS review_nk,
    oreview.order_id AS order_nk,
    o.order_status,
    oreview.review_score,
    -- Simple sentiment scoring based on review score
    CASE 
        WHEN oreview.review_score >= 4 THEN 1.0
        WHEN oreview.review_score = 3 THEN 0.0
        WHEN oreview.review_score <= 2 THEN -1.0
        ELSE 0.0
    END AS sentiment_score,
    -- Review length calculation
    COALESCE(LENGTH(oreview.review_comment_message), 0) AS review_length_chars,
    -- Review severity based on score
    CASE 
        WHEN oreview.review_score <= 2 THEN 'Critical'
        ELSE 'Standard'
    END AS review_severity,
    -- Comment status
    CASE 
        WHEN oreview.review_comment_message IS NOT NULL AND LENGTH(TRIM(oreview.review_comment_message)) > 0 
        THEN 'Has Comment'
        ELSE 'No Comment'
    END AS comment_status,
    -- Days since purchase calculation
    CASE 
        WHEN oreview.review_creation_date IS NOT NULL AND o.order_purchase_timestamp IS NOT NULL 
        THEN EXTRACT(DAY FROM (TO_TIMESTAMP(oreview.review_creation_date, 'YYYY-MM-DD HH24:MI:SS') - 
                               TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS')))
        ELSE NULL 
    END AS days_since_purchase,
    COALESCE(review_date_dim.date_id, -1) AS review_date_id,
    dc.customer_id
FROM stg.order_reviews oreview
JOIN stg.orders o ON oreview.order_id = o.order_id
JOIN stg.customers c ON o.customer_id = c.customer_id
JOIN final.dim_customer dc ON c.customer_unique_id = dc.customer_nk AND dc.current_flag = 'Current'
-- Date dimension join
LEFT JOIN final.dim_date review_date_dim 
    ON review_date_dim.date_actual = DATE(TO_TIMESTAMP(oreview.review_creation_date, 'YYYY-MM-DD HH24:MI:SS'))
ORDER BY oreview.review_id, oreview.order_id, oreview.updated_at DESC
ON CONFLICT (review_nk, order_nk) 
DO UPDATE SET
    order_status = EXCLUDED.order_status,
    review_score = EXCLUDED.review_score,
    sentiment_score = EXCLUDED.sentiment_score,
    review_length_chars = EXCLUDED.review_length_chars,
    review_severity = EXCLUDED.review_severity,
    comment_status = EXCLUDED.comment_status,
    days_since_purchase = EXCLUDED.days_since_purchase,
    review_date_id = EXCLUDED.review_date_id,
    customer_id = EXCLUDED.customer_id,
    updated_at = CURRENT_TIMESTAMP;