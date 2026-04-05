CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS final AUTHORIZATION postgres;

-- Final Schema
-- Purpose:  Dimension and fact tables following Kimball dimensional design
-- SCD:      dim_customer (Type 2), dim_seller (Type 2),
--           dim_product (Type 1), dim_date (Type 1)
-- Special:  dim_cluster (insert only), cluster_assignment (append only)

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_customer
-- SCD Type 2 — Location attributes: customer_city, customer_state,
--              customer_latitude, customer_longitude.
-- Tracks history via valid_from, valid_to, current_flag.
-- Natural key: customer_nk maps to customer_unique_id in source.
-- -----------------------------------------------------------------------------
CREATE TABLE final.dim_customer(
    customer_id uuid primary key default uuid_generate_v4(),
    customer_nk text NOT NULL,
    customer_city text,
    customer_state text,
    customer_latitude real,
    customer_longitude real,
    valid_from date NOT NULL DEFAULT current_date,
    valid_to date,
    current_flag varchar(100) NOT NULL DEFAULT 'Current',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS dim_customer_nk_idx
    ON final.dim_customer (customer_nk);
CREATE INDEX IF NOT EXISTS dim_customer_current_idx
    ON final.dim_customer (customer_nk, current_flag);

-- -----------------------------------------------------------------------------
-- dim_product
-- SCD Type 1 — All attributes overwritten in place on change.
-- Category and physical attribute changes are corrections not reclassifications.
-- Natural key: product_nk maps to product_id in source.
-- -----------------------------------------------------------------------------
CREATE TABLE final.dim_product(
    product_id uuid primary key default uuid_generate_v4(),
    product_nk text NOT NULL,
    product_category_name text,
    product_category_name_english text,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Add Unique Constraints to dim_product
    CONSTRAINT dim_product_nk UNIQUE (product_nk)
);

-- -----------------------------------------------------------------------------
-- dim_seller
-- SCD Type 2 — Location attributes: seller_city, seller_state,
--              seller_latitude, seller_longitude.
-- Tracks history via valid_from, valid_to, current_flag.
-- Natural key: seller_nk maps to seller_id in source.
-- -----------------------------------------------------------------------------
CREATE TABLE final.dim_seller(
    seller_id uuid primary key default uuid_generate_v4(),
    seller_nk text NOT NULL,
    seller_city text,
    seller_state text,
    seller_latitude real,
    seller_longitude real,
    valid_from date NOT NULL DEFAULT current_date,
    valid_to date,
    current_flag varchar(100) NOT NULL DEFAULT 'Current',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS dim_seller_nk_idx
    ON final.dim_seller (seller_nk);
CREATE INDEX IF NOT EXISTS dim_seller_current_idx
    ON final.dim_seller (seller_nk, current_flag);


-- -----------------------------------------------------------------------------
-- dim_date
-- -----------------------------------------------------------------------------
CREATE TABLE final.dim_date(
    date_id              	 INT NOT null primary KEY,
    date_actual              DATE NOT NULL,
    day_suffix               VARCHAR(4) NOT NULL,
    day_name                 VARCHAR(9) NOT NULL,
    day_of_year              INT NOT NULL,
    week_of_month            INT NOT NULL,
    week_of_year             INT NOT NULL,
    week_of_year_iso         CHAR(10) NOT NULL,
    month_actual             INT NOT NULL,
    month_name               VARCHAR(9) NOT NULL,
    month_name_abbreviated   CHAR(3) NOT NULL,
    quarter_actual           INT NOT NULL,
    quarter_name             VARCHAR(9) NOT NULL,
    year_actual              INT NOT NULL,
    first_day_of_week        DATE NOT NULL,
    last_day_of_week         DATE NOT NULL,
    first_day_of_month       DATE NOT NULL,
    last_day_of_month        DATE NOT NULL,
    first_day_of_quarter     DATE NOT NULL,
    last_day_of_quarter      DATE NOT NULL,
    first_day_of_year        DATE NOT NULL,
    last_day_of_year         DATE NOT NULL,
    mmyyyy                   CHAR(6) NOT NULL,
    mmddyyyy                 CHAR(10) NOT NULL,
    weekend_indr             VARCHAR(20) NOT NULL
);


-- -----------------------------------------------------------------------------
-- dim_cluster
-- Insert only — New rows inserted when a new cluster label and model
-- version combination appears. Existing rows never updated or deleted.
-- Natural key: cluster_label + model_version combination.
-- -----------------------------------------------------------------------------
CREATE TABLE final.dim_cluster(
    cluster_id uuid primary key default uuid_generate_v4(),
    cluster_label varchar(100) NOT NULL,
    model_version varchar(50) NOT NULL,
    assigned_date date NOT NULL DEFAULT current_date,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Add Unique Constraints to dim_cluster
    CONSTRAINT dim_cluster_nk UNIQUE (cluster_label, model_version)
);

-- =============================================================================
-- FACT TABLES
-- =============================================================================


-- -----------------------------------------------------------------------------
-- fct_order_items
-- Transaction Fact Table
-- Grain: One row per order item purchased by a customer.
-- BP1: Customers Placing Orders and Purchasing Items.
-- -----------------------------------------------------------------------------
CREATE TABLE final.fct_order_items(
	order_item_id uuid primary key default uuid_generate_v4() ,
	purchase_date_id int references final.dim_date(date_id),
    product_id uuid references final.dim_product(product_id),
	customer_id uuid references final.dim_customer(customer_id),
	seller_id uuid references final.dim_seller(seller_id),
     -- Degenerate Attributes
	order_nk text,
    order_item_sequence int, -- order_items.order_item_id
    order_status text,
    -- Measures
    price real,
    freight_value real,
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fct_order_items UNIQUE (order_nk, order_item_sequence)
);

CREATE INDEX IF NOT EXISTS fct_order_items_date_idx     ON final.fct_order_items (purchase_date_id);
CREATE INDEX IF NOT EXISTS fct_order_items_customer_idx ON final.fct_order_items (customer_id);
CREATE INDEX IF NOT EXISTS fct_order_items_seller_idx   ON final.fct_order_items (seller_id);
CREATE INDEX IF NOT EXISTS fct_order_items_product_idx  ON final.fct_order_items (product_id);
CREATE INDEX IF NOT EXISTS fct_order_items_order_idx    ON final.fct_order_items (order_nk);

-- -----------------------------------------------------------------------------
-- fct_order_payments
-- Transaction Fact Table
-- Grain: One row per payment method used per order.
-- BP2: Collecting Payments.
-- -----------------------------------------------------------------------------
CREATE TABLE final.fct_order_payments(
	payment_id uuid primary key default uuid_generate_v4() ,
	approval_date_id int references final.dim_date(date_id),
	customer_id uuid references final.dim_customer(customer_id),
     -- Degenerate Attributes
    order_nk text,
    payment_type text,
    payment_sequential int,
    -- Measures
    payment_value real,
	payment_installments int,
	expected_order_value real,
    discount_amount real,
    -- Audit
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fct_order_payments UNIQUE (order_nk, payment_sequential)
);

CREATE INDEX IF NOT EXISTS fct_order_payments_date_idx     ON final.fct_order_payments (approval_date_id);
CREATE INDEX IF NOT EXISTS fct_order_payments_customer_idx ON final.fct_order_payments (customer_id);
CREATE INDEX IF NOT EXISTS fct_order_payments_order_idx    ON final.fct_order_payments (order_nk);

-- -----------------------------------------------------------------------------
-- fct_order_delivery
-- Accumulating Snapshot Fact Table
-- Grain: One row per order shipment, updated at each delivery milestone.
-- BP3: Fulfilling and Delivering Orders.
-- Five role-playing date foreign keys all reference dim_date.
-- order_nk is UNIQUE — one delivery record per order.
-- -----------------------------------------------------------------------------
CREATE TABLE final.fct_order_delivery(
	delivery_id uuid primary key default uuid_generate_v4() ,
    purchase_date_id int references final.dim_date(date_id),
    approval_date_id int references final.dim_date(date_id),
    carrier_handoff_date_id int references final.dim_date(date_id),
    estimated_delivery_date_id int references final.dim_date(date_id),
    actual_delivery_date_id int references final.dim_date(date_id),
    customer_id uuid references final.dim_customer(customer_id),
    seller_id uuid references final.dim_seller(seller_id),
    -- Degenerate Attributes
    order_nk text,
    order_status text,
    multi_seller_order text,
    -- Measures
    days_purchase_to_approval int,
    days_approval_to_delivery int,
    days_carrier_handoff_to_delivery int,
    days_estimated_to_actual int,
    delivery_status text,
    late_severity text,
    -- Audit
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Add Unique Constraints to fct_order_delivery
    CONSTRAINT fct_order_delivery_order_nk UNIQUE (order_nk),
    CONSTRAINT fct_order_delivery_multi_seller_chk
        CHECK (multi_seller_order IN ('Single Seller', 'Multi Seller')),
    CONSTRAINT fct_order_delivery_status_chk
        CHECK (delivery_status IN ('On Time', 'Late')),
    CONSTRAINT fct_order_delivery_severity_chk
        CHECK (late_severity IN ('On Time', 'Within Threshold', 'Significantly Late'))
);
CREATE INDEX IF NOT EXISTS fct_order_delivery_customer_idx ON final.fct_order_delivery (customer_id);
CREATE INDEX IF NOT EXISTS fct_order_delivery_seller_idx   ON final.fct_order_delivery (seller_id);
CREATE INDEX IF NOT EXISTS fct_order_delivery_purchase_idx ON final.fct_order_delivery (purchase_date_id);
CREATE INDEX IF NOT EXISTS fct_order_delivery_actual_idx   ON final.fct_order_delivery (actual_delivery_date_id);

-- -----------------------------------------------------------------------------
-- fct_order_reviews
-- Transaction Fact Table
-- Grain: One row per review submission per order submitted by a customer.
-- BP4: Collecting Reviews from Customers.
-- order_nk is NOT unique — multiple reviews can exist per order.
-- -----------------------------------------------------------------------------
CREATE TABLE final.fct_order_reviews(
	review_id uuid primary key default uuid_generate_v4() ,
    review_date_id int references final.dim_date(date_id),
    customer_id uuid references final.dim_customer(customer_id),
    -- Degenerate Attributes
    review_nk text NOT NULL,
    order_nk text NOT NULL,
    -- Measures
    order_status text,
    review_score int,
    sentiment_score real,
    review_length_chars int,
    review_severity text,
    comment_status text,
    days_since_purchase int,
    -- Audit
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Add Unique Constraints to fct_order_reviews
    CONSTRAINT fct_order_reviews_nk UNIQUE (review_nk, order_nk),
    CONSTRAINT fct_order_reviews_score_chk
        CHECK (review_score BETWEEN 1 AND 5),
    CONSTRAINT fct_order_reviews_severity_chk
        CHECK (review_severity IN ('Critical', 'Standard')),
    CONSTRAINT fct_order_reviews_comment_chk
        CHECK (comment_status IN ('Has Comment', 'No Comment'))
);

CREATE INDEX IF NOT EXISTS fct_order_reviews_date_idx     ON final.fct_order_reviews (review_date_id);
CREATE INDEX IF NOT EXISTS fct_order_reviews_customer_idx ON final.fct_order_reviews (customer_id);
CREATE INDEX IF NOT EXISTS fct_order_reviews_order_idx    ON final.fct_order_reviews (order_nk);


-- -----------------------------------------------------------------------------
-- cluster_assignment
-- Append only — One row per customer per model run.
-- Never updated or deleted. Preserves full segment movement history.
-- -----------------------------------------------------------------------------
CREATE TABLE final.cluster_assignment(
    assignment_id uuid primary key default uuid_generate_v4(),
    customer_id uuid references final.dim_customer(customer_id),
    cluster_id uuid references final.dim_cluster(cluster_id),
    model_version varchar(50),
    assigned_date date NOT NULL DEFAULT current_date,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS cluster_assignment_customer_idx
    ON final.cluster_assignment (customer_id);
CREATE INDEX IF NOT EXISTS cluster_assignment_version_idx
    ON final.cluster_assignment (model_version);


-- Populating for staging date dimension 
INSERT INTO final.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

-- Add unknown date (-1) for handling NULL dates
INSERT INTO final.dim_date (date_id, date_actual, day_suffix, day_name, day_of_year, week_of_month, week_of_year, week_of_year_iso, month_actual, month_name, month_name_abbreviated, quarter_actual, quarter_name, year_actual, first_day_of_week, last_day_of_week, first_day_of_month, last_day_of_month, first_day_of_quarter, last_day_of_quarter, first_day_of_year, last_day_of_year, mmyyyy, mmddyyyy, weekend_indr) 
VALUES (-1, '1900-01-01', '1st', 'Unknown', 1, 1, 1, '1900-W01', 1, 'Unknown', 'Unk', 1, 'First', 1900, '1900-01-01', '1900-01-07', '1900-01-01', '1900-01-31', '1900-01-01', '1900-03-31', '1900-01-01', '1900-12-31', '011900', '01011900', 'weekday') 
ON CONFLICT (date_id) DO NOTHING;



