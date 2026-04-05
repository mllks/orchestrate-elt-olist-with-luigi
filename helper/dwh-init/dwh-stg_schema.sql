CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION postgres;

-- Staging
CREATE TABLE stg.product_category_name_translation (
    id uuid default uuid_generate_v4(),
    product_category_name text NOT NULL,
    product_category_name_english text NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_product_category_name_translation PRIMARY KEY (product_category_name)
);

CREATE TABLE stg.products (
    id uuid default uuid_generate_v4(),
    product_id text NOT NULL,
    product_category_name text,
    product_name_lenght real,
    product_description_lenght real,
    product_photos_qty real,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);

CREATE TABLE stg.customers (
    id uuid default uuid_generate_v4(),
    customer_id text NOT NULL,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

CREATE TABLE stg.sellers (
    id uuid default uuid_generate_v4(),
    seller_id text NOT NULL,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_sellers PRIMARY KEY (seller_id)
);

CREATE TABLE stg.geolocation (
    id uuid default uuid_generate_v4(),
	geolocation_zip_code_prefix integer NOT NULL,
    geolocation_lat real,
    geolocation_lng real,
    geolocation_city text,
    geolocation_state text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_geolocation PRIMARY KEY (geolocation_zip_code_prefix)
);

CREATE TABLE stg.orders (
    id uuid default uuid_generate_v4(),
    order_id text NOT NULL,
    customer_id text,
    order_status text,
    order_purchase_timestamp text,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_delivered_customer_date text,
    order_estimated_delivery_date text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);

CREATE TABLE stg.order_items (
    id uuid default uuid_generate_v4(),
    order_id text NOT NULL,
    order_item_id integer NOT NULL,
    product_id text,
    seller_id text,
    shipping_limit_date text,
    price real,
    freight_value real,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE stg.order_payments (
    id uuid default uuid_generate_v4(),
    order_id text NOT NULL,
    payment_sequential integer NOT NULL,
    payment_type text,
    payment_installments integer,
    payment_value real,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE stg.order_reviews (
    id uuid default uuid_generate_v4(),
    review_id text NOT NULL,
    order_id text NOT NULL,
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date text,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_order_reviews PRIMARY KEY (review_id, order_id)
);

ALTER TABLE stg.orders
ADD FOREIGN KEY (customer_id) REFERENCES stg.customers(customer_id);

ALTER TABLE stg.order_items
ADD FOREIGN KEY (order_id) REFERENCES stg.orders(order_id);

ALTER TABLE stg.order_payments
ADD FOREIGN KEY (order_id) REFERENCES stg.orders(order_id);

ALTER TABLE stg.order_reviews
ADD FOREIGN KEY (order_id) REFERENCES stg.orders(order_id);

ALTER TABLE stg.order_items
ADD FOREIGN KEY (product_id) REFERENCES stg.products(product_id);

ALTER TABLE stg.order_items
ADD FOREIGN KEY (seller_id) REFERENCES stg.sellers(seller_id);


ALTER TABLE stg.products
ADD FOREIGN KEY (product_category_name) REFERENCES stg.product_category_name_translation(product_category_name);

ALTER TABLE stg.customers
ADD FOREIGN KEY (customer_zip_code_prefix) REFERENCES stg.geolocation(geolocation_zip_code_prefix);

ALTER TABLE stg.sellers
ADD FOREIGN KEY (seller_zip_code_prefix) REFERENCES stg.geolocation(geolocation_zip_code_prefix);
