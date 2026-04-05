/*
 * SCD Type 2 Transformation for dim_customer
 * 
 * Strategy:
 * 1. Identify records where location attributes (city, state, lat, lng) have changed.
 * 2. Close the old version by updating valid_to and current_flag.
 * 3. Insert new versions for changed records and entirely new records.
 */

-- Step 1: Expire existing records that have changes in tracked location attributes
UPDATE final.dim_customer fd
SET 
    valid_to = CURRENT_TIMESTAMP,
    current_flag = 'Expired',
    updated_at = CURRENT_TIMESTAMP
FROM stg.customers stg
LEFT JOIN (
    -- Get unique lat/lng per zip code to avoid duplicates
    SELECT 
        geolocation_zip_code_prefix, 
        AVG(geolocation_lat) as geolocation_lat, 
        AVG(geolocation_lng) as geolocation_lng
    FROM stg.geolocation
    GROUP BY geolocation_zip_code_prefix
) geo ON stg.customer_zip_code_prefix = geo.geolocation_zip_code_prefix
WHERE fd.customer_nk = stg.customer_unique_id
  AND fd.current_flag = 'Current'
  AND (
      fd.customer_city IS DISTINCT FROM stg.customer_city OR
      fd.customer_state IS DISTINCT FROM stg.customer_state OR
      fd.customer_latitude IS DISTINCT FROM geo.geolocation_lat OR
      fd.customer_longitude IS DISTINCT FROM geo.geolocation_lng
  );

-- Step 2: Insert new records (both entirely new customers and new versions of updated customers)
INSERT INTO final.dim_customer (
    customer_nk,
    customer_city,
    customer_state,
    customer_latitude,
    customer_longitude,
    valid_from,
    valid_to,
    current_flag
)
SELECT DISTINCT ON (stg.customer_unique_id)
    stg.customer_unique_id,
    stg.customer_city,
    stg.customer_state,
    geo.geolocation_lat,
    geo.geolocation_lng,
    CURRENT_TIMESTAMP AS valid_from,
    '9999-12-31 23:59:59'::TIMESTAMP AS valid_to,
    'Current' AS current_flag
FROM stg.customers stg
LEFT JOIN (
    SELECT 
        geolocation_zip_code_prefix, 
        AVG(geolocation_lat) as geolocation_lat, 
        AVG(geolocation_lng) as geolocation_lng
    FROM stg.geolocation
    GROUP BY geolocation_zip_code_prefix
) geo ON stg.customer_zip_code_prefix = geo.geolocation_zip_code_prefix
WHERE NOT EXISTS (
    -- Only insert if there is no currently active record for this customer
    SELECT 1 
    FROM final.dim_customer fd
    WHERE fd.customer_nk = stg.customer_unique_id
      AND fd.current_flag = 'Current'
)
ORDER BY stg.customer_unique_id, stg.updated_at DESC;