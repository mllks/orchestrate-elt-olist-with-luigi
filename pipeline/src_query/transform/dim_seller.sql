/*
 * SCD Type 2 Transformation for dim_seller
 * 
 * Strategy:
 * 1. Identify records where location attributes (city, state, lat, lng) have changed.
 * 2. Close the old version by updating valid_to and current_flag.
 * 3. Insert new versions for changed records and entirely new records.
 */

-- Step 1: Expire existing records that have changes in tracked location attributes
UPDATE final.dim_seller fs
SET 
    valid_to = CURRENT_TIMESTAMP,
    current_flag = 'Expired',
    updated_at = CURRENT_TIMESTAMP
FROM stg.sellers stg
LEFT JOIN (
    -- Get unique lat/lng per zip code to avoid duplicates
    SELECT 
        geolocation_zip_code_prefix, 
        AVG(geolocation_lat) as geolocation_lat, 
        AVG(geolocation_lng) as geolocation_lng
    FROM stg.geolocation
    GROUP BY geolocation_zip_code_prefix
) geo ON stg.seller_zip_code_prefix = geo.geolocation_zip_code_prefix
WHERE fs.seller_nk = stg.seller_id
  AND fs.current_flag = 'Current'
  AND (
      fs.seller_city IS DISTINCT FROM stg.seller_city OR
      fs.seller_state IS DISTINCT FROM stg.seller_state OR
      fs.seller_latitude IS DISTINCT FROM geo.geolocation_lat OR
      fs.seller_longitude IS DISTINCT FROM geo.geolocation_lng
  );

-- Step 2: Insert new records (both entirely new sellers and new versions of updated sellers)
INSERT INTO final.dim_seller (
    seller_nk,
    seller_city,
    seller_state,
    seller_latitude,
    seller_longitude,
    valid_from,
    valid_to,
    current_flag
)
SELECT 
    stg.seller_id,
    stg.seller_city,
    stg.seller_state,
    geo.geolocation_lat,
    geo.geolocation_lng,
    CURRENT_TIMESTAMP AS valid_from,
    '9999-12-31 23:59:59'::TIMESTAMP AS valid_to,
    'Current' AS current_flag
FROM stg.sellers stg
LEFT JOIN (
    SELECT 
        geolocation_zip_code_prefix, 
        AVG(geolocation_lat) as geolocation_lat, 
        AVG(geolocation_lng) as geolocation_lng
    FROM stg.geolocation
    GROUP BY geolocation_zip_code_prefix
) geo ON stg.seller_zip_code_prefix = geo.geolocation_zip_code_prefix
WHERE NOT EXISTS (
    -- Only insert if there is no currently active record for this seller
    SELECT 1 
    FROM final.dim_seller fs
    WHERE fs.seller_nk = stg.seller_id
      AND fs.current_flag = 'Current'
);