INSERT INTO stg.geolocation
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
SELECT
    geolocation_zip_code_prefix, 
    geolocation_lat, 
    geolocation_lng, 
    geolocation_city, 
    geolocation_state
FROM
    public.geolocation  
    
ON CONFLICT(geolocation_zip_code_prefix) 
DO UPDATE SET
        geolocation_lat = EXCLUDED.geolocation_lat,
        geolocation_lng = EXCLUDED.geolocation_lng,
        geolocation_city = EXCLUDED.geolocation_city,
        geolocation_state = EXCLUDED.geolocation_state,
        updated_at = CASE WHEN 
                            stg.geolocation.geolocation_lat IS DISTINCT FROM EXCLUDED.geolocation_lat
                            OR stg.geolocation.geolocation_lng IS DISTINCT FROM EXCLUDED.geolocation_lng
                            OR stg.geolocation.geolocation_city IS DISTINCT FROM EXCLUDED.geolocation_city
                            OR stg.geolocation.geolocation_state IS DISTINCT FROM EXCLUDED.geolocation_state          
                        THEN 
                            CURRENT_TIMESTAMP
                        ELSE
                            stg.geolocation.updated_at
                        END;                    
