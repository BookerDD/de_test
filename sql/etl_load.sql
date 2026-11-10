CREATE TEMP TABLE stg ON COMMIT DROP AS
SELECT
    p.id AS price_id,
    p.loading_date,
    p.price,
    p.currency_code,
    p.nearest_rate_date,
    p.created_at,
    p.organization_id,
    p.loading_city_gcid,
    p.unloading_city_gcid,
    p.distance,
    p.total_weight,
    p.total_volume,
    p.body_type,
    p.complex,
    p.price_type,
    p.is_fact,
    p.outlier_mark,
    p.weight_category,
    p.volume_category,
    CASE WHEN p.currency_code = '643' THEN 1 ELSE er.value END AS fx_rate,
    CASE
	    WHEN p.price <= 0 THEN 'PRICE_NON_POSITIVE'
	    WHEN p.currency_code = '643' AND p.price <= 1000 THEN 'RUB_PRICE_LOWER_1000'
	    WHEN p.distance <= 0 THEN 'DISTANCE_NON_POSITIVE'
	    WHEN p.total_weight < 0 THEN 'WEIGHT_NEGATIVE'
	    WHEN p.total_volume < 0 THEN 'VOLUME_NEGATIVE'
	    WHEN NOT EXISTS (SELECT 1 FROM cities c WHERE c.gc_id = p.loading_city_gcid)
	      OR NOT EXISTS (SELECT 1 FROM cities c WHERE c.gc_id = p.unloading_city_gcid)
	        THEN 'UNKNOWN_CITY'
	    WHEN p.currency_code <> '643' AND er.value IS NULL THEN 'NO_RATE_MATCH'
	END AS reason_code
FROM prices p
LEFT JOIN exchange_rates er
       ON p.currency_code <> '643'
      AND er.num_code = p.currency_code
      AND er.setting_date = p.nearest_rate_date
WHERE (%(start_date)s::date IS NULL OR p.loading_date >= %(start_date)s::date)
    AND (%(end_date)s::date IS NULL OR p.loading_date <= %(end_date)s::date);;


DELETE FROM core.prices_clean
WHERE ((%(start_date)s::date IS NULL OR loading_date >= %(start_date)s::date)
    AND (%(end_date)s::date IS NULL OR loading_date <= %(end_date)s::date))
    OR price_id IN (SELECT price_id FROM stg);


DELETE FROM core.dq_quarantine_prices
WHERE ((%(start_date)s::date IS NULL OR loading_date >= %(start_date)s::date)
    AND (%(end_date)s::date IS NULL OR loading_date <= %(end_date)s::date))
    OR price_id IN (SELECT price_id FROM stg);


INSERT INTO core.dq_quarantine_prices (price_id, loading_date, reason_code)
SELECT price_id, loading_date, reason_code
FROM stg
WHERE reason_code IS NOT NULL;


INSERT INTO core.prices_clean (
    price_id, created_at, organization_id,
    loading_date, loading_city_gcid, unloading_city_gcid,
    distance, total_weight, total_volume,
    body_type, price, currency_code,
    nearest_rate_date, fx_rate, price_rub,
    complex, price_type, is_fact,
    outlier_mark, weight_category, volume_category
)
SELECT
    price_id,
    created_at,
    organization_id,
    loading_date,
    loading_city_gcid,
    unloading_city_gcid,
    distance,
    total_weight,
    total_volume,
    body_type,
    price,
    currency_code,
    nearest_rate_date,
    fx_rate,
    ROUND((price * fx_rate)::numeric, 2) AS price_rub,
    complex,
    price_type,
    is_fact,
    outlier_mark,
    COALESCE(weight_category,
        CASE
            WHEN round(total_weight::numeric, 1) >= 30  THEN 'G20T'
            WHEN round(total_weight::numeric, 1) >= 10  THEN 'FTL'
            WHEN round(total_weight::numeric, 1) >= 5   THEN '10T'
            WHEN round(total_weight::numeric, 1) >= 2.5 THEN '5T'
            WHEN round(total_weight::numeric, 1) >= 0.5 THEN 'L5T'
            ELSE 'OTH'
        END) AS weight_category,
    COALESCE(volume_category,
        CASE WHEN total_volume > 0 AND total_volume <= 120 THEN 'FTL' ELSE 'OTH' END) AS volume_category
FROM stg
WHERE reason_code IS NULL;


SELECT COALESCE(reason_code, 'LOADED') AS status, count(*) AS cnt
FROM stg
GROUP BY 1
ORDER BY 1;
