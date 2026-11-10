DELETE FROM mart.mart_route_card;


INSERT INTO mart.mart_route_card (
    month, loading_city_gcid, unloading_city_gcid, body_group, weight_category,
    p25_trip, median_trip, p75_trip, avg_trip,
    p25_km, median_km, p75_km,
    trips_count, carriers_count, cv_rate,
    mom_pct, yoy_pct, reliability_level
)
WITH base AS (
    SELECT
	    date_trunc('month', pd.price_date)::date AS month,
	    c.loading_city_gcid,
	    c.unloading_city_gcid,
	    bg.body_group,
	    c.weight_category,
	    c.price_id,
	    c.price_rub,
	    c.price_rub / c.distance AS price_km,
	    c.organization_id,
	    c.price_type,
	    pd.price_date
	FROM core.prices_clean c
	JOIN public.price_dates pd       ON pd.price_id = c.price_id
	JOIN core.v_price_body_group bg  ON bg.price_id = c.price_id
	WHERE c.is_fact
	  AND NOT c.complex
	  AND NOT c.outlier_mark
	  AND pd.price_date <= %(as_of)s::date
),
agg AS (
    SELECT
	    month,
	    loading_city_gcid,
	    unloading_city_gcid,
	    body_group,
	    weight_category,
	    percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub) AS p25_trip,
	    percentile_cont(0.5)  WITHIN GROUP (ORDER BY price_rub) AS median_trip,
	    percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub) AS p75_trip,
	    avg(price_rub)                                          AS avg_trip,
	    percentile_cont(0.25) WITHIN GROUP (ORDER BY price_km)  AS p25_km,
	    percentile_cont(0.5)  WITHIN GROUP (ORDER BY price_km)  AS median_km,
	    percentile_cont(0.75) WITHIN GROUP (ORDER BY price_km)  AS p75_km,
	    count(DISTINCT price_id)                                AS trips_count,
	    count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)) AS carriers_count,
	    CASE WHEN count(*) >= 2 AND avg(price_rub) <> 0
	         THEN stddev_samp(price_rub) / avg(price_rub)
	    END                                                     AS cv_rate,
	    max(price_date)                                         AS last_price_date
	FROM base
	GROUP BY month, loading_city_gcid, unloading_city_gcid, body_group, weight_category
)
SELECT
    a.month,
    a.loading_city_gcid,
    a.unloading_city_gcid,
    a.body_group,
    a.weight_category,
    a.p25_trip, a.median_trip, a.p75_trip, a.avg_trip,
    a.p25_km, a.median_km, a.p75_km,
    a.trips_count,
    a.carriers_count,
    a.cv_rate,
    CASE WHEN prev.median_trip <> 0
         THEN (a.median_trip - prev.median_trip) / prev.median_trip * 100
    END AS mom_pct,
    CASE WHEN py.median_trip <> 0
         THEN (a.median_trip - py.median_trip) / py.median_trip * 100
    END AS yoy_pct,
    CASE
        WHEN a.trips_count >= 30
         AND a.carriers_count >= 5
         AND %(as_of)s::date - a.last_price_date <= 30 THEN 'HIGH'
        WHEN a.trips_count >= 10 THEN 'MEDIUM'
        ELSE 'LOW_DATA'
    END AS reliability_level
FROM agg a
LEFT JOIN agg prev
       ON prev.loading_city_gcid   = a.loading_city_gcid
      AND prev.unloading_city_gcid = a.unloading_city_gcid
      AND prev.body_group          = a.body_group
      AND prev.weight_category     = a.weight_category
      AND prev.month = (a.month - interval '1 month')::date
LEFT JOIN agg py
       ON py.loading_city_gcid   = a.loading_city_gcid
      AND py.unloading_city_gcid = a.unloading_city_gcid
      AND py.body_group          = a.body_group
      AND py.weight_category     = a.weight_category
      AND py.month = (a.month - interval '12 months')::date;



SELECT reliability_level, count(*) FROM mart.mart_route_card GROUP BY 1 ORDER BY 1;
