SELECT
    c.price_id,
    c.loading_date,
    c.loading_city_gcid,
    c.unloading_city_gcid,
    lc.region    AS loading_region,
    lc.city      AS loading_city,
    lc.latitude  AS loading_lat,
    lc.longitude AS loading_lon,
    uc.region    AS unloading_region,
    uc.city      AS unloading_city,
    uc.latitude  AS unloading_lat,
    uc.longitude AS unloading_lon,
    c.distance,
    c.total_weight,
    c.total_volume,
    CASE
        WHEN c.body_type IS NULL                     THEN 'Не указан'
        WHEN c.body_type IN (56, 57, 58, 59, 60, 61) THEN 'Тент'
        WHEN c.body_type IN (4, 46, 47, 48)          THEN 'Рефрижератор'
        WHEN c.body_type = 2                         THEN 'Изотерм'
        WHEN c.body_type IN (1, 15)                  THEN 'Борт'
        ELSE 'Прочие'
    END AS body_group,
    c.weight_category,
    c.volume_category,
    c.outlier_mark,
    c.price_rub::float AS price_rub
FROM core.prices_clean c
JOIN core.v_city lc ON lc.gc_id = c.loading_city_gcid
JOIN core.v_city uc ON uc.gc_id = c.unloading_city_gcid
WHERE c.is_fact
  AND NOT c.complex
  AND c.loading_date >= %(train_start)s
ORDER BY c.price_id;
