CREATE OR REPLACE TABLE mart.mart_route_card (
    month               Date,
    loading_city_gcid   Int32,
    unloading_city_gcid Int32,
    body_group          String,
    weight_category     String,
    p25_trip            Nullable(Decimal(14,2)),
    median_trip         Nullable(Decimal(14,2)),
    p75_trip            Nullable(Decimal(14,2)),
    avg_trip            Nullable(Decimal(14,2)),
    p25_km              Nullable(Decimal(14,2)),
    median_km           Nullable(Decimal(14,2)),
    p75_km              Nullable(Decimal(14,2)),
    trips_count         Int32,
    carriers_count      Int32,
    cv_rate	            Nullable(Decimal(10,4)),
    mom_pct	            Nullable(Decimal(10,2)),
    yoy_pct             Nullable(Decimal(10,2)),
    reliability_level   String,
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (month, loading_city_gcid, unloading_city_gcid, body_group, weight_category)
AS SELECT * FROM postgresql('postgres:5432', 'deals_bigdata', 'mart_route_card',
                            '{{ conn.deals_bigdata.login }}', '{{ conn.deals_bigdata.password }}', 'mart')
