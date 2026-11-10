CREATE SCHEMA IF NOT EXISTS mart;


CREATE TABLE IF NOT EXISTS mart.mart_route_card (
    month               date        NOT NULL,
    loading_city_gcid   integer     NOT NULL,
    unloading_city_gcid integer     NOT NULL,
    body_group          varchar(32) NOT NULL,
    weight_category     varchar(4)  NOT NULL,
    p25_trip            numeric(14,2),
    median_trip         numeric(14,2),
    p75_trip            numeric(14,2),
    avg_trip            numeric(14,2),
    p25_km              numeric(14,2),
    median_km           numeric(14,2),
    p75_km              numeric(14,2),
    trips_count         integer     NOT NULL,
    carriers_count      integer     NOT NULL,
    cv_rate	            numeric(10,4),
    mom_pct	            numeric(10,2),
    yoy_pct             numeric(10,2),
    reliability_level   varchar(8)  NOT NULL,
    PRIMARY KEY (month, loading_city_gcid, unloading_city_gcid, body_group, weight_category)
);


CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    price_id	        bigint          NOT NULL,
    loading_date    	date            NOT NULL,
    loading_city_gcid	integer         NOT NULL,
    unloading_city_gcid	integer         NOT NULL,
    body_group	        varchar(32)     NOT NULL,
    actual_price_rub	numeric(14,2),
    predicted_price_rub	numeric(14,2)   NOT NULL,
    abs_error_rub   	numeric(14,2),
    ape_pct	            numeric(10,2),
    model_name	        varchar(64)     NOT NULL,
    scored_at	        timestamp(0)    NOT NULL DEFAULT now(),
    PRIMARY KEY (price_id, model_name)
);
