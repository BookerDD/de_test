CREATE OR REPLACE TABLE mart.ml_price_predictions (
    price_id	        Int64,
    loading_date    	Date,
    loading_city_gcid	Int32,
    unloading_city_gcid	Int32,
    body_group	        String,
    actual_price_rub	Nullable(Decimal(14,2)),
    predicted_price_rub	Decimal(14,2),
    abs_error_rub   	Nullable(Decimal(14,2)),
    ape_pct	            Nullable(Decimal(10,2)),
    model_name	        String,
    scored_at	        DateTime DEFAULT now(),
)
ENGINE = MergeTree
ORDER BY (model_name, loading_date, price_id)
AS SELECT * FROM postgresql('postgres:5432', 'deals_bigdata', 'ml_price_predictions',
                            '{{ conn.deals_bigdata.login }}', '{{ conn.deals_bigdata.password }}', 'mart')
