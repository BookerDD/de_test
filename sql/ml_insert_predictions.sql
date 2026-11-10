INSERT INTO mart.ml_price_predictions (
    price_id, loading_date, loading_city_gcid, unloading_city_gcid, body_group,
    actual_price_rub, predicted_price_rub, abs_error_rub, ape_pct, model_name
) VALUES %s;
