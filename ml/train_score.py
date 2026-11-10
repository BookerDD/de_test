import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from catboost import CatBoostRegressor
from psycopg2.extras import execute_values
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
    root_mean_squared_error,
)

log = logging.getLogger("ml")

MODEL_NAME = "catboost_v1"
TARGET = "price_rub"

CAT_FEATURES = [
    "loading_region", "unloading_region",
    "loading_city", "unloading_city",
    "body_group", "weight_category", "volume_category",
]
NUM_FEATURES = [
    "loading_lat", "loading_lon", "unloading_lat", "unloading_lon",
    "distance", "total_weight", "total_volume",
    "year", "month_sin", "month_cos", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
]
FEATURES = CAT_FEATURES + NUM_FEATURES

FORBIDDEN = {
    "price", "price_rub", "price_with_vat", "fx_rate",
    "price_group_id", "avg_price", "min_km_price", "max_km_price",
    "outlier_mark",
}

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
DATA_SQL = (SQL_DIR / "ml_dataset.sql").read_text(encoding="utf-8")
INSERT_SQL = (SQL_DIR / "ml_insert_predictions.sql").read_text(encoding="utf-8")


def load_data(conn, train_start):
    with conn.cursor() as cur:
        cur.execute(DATA_SQL, {"train_start": train_start.date()})
        df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    df["loading_date"] = pd.to_datetime(df["loading_date"])
    return df


def add_date_features(df):
    d = df["loading_date"]
    df["year"] = d.dt.year
    for name, value, period in [
        ("month", d.dt.month, 12),
        ("dow", d.dt.dayofweek, 7),
        ("doy", d.dt.dayofyear, 365.25),
    ]:
        df[f"{name}_sin"] = np.sin(2 * np.pi * value / period)
        df[f"{name}_cos"] = np.cos(2 * np.pi * value / period)
    return df


def save_predictions(conn, model, test, future):
    scored = pd.concat([test, future.assign(**{TARGET: np.nan})])
    out = scored[["price_id", "loading_date", "loading_city_gcid", "unloading_city_gcid", "body_group"]].copy()
    out["loading_date"] = out["loading_date"].dt.date
    out["actual"] = scored[TARGET]
    out["predicted"] = model.predict(scored[FEATURES])
    out["abs_error"] = (out["actual"] - out["predicted"]).abs()
    out["ape"] = out["abs_error"] / out["actual"] * 100
    out = out.round(2)
    out["model_name"] = MODEL_NAME
    rows = list(out.astype(object).where(out.notna(), None).itertuples(index=False, name=None))

    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM mart.ml_price_predictions WHERE model_name = %s", (MODEL_NAME,))
        execute_values(cur, INSERT_SQL, rows, page_size=5000)
    return len(rows)


def run_ml(conn, as_of):
    as_of = pd.Timestamp(as_of)
    train_start = as_of - pd.DateOffset(months=3)
    test_start = as_of - pd.DateOffset(weeks=3)
    log.info("ML start: as_of %s", as_of.date())

    df = add_date_features(load_data(conn, train_start))
    d = df["loading_date"]
    train = df[(d < test_start) & ~df["outlier_mark"]]
    test = df[(d >= test_start) & (d <= as_of)]
    future = df[d > as_of]
    log.info("Train: %s, test: %s, future: %s", len(train), len(test), len(future))

    model = CatBoostRegressor(
        iterations=1000, learning_rate=0.05, depth=6, loss_function="RMSE",
        random_seed=42, verbose=False, allow_writing_files=False,
    )
    model.fit(train[FEATURES], train[TARGET], cat_features=CAT_FEATURES)

    clean = test[~test["outlier_mark"]]
    y, p = clean[TARGET], model.predict(clean[FEATURES])
    metrics = {
        "R2": r2_score(y, p),
        "MAPE_pct": mean_absolute_percentage_error(y, p) * 100,
        "RMSE": root_mean_squared_error(y, p),
        "MAE": mean_absolute_error(y, p),
    }
    for name, value in metrics.items():
        log.info("%s: %.4f", name, value)

    log.info("Predictions saved: %s", save_predictions(conn, model, test, future))
    return metrics


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    conn = psycopg2.connect("")
    try:
        run_ml(conn, os.getenv("AS_OF", "2026-10-26"))
    finally:
        conn.close()
