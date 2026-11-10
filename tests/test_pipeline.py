import psycopg2

from etl.pipeline import run_etl, run_mart
from ml.train_score import FEATURES, FORBIDDEN


def sql_code(sql):
    with psycopg2.connect("") as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_no_invalid_rows_in_core():
    assert sql_code("""
        SELECT count(*) FROM core.prices_clean
        WHERE price <= 0
           OR (currency_code = '643' AND price <= 1000)
           OR distance <= 0
           OR total_weight < 0
           OR total_volume < 0
    """) == 0


def test_no_rows_lost():
    core = sql_code("SELECT count(*) FROM core.prices_clean")
    quarantine = sql_code("SELECT count(*) FROM core.dq_quarantine_prices")
    assert core + quarantine == sql_code("SELECT count(*) FROM public.prices")


def test_foreign_currency_without_rate_not_in_core():
    assert sql_code("""
        SELECT count(*) FROM core.prices_clean
        WHERE currency_code <> '643' AND nearest_rate_date IS NULL
    """) == 0


def test_rerun_does_not_duplicate():
    before = (sql_code("SELECT count(*) FROM core.prices_clean"),
              sql_code("SELECT count(*) FROM mart.mart_route_card"))

    conn = psycopg2.connect("")
    run_etl(conn)
    run_mart(conn, "2026-10-26")
    conn.close()

    after = (sql_code("SELECT count(*) FROM core.prices_clean"),
             sql_code("SELECT count(*) FROM mart.mart_route_card"))
    assert after == before


def test_no_future_months_in_mart():
    assert sql_code("SELECT count(*) FROM mart.mart_route_card WHERE month > '2026-10-26'") == 0


def test_no_leakage_in_features():
    assert not set(FEATURES) & FORBIDDEN


def test_future_predictions_have_no_actual():
    assert sql_code("""
        SELECT count(*) FROM mart.ml_price_predictions
        WHERE loading_date > '2026-10-26' AND actual_price_rub IS NOT NULL
    """) == 0
