import argparse
import logging
import os
from pathlib import Path

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("etl")

SQL_FILE = Path(__file__).resolve().parent.parent / "sql" / "etl_load.sql"
MART_SQL_FILE = Path(__file__).resolve().parent.parent / "sql" / "mart_route_card.sql"


def run_etl(conn, start_date=None, end_date=None):
    log.info("ETL start: window %s .. %s", start_date or "-", end_date or "-")
    sql = SQL_FILE.read_text(encoding="utf-8")
    with conn, conn.cursor() as cur:
        cur.execute(sql, {"start_date": start_date, "end_date": end_date})
        stats = dict(cur.fetchall())
    for status, cnt in stats.items():
        log.info("%s: %s", status, cnt)
    log.info("ETL done")
    return stats

def run_mart(conn, as_of):
    log.info("Mart start: as_of %s", as_of)
    sql = MART_SQL_FILE.read_text(encoding="utf-8")
    with conn, conn.cursor() as cur:
        cur.execute(sql, {"as_of": as_of})
        stats = dict(cur.fetchall())
    for level, cnt in stats.items():
        log.info("%s: %s", level, cnt)
    log.info("Mart done")
    return stats

def main():
    parser = argparse.ArgumentParser(description="Load prices into core with DQ quarantine")
    parser.add_argument("--start", default=os.getenv("ETL_START_DATE") or None, help="YYYY-MM-DD")
    parser.add_argument("--end", default=os.getenv("ETL_END_DATE") or None, help="YYYY-MM-DD")
    parser.add_argument("--as-of", default=os.getenv("AS_OF", "2026-10-26"), help="YYYY-MM-DD")
    args = parser.parse_args()

    conn = psycopg2.connect("")
    try:
        run_etl(conn, args.start, args.end)
        run_mart(conn, args.as_of)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
