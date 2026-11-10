FROM apache/superset:4.1.2

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.11 clickhouse-connect==0.8.18
RUN mkdir -p /app/pythonpath && printf '%s\n' \
    'import os' \
    'SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]' \
    'SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DB_URI"]' \
    > /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
USER superset
