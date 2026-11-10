CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.prices_clean (
    price_id            bigint           PRIMARY KEY,
    created_at          timestamp(0),
    organization_id     integer,
    loading_date        date             NOT NULL,
    loading_city_gcid   integer          NOT NULL,
    unloading_city_gcid integer          NOT NULL,
    distance            double precision NOT NULL,
    total_weight        double precision NOT NULL,
    total_volume        double precision NOT NULL,
    body_type           integer,
    price               double precision NOT NULL,
    currency_code       varchar(3)       NOT NULL,
    nearest_rate_date   date,
    fx_rate             double precision NOT NULL,
    price_rub           numeric(14,2)    NOT NULL,
    complex             boolean          NOT NULL,
    price_type          smallint         NOT NULL,
    is_fact             boolean          NOT NULL,
    outlier_mark        boolean          NOT NULL,
    weight_category     varchar(4)       NOT NULL,
    volume_category     varchar(4)       NOT NULL,
    loaded_at           timestamp(0)     NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS prices_clean_loading_date_idx ON core.prices_clean (loading_date);

CREATE TABLE IF NOT EXISTS core.dq_quarantine_prices (
    price_id     bigint       PRIMARY KEY,
    loading_date date         NOT NULL,
    reason_code  varchar(32)  NOT NULL,
    loaded_at    timestamp(0) NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dq_quarantine_loading_date_idx ON core.dq_quarantine_prices (loading_date);


-- Представление городов с выделением региона и удалением дубликатов
CREATE OR REPLACE VIEW core.v_city AS
SELECT DISTINCT ON (gc_id)
				   gc_id,
				   name AS city,
				   trim(split_part(full_name_local, ',', 2)) AS region,
				   latitude,
				   longitude
FROM public.cities
ORDER BY gc_id, id;


CREATE OR REPLACE VIEW core.v_price_body_group AS
WITH codes AS (
	SELECT
		pbt.price_id,
		pbt.body_type_id AS code
	FROM public.price_body_types pbt

	UNION ALL

	SELECT
		p.id AS price_id,
		p.body_type AS code
	FROM public.prices AS p
	WHERE NOT EXISTS (
		SELECT 1
		FROM public.price_body_types b
		WHERE b.price_id = p.id
	)
)
SELECT DISTINCT
    price_id,
    CASE
        WHEN code IS NULL                       THEN 'Не указан'
        WHEN code IN (56, 57, 58, 59, 60, 61)   THEN 'Тент'
        WHEN code IN (4, 46, 47, 48)            THEN 'Рефрижератор'
        WHEN code = 2                           THEN 'Изотерм'
        WHEN code IN (1, 15)                    THEN 'Борт'
        ELSE 'Прочие'
    END AS body_group
FROM codes;
