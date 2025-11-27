CREATE TABLE staging_orders (
    row_id INT,
    order_id TEXT,
    order_date DATE,
    ship_date DATE,
    ship_mode TEXT,
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    country_region TEXT,
    city TEXT,
    state_province TEXT,
    postal_code TEXT,
    region TEXT,
    product_id TEXT,
    category TEXT,
    sub_category TEXT,
    product_name TEXT,
    sales TEXT,
    quantity TEXT,
    discount TEXT,
    profit TEXT
)

COPY tableau_staging (
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country_region,
    city,
    state_province,
    postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit
) 
FROM './data/data.csv'
WITH (
    FORMAT CSV,
    HEADER true,
    DELIMITER ',',
    ENCODING 'UTF8',
    NULL ''
);

CREATE TYPE SEGMENT AS ENUM (SELECT DISTINCT upper(segment) FROM staging_orders);
CREATE TYPE P_CATEGORY AS ENUM (SELECT DISTINCT upper(trim(category)) FROM staging_orders);
CREATE TYPE REGION AS ENUM (SELECT DISTINCT upper(region) FROM staging_orders);

-- Originally intended to use enum, but there wasn't a good way to convert between each other.
CREATE OR REPLACE FUNCTION month_text(SMALLINT)
returns TEXT as $$
SELECT to_char(to_timestamp ($1::TEXT, 'MM'), 'Mon')
$$ language sql;

CREATE OR REPLACE FUNCTION weekday_text(SMALLINT)
returns TEXT as $$
SELECT to_char(to_timestamp ($1::TEXT, 'DD'), 'Dy')
$$ language sql;

CREATE OR REPLACE FUNCTION date_quarter(DATE)
returns TEXT as $$
SELECT to_char($1, 'Q')
$$ language sql;

----------------------------------------------------------------------------------

CREATE TABLE dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       TEXT UNIQUE,
    customer_name     TEXT,
    segment           SEGMENT,
);

CREATE TABLE dim_product (
    product_key       SERIAL PRIMARY KEY,
    product_id        TEXT UNIQUE,
    category          P_CATEGORY,
    sub_category      TEXT,
    product_name      TEXT
);

CREATE TABLE dim_date (
    timestamp         TIMESTAMP PRIMARY KEY,
    year              SMALLINT,
    month             SMALLINT,
    day               SMALLINT,
    CHECK (month BETWEEN 1 AND 12),
    CHECK (day BETWEEN 1 AND 32)
);

CREATE TABLE dim_geography (
    geography_key     SERIAL PRIMARY KEY,
    country_region    TEXT,
    state_province    TEXT,
    city              TEXT,
    postal_code       TEXT,
    region            REGION
);

CREATE TABLE dim_ship_mode (
    ship_mode_key     SERIAL PRIMARY KEY,
    ship_mode         TEXT UNIQUE
);

CREATE TABLE fact_sales (
    sales_key         SERIAL PRIMARY KEY,

    -- Foreign Keys
    order_id          TEXT,
    customer_key      INTEGER REFERENCES dim_customer(customer_key),
    product_key       INTEGER REFERENCES dim_product(product_key),
    date_key          INTEGER REFERENCES dim_date(date_key),
    ship_date_key     INTEGER REFERENCES dim_date(date_key),
    geography_key     INTEGER REFERENCES dim_geography(geography_key),
    ship_mode_key     INTEGER REFERENCES dim_ship_mode(ship_mode_key),

    -- Measures
    sales             NUMERIC,
    quantity          INTEGER,
    discount          NUMERIC,
    profit            NUMERIC
);

INSERT INTO dim_customer (customer_id, customer_name, segment)
SELECT DISTINCT
    customer_id,
    customer_name,
    upper(segment)::SEGMENT
FROM staging_orders;

INSERT INTO dim_product (full_date, category, sub_category, product_name)
SELECT DISTINCT ON (product_id)
    TRIM(product_id),
    uppter(TRIM(category))::P_CATEGORY,
    TRIM(sub_category),
    TRIM(product_name)
FROM staging_orders
ORDER BY product_id;

INSERT INTO dim_date (timestamp, year, month, day)
SELECT
    date + '00:00:00'::time as timestamp,
    EXTRACT(YEAR FROM date)::INT AS year,
    EXTRACT(MONTH FROM date)::INT AS month,
    EXTRACT(DAY FROM date)::INT AS day,
FROM generate_series(
    (SELECT MIN(dates) FROM (SELECT CONCAT(order_date, ship_date) as dates FROM staging_orders)), 
    (SELECT MAX(dates) FROM (SELECT CONCAT(order_date, ship_date) as dates FROM staging_orders)),
    '1 day'::interval) AS date;

INSERT INTO dim_geography (country_region, state_province, city, postal_code, region)
SELECT DISTINCT
    country_region,
    state_province,
    city,
    postal_code,
    uppter(region)::REGION
FROM staging_orders;

INSERT INTO fact_sales (
    order_id,
    customer_key,
    product_key,
    date_key,
    ship_date_key,
    geography_key,
    ship_mode_key,
    sales,
    quantity,
    discount,
    profit
)
SELECT
    o.order_id,

    -- foreign keys
    c.customer_key,
    p.product_key,
    o.order_date + '00:00:00'::time AS timestamp,
    o.ship_date + '00:00:00'::time AS timestamp,
    g.geography_key,
    s.ship_mode_key,

    -- measures
    o.sales,
    o.quantity,
    o.discount,
    o.profit
FROM staging_orders o
JOIN dim_customer c      ON o.customer_id = c.customer_id
JOIN dim_product p       ON o.product_id = p.product_id
JOIN dim_geography g     ON o.city = g.city
                         AND o.state_province = g.state_province
                         AND o.country_region = g.country_region
                         AND o.postal_code = g.postal_code
JOIN dim_ship_mode s     ON o.ship_mode = s.ship_mode
JOIN dim_date d          ON o.order_date + '00:00:00'::time = d.timestamp;