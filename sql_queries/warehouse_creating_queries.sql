CREATE TABLE dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       TEXT UNIQUE,
    customer_name     TEXT,
    segment           TEXT
);

CREATE TABLE dim_product (
    product_key       SERIAL PRIMARY KEY,
    product_id        TEXT UNIQUE,
    category          TEXT,
    sub_category      TEXT,
    product_name      TEXT
);

CREATE TABLE dim_date (
    date_key          INTEGER PRIMARY KEY,
    full_date         DATE,
    year              INTEGER,
    quarter           TEXT,
    month             TEXT,
    month_num         INTEGER,
    day               INTEGER,
    weekday           TEXT
);

CREATE TABLE dim_geography (
    geography_key     SERIAL PRIMARY KEY,
    country_region    TEXT,
    state_province    TEXT,
    city              TEXT,
    postal_code       TEXT,
    region            TEXT
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


INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_num, day, weekday)
SELECT
    TO_CHAR(date, 'YYYYMMDD')::INTEGER AS date_key,
    date AS full_date,
    EXTRACT(YEAR FROM date)::INT AS year,
    CASE
        WHEN EXTRACT(MONTH FROM date) IN (1,2,3) THEN 'Q1'
        WHEN EXTRACT(MONTH FROM date) IN (4,5,6) THEN 'Q2'
        WHEN EXTRACT(MONTH FROM date) IN (7,8,9) THEN 'Q3'
        ELSE 'Q4'
    END AS quarter,
    TO_CHAR(date, 'Month') AS month,
    EXTRACT(MONTH FROM date)::INT AS month_num,
    EXTRACT(DAY FROM date)::INT AS day,
    TO_CHAR(date, 'Day') AS weekday
FROM generate_series('2019-01-01'::DATE, '2025-12-31'::DATE, '1 day') AS date;

CREATE TABLE staging_orders (
    row_id INTEGER,
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
    sales NUMERIC,
    quantity INTEGER,
    discount NUMERIC,
    profit NUMERIC
);

INSERT INTO dim_customer (customer_id, customer_name, segment)
SELECT DISTINCT
    customer_id,
    customer_name,
    segment
FROM staging_orders;

DELETE FROM dim_product;

INSERT INTO dim_product (product_id, category, sub_category, product_name)
SELECT DISTINCT ON (product_id)
    TRIM(product_id),
    TRIM(category),
    TRIM(sub_category),
    TRIM(product_name)
FROM staging_orders
ORDER BY product_id;

INSERT INTO dim_ship_mode (ship_mode)
SELECT DISTINCT ship_mode
FROM staging_orders;

INSERT INTO dim_geography (country_region, state_province, city, postal_code, region)
SELECT DISTINCT
    country_region,
    state_province,
    city,
    postal_code,
    region
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
    TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER AS date_key,
    TO_CHAR(o.ship_date, 'YYYYMMDD')::INTEGER AS ship_date_key,
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
JOIN dim_date d          ON TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER = d.date_key;


Select * FROM fact_sales;

SELECT 
    (SELECT COUNT(*) FROM staging_orders) AS staging_count,
    (SELECT COUNT(*) FROM fact_sales) AS fact_count;



