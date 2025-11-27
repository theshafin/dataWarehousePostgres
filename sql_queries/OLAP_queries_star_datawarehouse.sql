-- Query Set 1
SELECT
    d.year,
    d.month_num,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_num
ORDER BY d.year, d.month_num;

--Query Set 2
SELECT
    g.region,
    p.category,
    s.ship_mode,
    SUM(f.profit) AS total_profit
FROM fact_sales f
JOIN dim_geography g  ON f.geography_key = g.geography_key
JOIN dim_product p    ON f.product_key = p.product_key
JOIN dim_ship_mode s  ON f.ship_mode_key = s.ship_mode_key
GROUP BY g.region, p.category, s.ship_mode
ORDER BY g.region, p.category, s.ship_mode;

--Query Set 3
SELECT
    c.customer_id,
    c.customer_name,
    SUM(f.profit) AS total_profit
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.customer_name
ORDER BY total_profit DESC
LIMIT 10;

--Query Set 4
SELECT
    p.category,
    p.sub_category,
    p.product_name,
    SUM(f.sales) AS total_sales,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.sales) DESC) AS rank_in_category
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category, p.sub_category, p.product_name
ORDER BY p.category, rank_in_category;

--Query Set 5
WITH monthly AS (
    SELECT
        d.year,
        d.month_num,
        SUM(f.profit) AS monthly_profit
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month_num
)
SELECT
    year,
    month_num,
    monthly_profit,
    LAG(monthly_profit) OVER (ORDER BY year, month_num) AS prev_month_profit,
    (monthly_profit - LAG(monthly_profit) OVER (ORDER BY year, month_num)) AS mom_change
FROM monthly
ORDER BY year, month_num;
