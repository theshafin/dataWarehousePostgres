-- Query Set 1
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit
FROM staging_orders
GROUP BY month
ORDER BY month;

-- Query Set 2

SELECT
    region,
    category,
    ship_mode,
    SUM(profit) AS total_profit
FROM staging_orders
GROUP BY region, category, ship_mode
ORDER BY region, category, ship_mode;

-- Query Set 3
SELECT
    customer_id,
    customer_name,
    SUM(profit) AS total_profit
FROM staging_orders
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC
LIMIT 10;

-- Query Set 4
SELECT
    category,
    sub_category,
    product_name,
    SUM(sales) AS total_sales,
    RANK() OVER (PARTITION BY category ORDER BY SUM(sales) DESC) AS rank_in_category
FROM staging_orders
GROUP BY category, sub_category, product_name
ORDER BY category, rank_in_category;

-- Query Set 5
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(profit) AS monthly_profit
    FROM staging_orders
    GROUP BY month
)
SELECT
    month,
    monthly_profit,
    LAG(monthly_profit) OVER (ORDER BY month) AS prev_month_profit,
    (monthly_profit - LAG(monthly_profit) OVER (ORDER BY month)) AS mom_change
FROM monthly
ORDER BY month;