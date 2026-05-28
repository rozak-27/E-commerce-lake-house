-- ══════════════════════════════════════════════════
--  Sample Trino Queries — E-Commerce Lakehouse
-- ══════════════════════════════════════════════════

-- 1. Revenue by category (last 7 days)
SELECT category,
       SUM(revenue)         AS total_revenue,
       SUM(order_count)     AS total_orders,
       AVG(avg_order_value) AS aov
FROM iceberg.gold.order_agg
WHERE hour >= NOW() - INTERVAL '7' DAY
GROUP BY category
ORDER BY total_revenue DESC;

-- 2. Conversion funnel
SELECT event_type,
       COUNT(*)                                              AS events,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)  AS pct
FROM iceberg.silver.user_events
GROUP BY event_type
ORDER BY events DESC;

-- 3. Top 10 products by revenue
SELECT product_id, product_name, category,
       SUM(total_amount)  AS revenue,
       COUNT(order_id)    AS orders,
       AVG(rating)        AS avg_rating
FROM iceberg.silver.user_events
WHERE event_type = 'checkout'
GROUP BY product_id, product_name, category
ORDER BY revenue DESC
LIMIT 10;

-- 4. Daily active users (last 30 days)
SELECT DATE(event_time)         AS date,
       COUNT(DISTINCT user_id)  AS dau,
       COUNT(*)                 AS total_events
FROM iceberg.silver.user_events
GROUP BY DATE(event_time)
ORDER BY date DESC
LIMIT 30;

-- 5. Iceberg time travel — compare row count 1 hour ago vs now
SELECT 'now'    AS snapshot, COUNT(*) FROM iceberg.bronze.raw_events
UNION ALL
SELECT '1h ago' AS snapshot, COUNT(*)
FROM iceberg.bronze.raw_events
FOR TIMESTAMP AS OF NOW() - INTERVAL '1' HOUR;

-- 6. Platform breakdown
SELECT platform, COUNT(DISTINCT user_id) AS users, COUNT(*) AS events
FROM iceberg.silver.user_events
GROUP BY platform
ORDER BY users DESC;
