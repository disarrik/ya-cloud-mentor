SELECT 
    payment_status,
    count() AS order_count,
    sum(total_amount) AS total_sum,
    avg(total_amount) AS avg_order_value
FROM orders
GROUP BY payment_status
ORDER BY total_sum DESC;


SELECT 
    o.payment_status,
    count(DISTINCT o.order_id) AS order_count,
    sum(oi.quantity) AS total_items_count,
    sum(oi.price * oi.quantity) AS total_items_revenue,
    avg(oi.price) AS avg_product_price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.payment_status
ORDER BY total_items_revenue DESC;


SELECT 
    toDate(order_date) AS date,
    count() AS order_count,
    sum(total_amount) AS daily_revenue
FROM orders
GROUP BY date
ORDER BY date DESC;


SELECT 
    user_id,
    user_name,
    count(order_id) AS order_count,
    sum(total_amount) AS total_spent
FROM orders
WHERE payment_status = 'paid'
GROUP BY user_id, user_name
ORDER BY total_spent DESC
LIMIT 10;

SELECT 
    user_id,
    user_name,
    count(order_id) AS order_count,
    sum(total_amount) AS total_spent
FROM orders
WHERE payment_status = 'paid'
GROUP BY user_id, user_name
ORDER BY order_count DESC
LIMIT 10;

