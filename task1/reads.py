good_currencies = spark.sql("""
SELECT 
    currency,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM 
    transactions_v2
WHERE 
    currency IN ('USD', 'EUR', 'RUB')
GROUP BY 
    currency
ORDER BY 
    total_amount DESC
""")
good_currencies.show()

fraud_analysis = spark.sql("""
SELECT 
    CASE WHEN is_fraud = 1 THEN 'Мошенническая' ELSE 'Нормальная' END AS transaction_type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM 
    transactions_v2
GROUP BY 
    is_fraud
""")
fraud_analysis.show()

daily_stats = spark.sql("""
SELECT 
    TO_DATE(transaction_date) AS date,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM 
    transactions_v2
GROUP BY 
    TO_DATE(transaction_date)
ORDER BY 
    date
""")
daily_stats.show()

monthly_analysis = spark.sql("""
SELECT 
    MONTH(transaction_date) AS month,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count
FROM 
    transactions_v2
GROUP BY 
    MONTH(transaction_date)
ORDER BY 
    month
""")
monthly_analysis.show()


logs_per_transaction = spark.sql("""
SELECT 
    t.transaction_id,
    t.amount,
    t.currency,
    t.merchant_category,
    t.is_fraud,
    COUNT(l.log_id) AS log_count
FROM 
    transactions_v2 t
LEFT JOIN 
    logs_v2 l ON t.transaction_id = l.transaction_id
GROUP BY 
    t.transaction_id, t.amount, t.currency, t.merchant_category, t.is_fraud
ORDER BY 
    log_count DESC
""")
logs_per_transaction.show()

top_log_categories = spark.sql("""
SELECT 
    l.category,
    COUNT(*) AS log_count,
    COUNT(DISTINCT l.transaction_id) AS transaction_count
FROM 
    logs_v2 l
JOIN 
    transactions_v2 t ON l.transaction_id = t.transaction_id
GROUP BY 
    l.category
ORDER BY 
    log_count DESC
""")
top_log_categories.show()

