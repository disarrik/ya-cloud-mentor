transactions_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(transactions_path)

logs_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(logs_path)

transactions_df.write \
    .mode("overwrite") \
    .saveAsTable("transactions_v2")

logs_df.write \
    .mode("overwrite") \
    .saveAsTable("logs_v2")
