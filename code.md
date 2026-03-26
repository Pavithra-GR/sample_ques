sales_df = spark.read.format("delta").load("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Bronze/sales")

customers_df = spark.read.format("delta").load("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Bronze/customers")

products_df = spark.read.format("delta").load("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Bronze/products")

stores_df = spark.read.format("delta").load("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Bronze/stores")

exchange_df = spark.read.format("delta").load("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Bronze/exchange_rates")




def clean_columns(df):
    return df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

sales_df = clean_columns(sales_df)
customers_df = clean_columns(customers_df)
products_df = clean_columns(products_df)
stores_df = clean_columns(stores_df)
exchange_df = clean_columns(exchange_df)



from pyspark.sql.functions import col, to_date

sales_df = sales_df \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("delivery_date", to_date(col("delivery_date"))) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("customerkey", col("customerkey").cast("int")) \
    .withColumn("productkey", col("productkey").cast("int")) \
    .withColumn("storekey", col("storekey").cast("int"))

products_df = products_df \
    .withColumn("unit_price_usd", col("unit_price_usd").cast("double")) \
    .withColumn("unit_cost_usd", col("unit_cost_usd").cast("double"))

exchange_df = exchange_df \
    .withColumn("date", to_date(col("date"))) \
    .withColumn("exchange", col("exchange").cast("double"))


sales_df = sales_df.fillna({"quantity": 0})

customers_df = customers_df.dropna(subset=["customerkey"])
products_df = products_df.dropna(subset=["productkey"])
stores_df = stores_df.dropna(subset=["storekey"])


sales_df = sales_df.dropDuplicates()
customers_df = customers_df.dropDuplicates()
products_df = products_df.dropDuplicates()
stores_df = stores_df.dropDuplicates()
exchange_df = exchange_df.dropDuplicates()


from pyspark.sql.functions import datediff

sales_df = sales_df.withColumn(
    "delivery_days",
    datediff("delivery_date", "order_date")
)


sales_df = sales_df.join(products_df, "productkey", "left")

sales_df = sales_df.withColumn(
    "revenue_usd",
    col("quantity") * col("unit_price_usd")
)


sales_df = sales_df.join(
    exchange_df,
    (sales_df.currency_cod == exchange_df.currency) &
    (sales_df.order_date == exchange_df.date),
    "left"
)

sales_df = sales_df.withColumn(
    "revenue_converted",
    col("revenue_usd") * col("exchange")
)

sales_df.write.format("delta").mode("overwrite").save("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Silver/sales")

customers_df.write.format("delta").mode("overwrite").save("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Silver/customers")

products_df.write.format("delta").mode("overwrite").save("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Silver/products")

stores_df.write.format("delta").mode("overwrite").save("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Silver/stores")

exchange_df.write.format("delta").mode("overwrite").save("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Silver/exchange_rates")