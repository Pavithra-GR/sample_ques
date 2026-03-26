from pyspark.sql.functions import col, when

# SALES
sales_df = sales_df.fillna({"quantity": 0})
sales_df = sales_df.dropna(subset=["customerkey", "productkey"])

sales_df = sales_df.withColumn(
    "sales_channel",
    when(col("storekey").isNull(), "Online").otherwise("In-Store")
)

# CUSTOMERS
customers_df = customers_df.dropna(subset=["customerkey"])

# PRODUCTS
products_df = products_df.dropna(subset=["productkey"])

# STORES
stores_df = stores_df.dropna(subset=["storekey"])

# EXCHANGE (optional)
exchange_df = exchange_df.dropna(subset=["date", "currency"])