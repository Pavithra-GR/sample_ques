from pyspark.sql.functions import col, trim, upper, lower, when, coalesce, to_date, try_to_date, regexp_replace, lit
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType

# Sales

df_sales = spark.table("electronic_retailer.01_Bronze.sales_raw")

df_sales_clean = df_sales \
    .withColumn("order_date", 
                coalesce(try_to_date(col("order_date"), "dd-MM-yyyy"),
                        try_to_date(col("order_date"), "M/d/yyyy"))) \
    .withColumn("delivery_date", 
                coalesce(try_to_date(col("delivery_date"), "M/d/yyyy"),
                        try_to_date(col("delivery_date"), "dd-MM-yyyy"))) \
    .withColumn("quantity", col("quantity").cast(IntegerType())) \
    .withColumn("currency_code", trim(upper(col("currency_code")))) \
    .dropDuplicates(["order_number", "line_item"]) \
    .filter(col("order_number").isNotNull() & 
            col("line_item").isNotNull() & 
            col("customer_key").isNotNull() &
            col("store_key").isNotNull())

df_sales_clean.write.mode("overwrite").saveAsTable("electronic_retailer.02_Silver.sales_clean")

# customer

df_customers = spark.table("electronic_retailer.01_Bronze.customers_raw")

df_customers_clean = df_customers \
    .withColumn("gender", trim(upper(col("gender")))) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("city", trim(col("city"))) \
    .withColumn("state_code", trim(upper(col("state_code")))) \
    .withColumn("state", when(col("state").isNull(), "Unknown").otherwise(trim(col("state")))) \
    .withColumn("zip_code", when(col("zip_code").isNull(), "Unknown").otherwise(trim(col("zip_code")))) \
    .withColumn("country", trim(col("country"))) \
    .withColumn("continent", trim(col("continent"))) \
    .withColumn("birthday", 
                coalesce(try_to_date(col("birthday"), "dd-MM-yyyy"),
                        try_to_date(col("birthday"), "M/d/yyyy"))) \
    .dropDuplicates(["customer_key"]) \
    .filter(col("customer_key").isNotNull())

df_customers_clean.write.mode("overwrite").saveAsTable("electronic_retailer.02_Silver.customers_clean")


# product

df_products = spark.table("electronic_retailer.01_Bronze.products_raw")

df_products_clean = df_products \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("brand", trim(col("brand"))) \
    .withColumn("color", trim(col("color"))) \
    .withColumn("unit_cost_usd", 
                regexp_replace(col("unit_cost_usd"), "[\\$,\\s]", "").cast(DoubleType())) \
    .withColumn("unit_price_usd", 
                regexp_replace(col("unit_price_usd"), "[\\$,\\s]", "").cast(DoubleType())) \
    .withColumn("subcategory", trim(col("subcategory"))) \
    .withColumn("category", trim(col("category"))) \
    .dropDuplicates(["product_key"]) \
    .filter(col("product_key").isNotNull())

df_products_clean.write.mode("overwrite").saveAsTable("electronic_retailer.02_Silver.products_clean")

# stores

df_stores = spark.table("electronic_retailer.01_Bronze.stores_raw")

df_stores_clean = df_stores \
    .withColumn("country", trim(col("country"))) \
    .withColumn("state", trim(col("state"))) \
    .withColumn("square_meters", 
                coalesce(col("square_meters").cast(DoubleType()), lit(0.0))) \
    .withColumn("open_date", 
                coalesce(try_to_date(col("open_date"), "dd-MM-yyyy"),
                        try_to_date(col("open_date"), "M/d/yyyy"))) \
    .dropDuplicates(["store_key"]) \
    .filter(col("store_key").isNotNull())

df_stores_clean.write.mode("overwrite").saveAsTable("electronic_retailer.02_Silver.stores_clean")


# exchange rate
df_exchange = spark.table("electronic_retailer.01_Bronze.exchange_rates_raw")

df_exchange_clean = df_exchange \
    .withColumn("date", 
                coalesce(try_to_date(col("date"), "dd-MM-yyyy"),
                        try_to_date(col("date"), "M/d/yyyy"))) \
    .withColumn("currency", trim(upper(col("currency")))) \
    .dropDuplicates(["date", "currency"]) \
    .filter(col("date").isNotNull() & col("currency").isNotNull())

df_exchange_clean.write.mode("overwrite").saveAsTable("electronic_retailer.02_Silver.exchange_rates_clean")



Data Quality Checks

1. Columns should be lowercase with spaces are replaced by underscore
2. Check if datatype are changed properly
3. Check if nulls/blanks are handled properly
