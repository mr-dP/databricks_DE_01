# Databricks notebook source
orders = spark.read.parquet("dbfs:/FileStore/tables/silver/orders")

products = spark.read.parquet("dbfs:/FileStore/tables/silver/products")

order_items = spark.read.parquet("dbfs:/FileStore/tables/silver/order_items")

customers = spark.read.parquet("dbfs:/FileStore/tables/silver/customers")

# COMMAND ----------

display(orders)

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

order_details = orders.select(
    "ORDER_ID", to_date("ORDER_TIMESTAMP").alias("DATE"), "CUSTOMER_ID", "STORE_NAME"
)
order_details.display()

# COMMAND ----------

order_items.display()

# COMMAND ----------

order_details = order_details.join(order_items, "order_id", "left").select(
    order_details.ORDER_ID,
    order_details["DATE"],
    order_details.CUSTOMER_ID,
    order_details["STORE_NAME"],
    order_items.UNIT_PRICE,
    order_items["quantity"],
)

# COMMAND ----------

display(order_details)

# COMMAND ----------

order_details = order_details.withColumn(
    "TOTAL_SALES_AMOUNT", order_details["unit_price"] * order_details["QUANTITY"]
)

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = (
    order_details.groupBy("order_id", "date", "customer_id", "store_name")
    .sum("TOTAL_SALES_AMOUNT")
    .withColumnRenamed("sum(TOTAL_SALES_AMOUNT)", "total_order_amount")
)

order_details.display()

# COMMAND ----------

from pyspark.sql.functions import round, col

order_details = order_details.select(
    "order_id", "date", "customer_id", "store_name", round(col("total_order_amount"), 2)
).withColumnRenamed("round(total_order_amount, 2)", "total_order_amount")

# COMMAND ----------

display(order_details)

# COMMAND ----------

order_details.write.mode("OVERWRITE").parquet(
    "dbfs:/FileStore/tables/gold/order_details"
)

# COMMAND ----------

display(order_details)

# COMMAND ----------

from pyspark.sql.functions import date_format

sales_with_month = order_details.withColumn(
    "month_year", date_format("DATE", "yyyy-MM")
)

# COMMAND ----------

sales_with_month.display()

# COMMAND ----------

from pyspark.sql.functions import sum

monthly_sales = (
    sales_with_month.groupBy("month_year")
    .agg(sum("total_order_amount").alias("total_sales"))
    .withColumn("total_sales", round("total_sales", 2))
    .orderBy("month_year", ascending=False)
)

# COMMAND ----------

monthly_sales.display()

# COMMAND ----------

monthly_sales.write.mode("overwrite").parquet(
    "dbfs:/FileStore/tables/gold/monthly_sales"
)

# COMMAND ----------

display(sales_with_month)

# COMMAND ----------

from pyspark.sql.functions import desc

stores_monthly_sales = (
    sales_with_month.groupBy("month_year", "store_name")
    .agg(sum("total_order_amount").alias("total_sales"))
    .sort(desc(col("month_year")))
    .select("month_year", "store_name", round("total_sales", 2))
)

stores_monthly_sales.display()

# COMMAND ----------

stores_monthly_sales.write.parquet(
    "dbfs:/FileStore/tables/gold/stores_monthly_sales", mode="overwrite"
)

# COMMAND ----------


