import os
import sys

# Windows Hadoop configuration
os.environ["HADOOP_HOME"] = r"C:\Users\Sameer Khan\hadoop"
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum

# ----------------------------------------
# AWS Configuration
# ----------------------------------------

CUSTOMERS_BUCKET = "samreer-bronze-customers-s3"
ORDERS_BUCKET    = "samreer-bronze-orders-s3"
PRODUCTS_BUCKET  = "samreer-bronze-product-s3"

spark = (
    SparkSession.builder
    .appName("Silver-To-Gold")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .getOrCreate()
)

print("Spark Session Created Successfully")



# ----------------------------------------
# Read Silver Layer
# ----------------------------------------

customers_df = spark.read.parquet(
    f"s3a://{CUSTOMERS_BUCKET}/silver/customers/"
)

orders_df = spark.read.parquet(
    f"s3a://{ORDERS_BUCKET}/silver/orders/"
)

products_df = spark.read.parquet(
    f"s3a://{PRODUCTS_BUCKET}/silver/products/"
)

print("Silver Data Loaded Successfully")


customers_df.show(5)
orders_df.show(5)
products_df.show(5)



# ----------------------------------------
# Join Customers and Orders
# ----------------------------------------

customer_orders = customers_df.join(
    orders_df,
    on="customer_id",
    how="inner"
)

print("Customer + Orders")
customer_orders.show()



# ----------------------------------------
# Join Products
# ----------------------------------------

customer_orders_products = customer_orders.join(
    products_df,
    on="product_id",
    how="inner"
)

print("Customer + Orders + Products")
customer_orders_products.show()


from pyspark.sql.functions import col

# ----------------------------------------
# Calculate Total Sales
# ----------------------------------------

customer_orders_products = customer_orders_products.withColumn(
    "total_sales",
    col("quantity") * col("price")
)

print("Added Total Sales")
customer_orders_products.show()



from pyspark.sql.functions import count, sum

# ----------------------------------------
# Customer Sales Summary
# ----------------------------------------

customer_sales_summary = (
    customer_orders_products
    .groupBy(
        "customer_id",
        "first_name",
        "last_name"
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        sum("total_sales").alias("total_sales")
    )
)

print("Customer Sales Summary")
customer_sales_summary.show()



# ----------------------------------------
# Write Gold Layer
# ----------------------------------------

customer_sales_summary.write \
    .mode("overwrite") \
    .parquet(
        f"s3a://{CUSTOMERS_BUCKET}/gold/customer_sales_summary/"
    )

print("Customer Sales Summary Gold table created successfully!")


gold_df = spark.read.parquet(
    f"s3a://{CUSTOMERS_BUCKET}/gold/customer_sales_summary/"
)

gold_df.show()



# ----------------------------------------
# Product Performance
# ----------------------------------------

product_performance = (
    customer_orders_products
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity_sold"),
        sum("total_sales").alias("total_revenue")
    )
)

print("Product Performance")
product_performance.show()



# ----------------------------------------
# Write Product Performance
# ----------------------------------------

product_performance.write \
    .mode("overwrite") \
    .parquet(
        f"s3a://{PRODUCTS_BUCKET}/gold/product_performance/"
    )

print("Product Performance Gold table created successfully!")



product_gold_df = spark.read.parquet(
    f"s3a://{PRODUCTS_BUCKET}/gold/product_performance/"
)

product_gold_df.show()




# ----------------------------------------
# Daily Sales Summary
# ----------------------------------------

daily_sales_summary = (
    customer_orders_products
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        sum("total_sales").alias("total_sales")
    )
)

print("Daily Sales Summary")
daily_sales_summary.show()




# ----------------------------------------
# Write Daily Sales Summary
# ----------------------------------------

daily_sales_summary.write \
    .mode("overwrite") \
    .parquet(
        f"s3a://{ORDERS_BUCKET}/gold/daily_sales_summary/"
    )

print("Daily Sales Summary Gold table created successfully!")



daily_gold_df = spark.read.parquet(
    f"s3a://{ORDERS_BUCKET}/gold/daily_sales_summary/"
)

daily_gold_df.show()


