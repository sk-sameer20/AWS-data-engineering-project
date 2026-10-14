import os
import sys

# Windows Hadoop configuration
os.environ["HADOOP_HOME"] = r"C:\Users\Sameer Khan\hadoop"
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

CUSTOMERS_BUCKET = "samreer-bronze-customers-s3"
ORDERS_BUCKET    = "samreer-bronze-orders-s3"
PRODUCTS_BUCKET  = "samreer-bronze-product-s3"

spark = (
    SparkSession.builder
    .appName("Incremental-Load")
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
# Read Existing Silver Orders
# ----------------------------------------

silver_orders_df = spark.read.parquet(
    f"s3a://{ORDERS_BUCKET}/silver/orders/"
)

print("Existing Silver Orders")
silver_orders_df.show(5)



# ----------------------------------------
# Read New Orders from Bronze
# ----------------------------------------

new_orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{ORDERS_BUCKET}/orders_new.csv")
)

print("New Orders")
new_orders_df.show(5)



# ----------------------------------------
# Find Only New Orders
# ----------------------------------------

incremental_orders_df = new_orders_df.join(
    silver_orders_df.select("order_id"),
    on="order_id",
    how="left_anti"
)

print("New Incremental Orders")
incremental_orders_df.show()

print("New Records Found:", incremental_orders_df.count())




# ----------------------------------------
# Append New Orders into Silver
# ----------------------------------------

if incremental_orders_df.count() > 0:

    incremental_orders_df.write \
        .mode("append") \
        .parquet(
            f"s3a://{ORDERS_BUCKET}/silver/orders/"
        )

    print("Incremental data loaded successfully")

else:
    print("No new records to load")



updated_silver_orders_df = spark.read.parquet(
    f"s3a://{ORDERS_BUCKET}/silver/orders/"
)

print("Updated Silver Count:")
print(updated_silver_orders_df.count())

updated_silver_orders_df.show()


customer_sales = spark.read.parquet(
    f"s3a://{CUSTOMERS_BUCKET}/gold/customer_sales_summary/"
)

customer_sales.show()


daily_sales = spark.read.parquet(
    f"s3a://{ORDERS_BUCKET}/gold/daily_sales_summary/"
)

daily_sales.show()


product_performance = spark.read.parquet(
    f"s3a://{PRODUCTS_BUCKET}/gold/product_performance/"
)

product_performance.show()