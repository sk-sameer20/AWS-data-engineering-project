import os
import sys

# Windows Hadoop configuration
os.environ["HADOOP_HOME"] = r"C:\Users\Sameer Khan\hadoop"
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

# ----------------------------------------
# AWS Configuration
# ----------------------------------------

CUSTOMERS_BUCKET = "samreer-bronze-customers-s3"
ORDERS_BUCKET    = "samreer-bronze-orders-s3"
PRODUCTS_BUCKET  = "samreer-bronze-product-s3"


spark = (
    SparkSession.builder
    .appName("AWS-DE-Project")
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
# Read Customers Bronze Data
# ----------------------------------------

customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{CUSTOMERS_BUCKET}/customers.csv")
)

customers_df.show()
customers_df.printSchema()
print(customers_df.columns)


from pyspark.sql.functions import col, trim

# ----------------------------------------
# Bronze -> Silver Transformation
# ----------------------------------------

customers_silver = (
    customers_df
    .dropDuplicates(["customer_id"])
    .filter(col("customer_id").isNotNull())
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
)

print("Customers Silver Data")
customers_silver.show()


# ----------------------------------------
# Write Silver Layer
# ----------------------------------------

customers_silver.write \
    .mode("overwrite") \
    .parquet(
        f"s3a://{CUSTOMERS_BUCKET}/silver/customers/"
    )

print("Customers Silver layer created successfully!")


silver_df = spark.read.parquet(
    f"s3a://{CUSTOMERS_BUCKET}/silver/customers/"
)

print("Reading Silver Data")
silver_df.show()


# ----------------------------------------
# Read Products Bronze Data
# ----------------------------------------

products_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{PRODUCTS_BUCKET}/products.csv")
)

print("Products Bronze Data")
products_df.show()
products_df.printSchema()
print(products_df.columns)



from pyspark.sql.functions import col, trim

# ----------------------------------------
# Bronze -> Silver Transformation (Products)
# ----------------------------------------

products_silver = (
    products_df
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
    .filter(col("price") > 0)
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
)

print("Products Silver Data")
products_silver.show()


# ----------------------------------------
# Write Products Silver Layer
# ----------------------------------------

products_silver.write \
    .mode("overwrite") \
    .parquet(
        f"s3a://{PRODUCTS_BUCKET}/silver/products/"
    )

print("Products Silver layer created successfully!")


products_silver_df = spark.read.parquet(
    f"s3a://{PRODUCTS_BUCKET}/silver/products/"
)

print("Reading Products Silver Data")
products_silver_df.show()


# ----------------------------------------
# Read Orders Bronze Data
# ----------------------------------------

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{ORDERS_BUCKET}/orders.csv")
)

print("Orders Bronze Data")
orders_df.show()
orders_df.printSchema()
print(orders_df.columns)


from pyspark.sql.functions import col

# ----------------------------------------
# Bronze -> Silver Transformation (Orders)
# ----------------------------------------

orders_silver = (
    orders_df
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("quantity") > 0)
)

print("Orders Silver Data")
orders_silver.show()


# ----------------------------------------
# Write Orders Silver Layer
# ----------------------------------------

orders_silver.write \
    .mode("overwrite") \
    .parquet(
        f"s3a://{ORDERS_BUCKET}/silver/orders/"
    )

print("Orders Silver layer created successfully!")


orders_silver_df = spark.read.parquet(
    f"s3a://{ORDERS_BUCKET}/silver/orders/"
)

print("Reading Orders Silver Data")
orders_silver_df.show()