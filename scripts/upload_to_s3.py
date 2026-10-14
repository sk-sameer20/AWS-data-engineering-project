import boto3
from pathlib import Path 


# ----------------------------------------
# Project Paths
# ----------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"


# ----------------------------------------
# AWS S3 Configuration
# ----------------------------------------

CUSTOMERS_BUCKET = "samreer-bronze-customers-s3"
ORDERS_BUCKET    = "samreer-bronze-orders-s3"
PRODUCTS_BUCKET  = "samreer-bronze-product-s3"

s3 = boto3.client("s3")


# ----------------------------------------
# Upload File to Amazon S3
# ----------------------------------------

def upload_file(file_name, bucket_name, s3_key):

    local_file = RAW_DATA_PATH / file_name

    s3.upload_file(
        str(local_file),
        bucket_name,
        s3_key
    )

    print(f"✅ {file_name} → s3://{bucket_name}/{s3_key} uploaded successfully!")


# ----------------------------------------
# Main Function
# ----------------------------------------

def main():

    upload_file(
        "customers.csv",
        CUSTOMERS_BUCKET,
        "customers/customers.csv"
    )

    upload_file(
        "products.csv",
        PRODUCTS_BUCKET,
        "products/products.csv"
    )

    upload_file(
        "orders.csv",
        ORDERS_BUCKET,
        "orders/orders.csv"
    )


if __name__ == "__main__":
    main()    