from faker import Faker
import pandas as pd
from pathlib import Path
import random

# ----------------------------------------
# Initialize Faker
# ----------------------------------------
fake = Faker("en_IN")

# ----------------------------------------
# Project Paths
# ----------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"

# Create the raw folder if it doesn't exist
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)


# ----------------------------------------
# Generate Customers Dataset
# ----------------------------------------
def generate_customers():

    customers = []

    for customer_id in range(1, 101):

        customer = {
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state(),
            "signup_date": fake.date_between(
                start_date="-2y",
                end_date="today"
            )
        }

        customers.append(customer)

    customers_df = pd.DataFrame(customers)

    customers_df.to_csv(
        RAW_DATA_PATH / "customers.csv",
        index=False
    )

    print("✅ customers.csv created successfully!")


# ----------------------------------------
# Generate Products Dataset
# ----------------------------------------
def generate_products():

    product_names = [
        "Laptop",
        "Mouse",
        "Keyboard",
        "Monitor",
        "Headphones",
        "USB Cable",
        "Tablet",
        "Smartphone",
        "Printer",
        "Webcam"
    ]

    categories = [
        "Electronics",
        "Accessories"
    ]

    products = []

    for product_id in range(1, 11):

        product = {
            "product_id": product_id,
            "product_name": product_names[product_id - 1],
            "category": random.choice(categories),
            "price": random.randint(500, 50000)
        }

        products.append(product)

    products_df = pd.DataFrame(products)

    products_df.to_csv(
        RAW_DATA_PATH / "products.csv",
        index=False
    )

    print("✅ products.csv created successfully!")



# ----------------------------------------
# Generate Orders Dataset
# ----------------------------------------
def generate_orders():

    orders = []

    for order_id in range(1, 501):

        order = {
            "order_id": order_id,
            "customer_id": random.randint(1, 100),
            "product_id": random.randint(1, 10),
            "quantity": random.randint(1, 5),
            "order_date": fake.date_between(
                start_date="-1y",
                end_date="today"
            )
        }

        orders.append(order)

    orders_df = pd.DataFrame(orders)

    orders_df.to_csv(
        RAW_DATA_PATH / "orders.csv",
        index=False
    )

    print("✅ orders.csv created successfully!")    


# ----------------------------------------
# Main Function
# ----------------------------------------
def main():

    generate_customers()
    generate_products()
    generate_orders()


# ----------------------------------------
# Program Entry Point
# ----------------------------------------
if __name__ == "__main__":
    main()



    