import os
import logging
from faker import Faker
import pandas as pd
import datetime
import random

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

fake = Faker()

def generate_data():
    try:
        print("generate_data function is being called!")
        print(f"Current working directory: {os.getcwd()}")

        output_folder = "/opt/airflow/dummy_transactions"
        if not os.path.exists(output_folder):
            print(f"Creating output directory: {output_folder}")
            os.makedirs(output_folder)

        products = [
            ("Apple", 0.5), ("Banana", 0.3), ("Milk", 2.0), ("Bread", 1.5),
            ("Eggs", 3.0), ("Chicken", 5.0), ("Rice", 4.0), ("Pasta", 2.5),
            ("Cereal", 3.8), ("Juice", 2.2), ("Soda", 1.8), ("Chocolate", 1.2),
            ("Chips", 1.5), ("Coffee", 4.5), ("Tea", 3.0)
        ]

        platforms = ["PostgreSQL", "Google Sheets", "CSV"]

        transaction_data = []
        for _ in range(300):
            invoice_number = fake.unique.random_int(min=10000, max=99999)
            invoice_date = fake.date_between(start_date="-1y", end_date="today")
            customer_name = fake.name()
            product, unit_price = random.choice(products)
            quantity = random.randint(1, 10)
            total_price = round(quantity * unit_price, 2)
            platform = random.choice(platforms)
            last_update = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            transaction_data.append([
                invoice_number, invoice_date, customer_name, 
                product, quantity, unit_price, total_price, platform, last_update
            ])

        df = pd.DataFrame(transaction_data, columns=[
            "Invoice Number", "Invoice Date", "Customer Name", 
            "Product Name", "Quantity", "Unit Price", "Total Amount", "Platform", "Last Update"
        ])

        print(df.head())

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_folder, f"supermarket_transactions_{timestamp}.csv")
        df.to_csv(output_path, index=False)

        print(f"File saved to: {output_path}")

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    generate_data()