
import pandas as pd
import json
import os
import logging

def transform():
    try:
        logging.info("Transform started")

        with open("/opt/airflow/data/raw/sales.json") as f:
            data = json.load(f)

        df = pd.DataFrame(data)

        # Create new column
        df["total_amount"] = df["price"] * df["qty"]

        # Remove duplicates (idempotency)
        df.drop_duplicates(inplace=True)

        # Filter high value orders
        df = df[df["total_amount"] > 1000]

        os.makedirs("/opt/airflow/data/processed", exist_ok=True)

        output_path = "/opt/airflow/data/processed/sales_clean.csv"
        df.to_csv(output_path, index=False)

        logging.info(f" Transformed data saved to {output_path}")

    except Exception as e:
        logging.error(f" Transform failed: {e}")
        raise