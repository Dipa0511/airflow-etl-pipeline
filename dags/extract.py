
import json
import os
import logging

def extract():
    try:
        logging.info("Extract started")

        data = [
            {"order_id": 1, "product": "Laptop", "price": 50000, "qty": 1},
            {"order_id": 2, "product": "Mouse", "price": 500, "qty": 2},
            {"order_id": 3, "product": "Keyboard", "price": 1500, "qty": 1},
            {"order_id": 4, "product": "Monitor", "price": 2000, "qty": 1}
        ]

        os.makedirs("/opt/airflow/data/raw", exist_ok=True)

        file_path = "/opt/airflow/data/raw/sales.json"

        with open(file_path, "w") as f:
            json.dump(data, f)

        logging.info(f"Data written to {file_path}")

    except Exception as e:
        logging.error(f" Extract failed: {e}")
        raise