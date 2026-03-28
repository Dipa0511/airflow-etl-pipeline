import pandas as pd
import logging
from sqlalchemy import create_engine, text

def load():
    try:
        logging.info("Load started")

        df = pd.read_csv("/opt/airflow/data/processed/sales_clean.csv")

        engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")

        with engine.begin() as conn:

            # Create table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sales_data (
                    order_id BIGINT PRIMARY KEY,
                    product TEXT,
                    price BIGINT,
                    qty BIGINT,
                    total_amount BIGINT
                )
            """))

            # 🔥 INSERT DATA (THIS WAS MISSING)
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT INTO sales_data (order_id, product, price, qty, total_amount)
                    VALUES (:order_id, :product, :price, :qty, :total_amount)
                    ON CONFLICT (order_id) DO NOTHING
                """), row.to_dict())

        logging.info("✅ Data loaded successfully")

    except Exception as e:
        logging.error(f"Load failed: {e}")
        raise