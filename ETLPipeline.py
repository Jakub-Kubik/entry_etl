import logging
from typing import Callable, Dict

import duckdb
import pandas as pd


class ETLPipeline:
    def __init__(self):
        self.con = duckdb.connect(database="mydata.db", read_only=False)
        logging.basicConfig(level=logging.INFO)

    def extract(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path)

    def transform_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop_duplicates(subset=["type", "company", "firstName"], inplace=True)
        # Keep only rows where isActive is True
        df = df[df["isActive"]]
        # Fill missing values with "Unknown"
        df.fillna("Unknown", inplace=True)
        return df

    def transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop_duplicates(subset=["name", "status"], inplace=True)
        # Fill missing values with "Unknown"
        df.fillna("Unknown", inplace=True)
        return df

    def transform_purchase_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop_duplicates(inplace=True)
        # Fill missing values with "Unknown"
        df.fillna("Unknown", inplace=True)
        return df

    def transform_sale_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop_duplicates(inplace=True)
        # Fill missing values with "Unknown"
        df.fillna("Unknown", inplace=True)
        return df

    def transform_stockstream(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop_duplicates(inplace=True)
        # Fill missing values with "Unknown"
        df.fillna("Unknown", inplace=True)
        return df

    def table_exists(self, table_name: str) -> bool:
        query = f"SELECT 1 FROM (SELECT * FROM {table_name}) AS _ LIMIT 0"
        try:
            self.con.execute(query)
            return True
        except Exception:
            return False

    def load(self, df: pd.DataFrame, table_name: str) -> None:
        # Check if the table already exists
        if self.table_exists(table_name):
            logging.warning(f"Table {table_name} already exists. Skip table creation. Only override data.")
            # Register the DataFrame as a temporary table
            self.con.register(f"temp_{table_name}", df)

            # Delete previous content in the table
            self.con.execute(f"DELETE FROM {table_name}")

            # Insert data into the actual table
            self.con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_{table_name}")

            # Unregister the temporary table
            self.con.unregister(f"temp_{table_name}")
            return

        self.con.register(table_name, df)
        self.con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")
        self.con.unregister(table_name)

    def process(self, files: Dict[str, str]) -> None:
        for table_name, file_path in files.items():
            # Extract
            raw_data = self.extract(file_path)

            # Transform
            transform_method: Callable[[pd.DataFrame], pd.DataFrame] = getattr(self, f"transform_{table_name}")
            cleaned_data = transform_method(raw_data)

            # Load
            self.load(cleaned_data, table_name)


files = {
    # no need to process contact_suplier because it is stored in contacts
    "contacts": "data/contacts-20230414T185305.csv",
    "products": "data/products-20230414T185305.csv",
    "purchase_orders": "data/purchase_orders-20230414T185305.csv",
    "sale_orders": "data/sale_order-20230414T185305.csv",
    "stockstream": "data/stockstream-20230414T185305.csv",
}

pipeline = ETLPipeline()
pipeline.process(files)
