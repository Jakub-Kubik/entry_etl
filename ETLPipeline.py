import logging
from typing import Callable, Dict

import duckdb
import pandas as pd


class ETLPipeline:
    """ETL Pipeline for processing various CSV files."""

    def __init__(self):
        """Initialize the ETL Pipeline with a database connection and logging."""
        self.con = duckdb.connect(database="mydata.db", read_only=False)
        logging.basicConfig(level=logging.INFO)
        logging.info("ETL pipeline initialized.")

    @staticmethod
    def _extract(file_path: str) -> pd.DataFrame:
        logging.info(f"Extracting data from {file_path}.")
        return pd.read_csv(file_path)

    @staticmethod
    def _transform_contacts(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Transforming contacts data.")
        df.drop_duplicates(subset=["type", "company", "firstName"], inplace=True)
        df = df[df["isActive"]]
        df.fillna("Unknown", inplace=True)
        return df

    @staticmethod
    def _transform_products(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Transforming products data.")
        df.drop_duplicates(subset=["name", "status"], inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    @staticmethod
    def _transform_purchase_orders(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Transforming purchase orders data.")
        df.drop_duplicates(inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    @staticmethod
    def _transform_sale_orders(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Transforming sale orders data.")
        df.drop_duplicates(inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    @staticmethod
    def _transform_stockstream(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Transforming stockstream data.")
        df.drop_duplicates(inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    def _table_exists(self, table_name: str) -> bool:
        query = f"SELECT 1 FROM (SELECT * FROM {table_name}) AS _ LIMIT 0"
        try:
            self.con.execute(query)
            return True
        except Exception:
            return False

    def _load(self, df: pd.DataFrame, table_name: str) -> None:
        logging.info(f"Loading data into table {table_name}.")
        if self._table_exists(table_name):
            logging.warning(f"Table {table_name} already exists. Skip table creation. Only override data.")
            self.con.register(f"temp_{table_name}", df)
            self.con.execute(f"DELETE FROM {table_name}")
            self.con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_{table_name}")
            self.con.unregister(f"temp_{table_name}")
            return

        self.con.register(table_name, df)
        self.con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")
        self.con.unregister(table_name)

    def process(self, files: Dict[str, str]) -> None:
        logging.info("Starting the ETL process.")
        for table_name, file_path in files.items():
            logging.info(f"Processing {table_name} from {file_path}.")
            raw_data = self._extract(file_path)
            transform_method: Callable[[pd.DataFrame], pd.DataFrame] = getattr(self, f"_transform_{table_name}")
            cleaned_data = transform_method(raw_data)
            self._load(cleaned_data, table_name)
        logging.info("ETL process completed.")


files = {
    "contacts": "data/contacts-20230414T185305.csv",
    "products": "data/products-20230414T185305.csv",
    "purchase_orders": "data/purchase_orders-20230414T185305.csv",
    "sale_orders": "data/sale_order-20230414T185305.csv",
    "stockstream": "data/stockstream-20230414T185305.csv",
}

pipeline = ETLPipeline()
pipeline.process(files)
