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

    def _extract(self, file_path: str) -> pd.DataFrame:
        """Extract data from the given file path.

        Args:
            file_path: Path to the CSV file.

        Returns:
            DataFrame containing the extracted data.
        """
        logging.info(f"Extracting data from {file_path}")
        return pd.read_csv(file_path)

    def _transform_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Contacts data.

        Args:
            df: DataFrame containing Contacts data.

        Returns:
            DataFrame with transformed data.
        """
        logging.info("Transforming Contacts data")
        df.drop_duplicates(subset=["type", "company", "firstName"], inplace=True)
        df = df[df["isActive"]]
        df.fillna("Unknown", inplace=True)
        return df

    def _transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Products data.

        Args:
            df: DataFrame containing Products data.

        Returns:
            DataFrame with transformed data.
        """
        logging.info("Transforming Products data")
        df.drop_duplicates(subset=["name", "status"], inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    def _transform_purchase_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Purchase Orders data.

        Args:
            df: DataFrame containing Purchase Orders data.

        Returns:
            DataFrame with transformed data.
        """
        logging.info("Transforming Purchase Orders data")
        df.drop_duplicates(inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    def _transform_sale_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Sale Orders data.

        Args:
            df: DataFrame containing Sale Orders data.

        Returns:
            DataFrame with transformed data.
        """
        logging.info("Transforming Sale Orders data")
        df.drop_duplicates(inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    def _transform_stockstream(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Stockstream data.

        Args:
            df: DataFrame containing Stockstream data.

        Returns:
            DataFrame with transformed data.
        """
        logging.info("Transforming Stockstream data")
        df.drop_duplicates(inplace=True)
        df.fillna("Unknown", inplace=True)
        return df

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table_name: Name of the table.

        Returns:
            True if the table exists, False otherwise.
        """
        query = f"SELECT 1 FROM (SELECT * FROM {table_name}) AS _ LIMIT 0"
        try:
            self.con.execute(query)
            return True
        except Exception:
            return False

    def _load(self, df: pd.DataFrame, table_name: str) -> None:
        """Load the transformed data into the specified table.

        Args:
            df: DataFrame containing the data to load.
            table_name: Name of the table to load the data into.
        """
        logging.info(f"Loading data into {table_name}")
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
        """Process the specified files using the ETL Pipeline.

        Args:
            files: Dictionary containing table names and corresponding file paths.
        """
        logging.info("Starting the ETL process")
        for table_name, file_path in files.items():
            logging.info(f"Processing {table_name} from {file_path}")
            raw_data = self._extract(file_path)
            transform_method: Callable[[pd.DataFrame], pd.DataFrame] = getattr(self, f"_transform_{table_name}")
            cleaned_data = transform_method(raw_data)
            self._load(cleaned_data, table_name)


files = {
    "contacts": "data/contacts-20230414T185305.csv",
    "products": "data/products-20230414T185305.csv",
    "purchase_orders": "data/purchase_orders-20230414T185305.csv",
    "sale_orders": "data/sale_order-20230414T185305.csv",
    "stockstream": "data/stockstream-20230414T185305.csv",
}

pipeline = ETLPipeline()
pipeline.process(files)
