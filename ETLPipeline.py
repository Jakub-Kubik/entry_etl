import logging

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

    def _transform(self, df: pd.DataFrame, transformation_config: dict) -> pd.DataFrame:
        """Apply transformations to the given DataFrame.

        Args:
            df: DataFrame containing the data.
            transformation_config: Dictionary containing transformation rules.

        Returns:
            DataFrame with transformed data.
        """
        logging.info("Transforming data")
        if transformation_config.get("duplicates_subset"):
            df.drop_duplicates(subset=transformation_config["duplicates_subset"], inplace=True)
        if transformation_config.get("fillna_value"):
            df.fillna(transformation_config["fillna_value"], inplace=True)
        if transformation_config.get("filter_active"):
            df = df[df["isActive"]]
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

    def process(self, files: dict) -> None:
        """Process the specified files using the ETL Pipeline.

        Args:
            files: Dictionary containing table names and corresponding file paths and transformation rules.
        """
        logging.info("Starting the ETL process")
        for table_name, config in files.items():
            logging.info(f"Processing {table_name} from {config['file_path']}")
            raw_data = self._extract(config["file_path"])
            cleaned_data = self._transform(raw_data, config["transformation_config"])
            self._load(cleaned_data, table_name)


files = {
    "contacts": {
        "file_path": "data/contacts-20230414T185305.csv",
        "transformation_config": {
            "duplicates_subset": ["type", "company", "firstName"],
            "fillna_value": "Unknown",
            "filter_active": True,
        },
    },
    "products": {
        "file_path": "data/products-20230414T185305.csv",
        "transformation_config": {"duplicates_subset": ["name", "status"], "fillna_value": "Unknown"},
    },
    "purchase_orders": {
        "file_path": "data/purchase_orders-20230414T185305.csv",
        "transformation_config": {"duplicates_subset": None, "fillna_value": "Unknown"},
    },
    "sale_orders": {
        "file_path": "data/sale_order-20230414T185305.csv",
        "transformation_config": {"duplicates_subset": None, "fillna_value": "Unknown"},
    },
    "stockstream": {
        "file_path": "data/stockstream-20230414T185305.csv",
        "transformation_config": {"duplicates_subset": None, "fillna_value": "Unknown"},
    },
}


pipeline = ETLPipeline()
pipeline.process(files)
