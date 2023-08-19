# ETL for Inventoro API Integration

This project aims to create an ETL (Extract, Transform, Load) process that utilizes data from the provided datasets and sends it to the Inventoro API. The goal is to showcase the thought process and approach to implementing this ETL process.

## Getting Started

1. Create an account on [Inventoro](https://app.inventoro.com).
2. Establish an API connection by visiting [API Connection](https://app.inventoro.com/integrations/apiconnection?legacy).
3. Configure the necessary API settings to enable data upload.

## ETL Process Overview

The ETL process involves the following steps:

### Extract

Data is extracted from the provided CSV files using the Pandas library in Python.

### Transform

Several transformations are applied to the extracted data:

- Removal of duplicate records
- Handling of missing values
- Removal of leading/trailing whitespace
- Standardization of date formats

### Load

Cleaned and transformed data is loaded into DuckDB, a high-performance analytical database.

### API Integration

The cleaned data stored in DuckDB is then sent to the Inventoro API using a custom API connection.

## Usage

1. Clone this repository.
2. Install the required Python packages using `pip install -r requirements.txt`.
3. Run the ETL pipeline script `etl_pipeline.py`.
4. Follow the prompts to specify file paths and any customization needed.

## Contribution

Contributions to enhance and extend this ETL process are welcome. Feel free to fork this repository and submit pull requests.

## Contact

For any inquiries, contact `Jan Jakub Kubik` at `jakupkubik@gmail.com`.
