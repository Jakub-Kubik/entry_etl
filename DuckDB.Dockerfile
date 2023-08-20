FROM ubuntu:20.04

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    xz-utils

# Download and extract DuckDB CLI
RUN wget -q https://github.com/duckdb/duckdb/releases/download/v0.3.1/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && chmod +x duckdb \
    && mv duckdb /usr/local/bin/

# Set up a volume for data persistence
VOLUME /duckdb_data

# Start the DuckDB CLI
CMD ["duckdb", "/duckdb_data"]
