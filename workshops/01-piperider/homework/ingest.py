import argparse
import os

import duckdb


def ingest(data_dir: str, db_name: str) -> None:
    db_full_name = f"{db_name}.duckdb"
    if os.path.exists(db_full_name):
        os.remove(db_full_name)
    conn = duckdb.connect(db_full_name)
    for taxi_type in ("green", "yellow", "fhv"):
        parquet_file = f"{data_dir}/{taxi_type}/{taxi_type}.parquet"
        # Convert files to Parquet (for speed purposes):
        if not os.path.exists(parquet_file):
            conn.execute(
                f"""
            COPY (
                SELECT * FROM 
                read_csv_auto('{data_dir}/{taxi_type}/*.csv*', header=True) 
            )
            TO '{parquet_file}' (FORMAT 'PARQUET', CODEC 'ZSTD');
            """
            )
        # Create table with ABSOLUTE path, so database works when we move directories:
        parquet_file = os.path.join(os.getcwd(), parquet_file)
        conn.execute(
            f"""
            CREATE VIEW raw_{taxi_type}_tripdata AS
            SELECT * FROM read_parquet('{parquet_file}')
        """
        )
    conn.close()
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Ingests Taxi CSV data downloaded by `download.py` "
            "into a DuckDB database."
        )
    )
    parser.add_argument(
        "--data_dir",
        required=False,
        help=("Directory where downloaded CSVs are stored."),
        default="data",
        type=str,
    )
    parser.add_argument(
        "--db_name",
        required=False,
        help=("Name of DuckDB database file to create."),
        default="taxi",
        type=str,
    )
    args_dict = vars(parser.parse_args())
    ingest(**args_dict)
