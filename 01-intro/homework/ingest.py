#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
from typing import Any
from dateutil.parser import parse as datetime_parse
from sqlalchemy import create_engine


def main(
    user: str,
    password: str,
    host: str,
    port: str,
    db: str,
    table_name: str,
    url: str,
    chunksize: int,
    verbose: bool,
) -> None:
    """
    A minimalist script that ; please run `python ingest.py -h` or `python3 ingest.py -h` for further details.
    """
    # Connect to postgres database:
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    # CSV file name is the last part of the url, after the final '/':
    csv_name = url.split("/")[-1]
    os.system(f"wget {url} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=chunksize)
    for i, df in enumerate(df_iter):
        # For first chunk, take note of which columns contain datetime strings:
        if i == 0:
            datetime_cols = [col for col in df if is_datetime_str(df[col][0])]
        # Convert datetime string values into datetimes:
        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col])
        # If table already exists in database, replace it with column names:
        if i == 0:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        # Append values to table:
        df.to_sql(name=table_name, con=engine, if_exists="append", method="multi")
        if verbose:
            print(f"Chunk {i+1} ingested into database.")
    return None


def is_datetime_str(val: Any):
    """
    Returns `True` if `val` is a `str` that can be parsed as a datetime;
    otherwise, returns `False`.
    """
    try:
        datetime_parse(val, fuzzy=False)
        is_dt_str = True
    except (ValueError, TypeError):
        is_dt_str = False
    return is_dt_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Minimal script to ingest CSV data into a Postgres database."""
    )

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file to ingest")
    parser.add_argument(
        "--chunksize",
        required=False,
        default=100_000,
        help="number of rows to ingest at a time (default: 100_000)",
    )
    parser.add_argument(
        "--verbose",
        required=False,
        default=False,
        help="whether progress should be printed (default: False)",
    )

    args_dict = vars(parser.parse_args())
    # Ensure `chunksize` is an `int`:
    args_dict["chunksize"] = int(args_dict["chunksize"])

    main(**args_dict)
