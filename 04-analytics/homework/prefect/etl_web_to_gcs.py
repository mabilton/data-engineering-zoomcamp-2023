import argparse
import os
from itertools import product
from pathlib import Path
from typing import Sequence
from urllib.error import HTTPError

import pandas as pd
import yaml
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task()
def create_path(file_name, taxi):
    save_dir = f"{taxi}_tripdata"
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    path = Path(os.path.join(save_dir, f"{file_name}.parquet"))
    return path


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


with open("col_dtypes.yml") as stream:
    COL_DTYPES = yaml.safe_load(stream)


@task()
def clean(df: pd.DataFrame, taxi: str) -> pd.DataFrame:
    """Fix column name issues"""
    df = df.rename(columns={col: col.lower() for col in df})
    for col_name, col_dtype in COL_DTYPES[taxi].items():
        if col_dtype == "datetime":
            df[col_name] = pd.to_datetime(df[col_name])
        else:
            df[col_name] = df[col_name].astype(col_dtype)
    return df


@task()
def write_local(df: pd.DataFrame, path: Path) -> Path:
    """Write DataFrame out locally as a CSV (if `to_parquet` is `False`)
    or as a Parquet file (if `to_parquet` is `True`)"""
    df.to_parquet(path, compression="gzip", index=False)
    return None


@task()
def write_gcs(bucket_block: str, path: Path) -> None:
    """Upload local parquet file to GCS"""
    GcsBucket.load(bucket_block).upload_from_path(from_path=path, to_path=path)
    return None


@flow(log_prints=True)
def etl_web_to_gcs(
    taxi: str,
    years: Sequence[int],
    months: Sequence[int],
    bucket_block: str,
    redownload: bool,
) -> None:
    """The main ETL function"""
    for month, year in product(months, years):
        file_name = f"{taxi}_tripdata_{year}-{month:02d}"
        dataset_url = (
            "https://github.com/DataTalksClub/"
            f"nyc-tlc-data/releases/download/{taxi}/{file_name}.csv.gz"
        )
        path = create_path(file_name, taxi)
        if (not redownload) and os.path.isfile(path):
            continue
        try:
            df = fetch(dataset_url)
        except HTTPError:
            continue
        df = clean(df, taxi)
        write_local(df, path)
        write_gcs(bucket_block, path)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Prefect flow that pulls NY Taxi data "
            "from Github and uploads it to a GCS Bucket."
        )
    )
    parser.add_argument(
        "--taxi",
        required=True,
        help=(
            "Name of taxi data to upload; valid values "
            "are 'fhv', 'yellow' and 'green'."
        ),
        type=lambda x: x.lower() if isinstance(x, str) else x,
        choices=["fhv", "yellow", "green"],
    )
    parser.add_argument(
        "--years",
        required=True,
        help=("Year(s) of data to upload; more than " "one year can be specified."),
        type=lambda x: [int(val) for val in x.strip("[]").split(",")]
        if isinstance(x, str)
        else x,
    )
    parser.add_argument(
        "--months",
        required=False,
        default=list(range(1, 13)),
        help=("Month(s) of data to upload; more than " "one month can be specified."),
        type=lambda x: [int(val) for val in x.strip("[]").split(",")]
        if isinstance(x, str)
        else x,
    )
    parser.add_argument(
        "--bucket_block",
        required=False,
        default="gcs-bucket-zoomcamp",
        help=(
            "Name to give to created GCS Bucket block "
            "(default: 'gcs-bucket-zoomcamp')."
        ),
        type=str,
    )
    parser.add_argument(
        "--redownload",
        required=False,
        default=False,
        help=(
            "Whether already downloaded files should be "
            "re-downloaded and re-uploaded (default: False)."
        ),
        type=lambda x: (x.lower() == "true") if isinstance(x, str) else x,
    )
    args_dict = vars(parser.parse_args())
    etl_web_to_gcs(**args_dict)
