import argparse
import os
from pathlib import Path
from typing import Sequence
from urllib.error import HTTPError

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

DATA_SAVE_DIR = "data"


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task()
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues and column name issues"""
    df = df.rename(columns={col: col.lower() for col in df})
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
    df["pulocationid"] = df["pulocationid"].astype("Int64")
    df["dolocationid"] = df["dolocationid"].astype("Int64")
    return df


@task()
def write_local(df: pd.DataFrame, file_name: str, to_parquet: bool) -> Path:
    """Write DataFrame out locally as a CSV (if `to_parquet` is `False`)
    or as a Parquet file (if `to_parquet` is `True`)"""
    if not os.path.isdir(DATA_SAVE_DIR):
        os.makedirs(DATA_SAVE_DIR)
    if not to_parquet:
        path = Path(os.path.join(DATA_SAVE_DIR, f"{file_name}.csv.gz"))
        df.to_csv(path, compression="gzip", index=False)
    else:
        path = Path(os.path.join(DATA_SAVE_DIR, f"{file_name}.parquet"))
        df.to_parquet(path, compression="gzip", index=False)
    return path


@task()
def write_gcs(bucket_block: str, path: Path) -> None:
    """Upload local parquet file to GCS"""
    GcsBucket.load(bucket_block).upload_from_path(from_path=path, to_path=path)
    return None


@flow(log_prints=True)
def fhv_etl_web_to_gcs(
    year: int, months: Sequence[int], bucket_block: str, to_parquet: bool
) -> None:
    """The main ETL function"""
    if not isinstance(months, Sequence):
        months = [months]
    for month in months:
        file_name = f"fhv_tripdata_{year}-{month:02d}"
        dataset_url = (
            "https://github.com/DataTalksClub/"
            f"nyc-tlc-data/releases/download/fhv/{file_name}.csv.gz"
        )
        try:
            df = fetch(dataset_url)
        except HTTPError:
            break
        df_clean = clean(df)
        path = write_local(df_clean, file_name, to_parquet)
        write_gcs(bucket_block, path)
        month += 1
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Prefect flow that pulls NY Taxi data "
            "from Github and uploads it to a GCS Bucket."
        )
    )
    parser.add_argument(
        "--year", required=True, help="Year of data to pull and upload.", type=int
    )
    parser.add_argument(
        "--months",
        required=False,
        default=list(range(1, 13)),
        help=(
            "Month(s) of data to pull and upload; more than "
            "one month can be specified."
        ),
        type=lambda x: [int(val) for val in x.strip("[]").split(",")]
        if isinstance(x, str)
        else x,
    )
    parser.add_argument(
        "--bucket_block",
        required=False,
        default="gcs-bucket-zoomcamp",
        help=("Name of GCS Bucket Prefect block (default: 'gcs-bucket-zoomcamp')."),
        type=str,
    )
    parser.add_argument(
        "--parquet",
        required=False,
        default=False,
        help=(
            "Whether FHV data should be uploaded as Parquet files; "
            "if False, FHV data is uploaded as Gzipped CSVs "
            "(default: False)."
        ),
        type=bool,
    )
    args_dict = vars(parser.parse_args())
    args_dict["to_parquet"] = args_dict.pop("parquet")
    fhv_etl_web_to_gcs(**args_dict)
