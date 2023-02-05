import argparse
import os
from pathlib import Path
from typing import Sequence, Union

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    try:
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    except KeyError:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(save_dir: str, df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    save_dir = os.path.join(save_dir, color)
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    path = Path(os.path.join(save_dir, f"{dataset_file}.parquet"))
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(gcs_block: str, path: Path) -> None:
    """Upload local parquet file to GCS"""
    GcsBucket.load(gcs_block).upload_from_path(from_path=path, to_path=path)
    return None


@flow(log_prints=True)
def etl_web_to_gcs(
    color: str,
    year: int,
    months: Union[int, Sequence[int]],
    save_dir: str,
    gcs_block: str,
) -> None:
    """The main ETL function"""
    if not isinstance(months, Sequence):
        months = [months]
    num_rows_processed = 0
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = (
            "https://github.com/DataTalksClub/nyc-tlc-data/releases/"
            f"download/{color}/{dataset_file}.csv.gz"
        )
        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(save_dir, df_clean, color, dataset_file)
        write_gcs(gcs_block, path)
        num_rows_processed += len(df)
    print(f"Total number of rows processed = {num_rows_processed}")
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Prefect flow that pulls NY Taxi data "
            "from Github and uploads it to a GCS Bucket."
        )
    )
    parser.add_argument(
        "--color",
        required=True,
        help="Color of Taxi data to pull and upload.",
        type=str,
    )
    parser.add_argument(
        "--year", required=True, help="Year of data to pull and upload.", type=int
    )
    parser.add_argument(
        "--months",
        required=True,
        help=(
            "Month(s) of data to pull and upload; more than "
            "one month can be specified."
        ),
        type=lambda x: [int(val) for val in x.strip("[]").split(",")]
        if isinstance(x, str)
        else x,
    )
    parser.add_argument(
        "--save_dir",
        required=True,
        help=(
            "Local directory to save pulled data to; if it doesn't "
            "exist, directory is created."
        ),
        type=str,
    )
    parser.add_argument(
        "--gcs_block",
        required=True,
        help=("Name of GCS Bucket Prefect block."),
        type=str,
    )
    args_dict = vars(parser.parse_args())
    etl_web_to_gcs(**args_dict)
