import os
from pathlib import Path
from typing import Sequence

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
def write_local(data_dir: str, df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    save_dir = os.path.join(data_dir, color)
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    path = Path(os.path.join(save_dir, f"{dataset_file}.parquet"))
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(gcs_bucket_block_name: str, path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(
    color: str = "green",
    year: int = 2020,
    months: Sequence[int] = (1,),
    data_dir: str = "data",
    gcs_bucket_block_name: str = "gcs-zoomcamp",
) -> None:
    """The main ETL function"""
    if not isinstance(months, Sequence):
        months = [months]
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = (
            "https://github.com/DataTalksClub/nyc-tlc-data/releases/"
            f"download/{color}/{dataset_file}.csv.gz"
        )
        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(data_dir, df_clean, color, dataset_file)
        write_gcs(gcs_bucket_block_name, path)


if __name__ == "__main__":
    etl_web_to_gcs()
