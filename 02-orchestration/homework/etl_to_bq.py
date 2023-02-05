import os
from pathlib import Path
from typing import Sequence

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(
    color: str,
    year: int,
    month: int,
    gcs_block_name: str,
    gcs_data_dir: str,
    local_data_dir: str,
) -> Path:
    """Download trip data from GCS"""
    gcs_subpath = os.path.join(color, f"{color}_tripdata_{year}-{month:02}.parquet")
    gcs_path = f"{gcs_data_dir}/{gcs_subpath}"
    gcs_block = GcsBucket.load(gcs_block_name)
    if not os.path.isdir(local_data_dir):
        os.makedirs(local_data_dir)
    gcs_block.get_directory(from_path=gcs_path, local_path=local_data_dir)
    return Path(os.path.join(local_data_dir, gcs_subpath))


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    df["passenger_count"].fillna(0, inplace=True)
    return df


@task()
def write_bq(
    df: pd.DataFrame,
    bq_db_name: str,
    bq_table_name: str,
    gcp_project_id: str,
    gcs_cred_block_name: str,
    chunksize: int,
) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load(gcs_cred_block_name)

    df.to_gbq(
        destination_table=f"{bq_db_name}.{bq_table_name}",
        project_id=gcp_project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=chunksize,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(
    color: str = "yellow",
    year: int = 2021,
    months: Sequence[int] = (1,),
    gcs_bucket_block_name: str = "gcs-zoomcamp",
    gcs_cred_block_name: str = "zoomcamp",
    gcs_data_dir: str = "data",
    bq_db_name: str = "taxi_data",
    bq_table_name: str = "rides",
    gcp_project_id: str = "clear-nebula-375807",
    local_data_dir: str = "data",
    chunksize: int = 100_000,
) -> None:
    """Main ETL flow to load data into Big Query"""
    if not isinstance(months, Sequence):
        months = [months]
    num_rows_processed = 0
    for month in months:
        path = extract_from_gcs(
            color, year, month, gcs_bucket_block_name, gcs_data_dir, local_data_dir
        )
        df = transform(path)
        write_bq(
            df,
            bq_db_name,
            bq_table_name,
            gcp_project_id,
            gcs_cred_block_name,
            chunksize,
        )
        num_rows_processed += len(df)
    print(f"Total number of rows processed = {num_rows_processed}")


if __name__ == "__main__":
    etl_gcs_to_bq()
