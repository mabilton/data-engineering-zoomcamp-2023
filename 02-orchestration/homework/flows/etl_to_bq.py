import argparse
import os
from pathlib import Path
from typing import Sequence, Union

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(
    color: str,
    year: int,
    month: int,
    bucket_block: str,
    gcs_dir: str,
    save_dir: str,
) -> Path:
    """Download trip data from GCS"""
    gcs_subpath = os.path.join(color, f"{color}_tripdata_{year}-{month:02}.parquet")
    gcs_path = f"{gcs_dir}/{gcs_subpath}"
    gcs_block = GcsBucket.load(bucket_block)
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    gcs_block.get_directory(from_path=gcs_path, local_path=save_dir)
    return Path(os.path.join(save_dir, gcs_subpath))


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
    project_id: str,
    cred_block: str,
    chunksize: int,
) -> None:
    """Write DataFrame to BiqQuery"""
    df.to_gbq(
        destination_table=f"{bq_db_name}.{bq_table_name}",
        project_id=project_id,
        credentials=GcpCredentials.load(
            cred_block
        ).get_credentials_from_service_account(),
        chunksize=chunksize,
        if_exists="append",
    )
    return None


@flow(log_prints=True)
def etl_gcs_to_bq(
    color: str,
    year: int,
    months: Union[int, Sequence[int]],
    bucket_block: str,
    gcs_dir: str,
    cred_block: str,
    bq_db_name: str,
    bq_table_name: str,
    project_id: str,
    save_dir: str,
    chunksize: int,
) -> None:
    """Main ETL flow to load data into Big Query"""
    if not isinstance(months, Sequence):
        months = [months]
    num_rows_processed = 0
    for month in months:
        path = extract_from_gcs(color, year, month, bucket_block, gcs_dir, save_dir)
        df = transform(path)
        write_bq(
            df,
            bq_db_name,
            bq_table_name,
            project_id,
            cred_block,
            chunksize,
        )
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
        "--bucket_block",
        required=True,
        help=("Name of GCS Bucket Prefect block to pull Taxi data from."),
        type=str,
    )
    parser.add_argument(
        "--gcs_dir",
        required=True,
        help=("Directory of data within GCS bucket."),
        type=str,
    )
    parser.add_argument(
        "--cred_block",
        required=True,
        help=(
            "Name of GCP Credentials Prefect block to "
            "use to write to BigQuery database."
        ),
        type=str,
    )
    parser.add_argument(
        "--bq_db_name",
        required=True,
        help=("Name of BigQuery database to connect to."),
        type=str,
    )
    parser.add_argument(
        "--bq_table_name",
        required=True,
        help=("Name of table within BigQuery database to connect to."),
        type=str,
    )
    parser.add_argument(
        "--project_id",
        required=True,
        help=("GCP Project ID that hosts BigQuery database."),
        type=str,
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

    args_dict = vars(parser.parse_args())
    print(args_dict["months"])
    etl_gcs_to_bq(**args_dict)