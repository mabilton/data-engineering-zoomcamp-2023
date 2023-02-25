import argparse
from typing import Sequence

from google.cloud import bigquery
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task()
def create_bq_client(credentials: GcpCredentials) -> bigquery.Client:
    return credentials.get_bigquery_client()


@task()
def check_taxi_value(taxi: str) -> None:
    if taxi not in ("fhv", "yellow", "green"):
        raise ValueError(
            f"{taxi} is not a legitimate `taxi` value; "
            "must select one of: 'fhv', 'green', or 'yellow'."
        )
    return None


@task()
def create_table_name(project_id: str, taxi: str, bq_db: str) -> str:
    return f"{project_id}.{bq_db}.raw_{taxi}_tripdata"


@task()
def create_external_table(
    bq_client: bigquery.Client, table_name: str, bucket_name: str, taxi: str
) -> None:
    creation_operation = f"""
    CREATE OR REPLACE EXTERNAL TABLE {table_name} 
    OPTIONS (
    format = 'PARQUET',
    uris = ['gs://{bucket_name}/{taxi}_tripdata/{taxi}_tripdata_*.parquet']
    );
    """
    job = bq_client.query(creation_operation)
    job.result()
    return None


@flow(log_prints=True)
def etl_gcs_to_bq(
    taxis: Sequence[str], cred_block: str, gcs_bucket: str, bq_db: str
) -> None:
    """The main ETL function"""
    creds = GcpCredentials.load(cred_block)
    bq_client = create_bq_client(creds)
    project_id = creds.project
    bucket_name = GcsBucket.load(gcs_bucket).bucket
    for taxi in taxis:
        check_taxi_value(taxi)
        taxi = taxi.lower()
        table_name = create_table_name(project_id, taxi, bq_db)
        create_external_table(bq_client, table_name, bucket_name, taxi)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Prefect flow that creates an external table from the "
            "Parquet data pushed to a GCS bucket by the main Prefect "
            "flow in `etl_web_to_gcs.py`."
        )
    )
    parser.add_argument(
        "--taxis",
        required=False,
        default=["fhv", "yellow", "green"],
        help=(
            "Name of taxi(s) to create an external BigQuery table "
            "for; valid values are 'fhv', 'yellow' and 'green'."
        ),
        type=lambda x: [x.lower()] if isinstance(x, str) else x,
    )
    parser.add_argument(
        "--cred_block",
        required=False,
        default="gcp-cred-zoomcamp",
        help=(
            "Name of GCP Credentials Prefect block " "(default: 'gcp-cred-zoomcamp')."
        ),
        type=str,
    )
    parser.add_argument(
        "--gcs_bucket",
        required=False,
        default="gcs-bucket-zoomcamp",
        help=("Name of GCS Bucket Prefect block " "(default: 'gcs-bucket-zoomcamp')."),
        type=str,
    )
    parser.add_argument(
        "--bq_db",
        required=False,
        default="taxi_data",
        help=(
            "Name of BigQuery database to create " "tables for (default: 'taxi_data')."
        ),
        type=str,
    )

    args_dict = vars(parser.parse_args())
    etl_gcs_to_bq(**args_dict)
