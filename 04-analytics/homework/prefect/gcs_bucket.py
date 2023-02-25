import argparse

from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


def create_gcs_bucket_block(
    bucket: str, cred_block: str, block_name: str, overwrite: bool
) -> None:
    gcp_credentials = GcpCredentials.load(cred_block)
    gcs_bucket_block = GcsBucket(bucket=bucket, gcp_credentials=gcp_credentials)
    gcs_bucket_block.save(block_name, overwrite=overwrite)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Creates a Google Cloud Storage Bucket block in Prefect."
    )
    parser.add_argument(
        "--bucket",
        required=False,
        default="zoomcamp_data_lake",
        help=("Name of the GCS bucket to connect to (default: 'fhv_tripdata')."),
        type=str,
    )
    parser.add_argument(
        "--cred_block",
        required=False,
        default="gcp-cred-zoomcamp",
        help="Name of GCP Credentials Prefect block (default: 'gcp-cred-zoomcamp').",
        type=str,
    )
    parser.add_argument(
        "--block_name",
        required=False,
        default="gcs-bucket-zoomcamp",
        help=(
            "Name of give to create GCS Bucket "
            "block (default: 'gcs-bucket-zoomcamp')."
        ),
        type=str,
    )
    parser.add_argument(
        "--overwrite",
        required=False,
        default=True,
        help=(
            "Whether block should be overwritten if "
            "it already exists (default: True)."
        ),
        type=bool,
    )
    args_dict = vars(parser.parse_args())
    create_gcs_bucket_block(**args_dict)
