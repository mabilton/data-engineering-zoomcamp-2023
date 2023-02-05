import argparse

from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


def create_gcs_bucket_block(bucket: str, cred_block: str, block_name: str) -> None:
    gcp_credentials = GcpCredentials.load(cred_block)
    gcs_bucket_block = GcsBucket(bucket=bucket, gcp_credentials=gcp_credentials)
    gcs_bucket_block.save(block_name)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Creates a Google Cloud Storage Bucket block in Prefect."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Name of the GCS bucket to connect to.",
        type=str,
    )
    parser.add_argument(
        "--cred_block",
        required=True,
        help="Name of GCP Credentials Prefect block.",
        type=str,
    )
    parser.add_argument(
        "--block_name",
        required=True,
        help="Name of give to create GCS Bucket block.",
        type=str,
    )
    args_dict = vars(parser.parse_args())
    create_gcs_bucket_block(**args_dict)
