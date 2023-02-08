import argparse
import json

from prefect_gcp import GcpCredentials


def create_gcp_credentials_block(
    credentials: str, block_name: str, overwrite: bool
) -> None:
    with open(credentials) as f:
        creds = json.load(f)
    GcpCredentials(service_account_info=creds).save(block_name, overwrite=overwrite)
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Creates a Google Cloud Platform Credentials block in Prefect."
    )
    parser.add_argument(
        "--credentials", required=True, help="Path to credentials JSON.", type=str
    )
    parser.add_argument(
        "--block_name",
        required=True,
        help="Name to give to created GCP credentials block.",
        type=str,
    )
    parser.add_argument(
        "--overwrite",
        required=False,
        default=False,
        help=(
            "Whether block should be overwritten if "
            "it already exists (default: False)."
        ),
        type=bool,
    )
    args_dict = vars(parser.parse_args())
    create_gcp_credentials_block(**args_dict)
