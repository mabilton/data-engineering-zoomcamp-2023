import gzip
import os
import shutil
from typing import Optional

import pandas as pd
import wget
import yaml

"""
Miscellaneous utility functions called inside of `02-answers.md` and `03-streaming-app.ipynb`.
"""

KAFKA_SETTINGS_PATH = "kafka_settings.yml"


def load_kafka_settings() -> dict[str, str]:
    f"""
    Loads the '{KAFKA_SETTINGS_PATH}' yaml file, which contains the configuration settings
    of the Kafka Cluster that is started by executing `docker compose up` inside of the
    `06-stream/homework` directory.
    """
    with open(KAFKA_SETTINGS_PATH) as f:
        settings = yaml.safe_load(f)
    return settings


def download_csv(url: str, save_dir: Optional[str] = None) -> str:
    """
    Downloads CSV file located at `url` to `save_dir`; the path to the
    downloaded CSV file is returned.

    The name of the CSV file is inferred from the `url`. If `save_dir` is not
    specified, the CSV is saved to the current directory. If the downloaded CSV
    file is gzipped, it is automatically unzipped. If the CSV file already
    exists in `save_dir`, the CSV file is *not* re-downloaded, and the path
    to this already downloaded file is returned.
    """
    if save_dir is None:
        save_dir = os.getcwd()
    filename = os.path.basename(url)
    save_path = os.path.join(save_dir, filename)
    is_zipped = filename.endswith(".gz")
    unzipped_path = save_path.rstrip(".gz") if is_zipped else save_path
    if (not os.path.exists(save_path)) and (not os.path.exists(unzipped_path)):
        wget.download(url, out=save_path)
    if is_zipped and (not os.path.exists(unzipped_path)):
        with gzip.open(save_path, "r") as f_in, open(unzipped_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(save_path)
    return unzipped_path


def print_csv_cardinality(url: str, column: str) -> None:
    """
    Downloads the CSV file from `url` (if it doesn't already exist in the
    current directory), counts the number of unique values (i.e. the cardinality)
    of a specified `column`, and then prints this count to the user.
    """
    path = download_csv(url)
    df = pd.read_csv(path)
    print("Number of unique values in " f"'{column}' column: {df[column].nunique()}")
    return None
