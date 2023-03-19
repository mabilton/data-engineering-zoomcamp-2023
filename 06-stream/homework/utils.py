import gzip
import os
import shutil
from typing import Optional

import pandas as pd
import wget
import yaml

KAFKA_SETTINGS_PATH = "kafka_settings.yml"


def load_kafka_settings() -> dict[str, str]:
    with open(KAFKA_SETTINGS_PATH) as f:
        settings = yaml.safe_load(f)
    return settings


def download_csv(url: str, save_dir: Optional[str] = None) -> str:
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


def print_csv_values_count(url: str, column: str) -> None:
    path = download_csv(url)
    df = pd.read_csv(path)
    print(df[column].value_counts())
    return None
