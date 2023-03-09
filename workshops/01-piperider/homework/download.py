import argparse
import os
from itertools import product

import wget


def download(save_dir: str) -> None:

    for taxi_type in ("yellow", "green", "fhv"):
        taxi_save_dir = os.path.join(save_dir, taxi_type)
        if not os.path.isdir(taxi_save_dir):
            os.makedirs(taxi_save_dir)
    for colour, month, year in product(["green", "yellow"], range(1, 13), [2019, 2020]):
        url = (
            "https://github.com/DataTalksClub/nyc-tlc-data/releases/"
            f"download/{colour}/{colour}_tripdata_{year}-{month:02d}.csv.gz"
        )
        filename = os.path.join(save_dir, colour, os.path.basename(url))
        if os.path.exists(filename):
            os.remove(filename)
        wget.download(url, out=filename)
    for month in range(1, 13):
        url = (
            "https://github.com/DataTalksClub/nyc-tlc-data/releases/"
            f"download/fhv/fhv_tripdata_2019-{month:02d}.csv.gz"
        )
        filename = os.path.join(save_dir, "fhv", os.path.basename(url))
        if os.path.exists(filename):
            os.remove(filename)
        wget.download(url, out=filename)
    # Print empty line since `wget` does not:
    print()
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Downloads NYC TLC Taxi Data for Yellow Taxis in "
            "2019 and 2020, Green Taxis in 2019 and 2020, as well "
            "as for FHV Taxis in 2019."
        )
    )
    parser.add_argument(
        "--save_dir",
        required=False,
        help=("Directory to save downloaded Taxi data to."),
        default="data",
        type=str,
    )
    args_dict = vars(parser.parse_args())
    download(**args_dict)
