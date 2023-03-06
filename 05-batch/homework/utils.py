import os
from typing import Mapping, Optional

import wget

"""
Utility functions called by the `02-answers.ipynb` Jupyter notebook.
"""


def download_csv(url: str, save_dir: Optional[str] = None) -> str:
    """
    Downloads a CSV to the `save_dir` directory from a specified `url`.

    The name of the CSV file is automatically inferred from the `url`.

    If the CSV file already exists in `save_dir`, it will be redownloaded
    and overwritten; if redownloading occurs, a warning will be issued to
    the user.

    Inputs
    ------
    url
        Url of the CSV file to download
    save_dir
        Directory the CSV file should be downloaded to. If not specified,
        the CSV will be downloaded to the current working directory.

    Returns
    -------
    csv_path
        The path to the downloaded CSV file.
    """
    if save_dir is None:
        save_dir = os.getcwd()
    filename = os.path.join(save_dir, os.path.basename(url))
    if os.path.exists(filename):
        print(f"'{filename}' already exists, but will be redownloaded.")
        os.remove(filename)
    return wget.download(url, out=filename)


def get_parquet_file_sizes(dir_to_search: str) -> Mapping[str, float]:
    """
    Returns dictionary storing the file size (in MB) of all the Parquet
    files (i.e. files ending with a `.parquet` extension) contained in
    the `dir_to_search` directory.

    The keys of the returned dictionary are the file names of the Parquet
    files found in `dir_to_search`.

    Inputs
    ------
    dir_to_search
        Directory to search for Parquet files (i.e. files ending with a
        `.parquet` extension).

    Returns
    -------
    file_sizes
        Dictionary containing file sizes (in MB) of all the Parquet files
        found in `dir_to_search`.
    """
    parquet_sizes = {}
    for file in os.listdir(dir_to_search):
        if file.endswith(".parquet"):
            path = os.path.join(dir_to_search, file)
            size_in_bytes = os.stat(path).st_size
            size_in_mb = size_in_bytes / (1024**2)
            parquet_sizes[file] = size_in_mb
    return parquet_sizes
