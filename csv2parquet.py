import argparse
import os
import pathlib

import pandas as pd
import pyarrow


def path_to(*path_parts):
    """
    Compiles subsequent directories and file name in one path leading to the last element.
    :param path_parts: name(s) of folder(s) and/or file
    :type path_parts: List[str]
    :return: joined path to object in file system
    :rtype: str
    """
    return os.path.join(os.path.dirname(__file__), *path_parts)


def collect_args():
    """
    Collects input and output folder as arguments from command line and returns paths to these folders
    :return: Path to input folder, path to output folder
    :rtype: Tuple[str]
    """
    parser = argparse.ArgumentParser(description="CSV to Parquet bulk converter")
    parser.add_argument("input_dir", type=str, help="Input directory with CSV files")
    parser.add_argument("output_dir", type=str, help="Output directory for Parquet files")
    args = parser.parse_args()
    return path_to(args.input_dir), path_to(args.output_dir)


def main(input_dir, output_dir):
    """
    Converts csv files from input folder into Parquet format and saves results to output folder
    :param input_dir: folder with csv files
    :param output_dir: folder for saving Parquet files
    :return: None
    """
    pathlib.Path(output_dir).mkdir(exist_ok=True)
    try:
        csv_files = [file_ for file_ in os.listdir(input_dir) if file_.endswith(".csv")]
    except FileNotFoundError as err:
        raise FileNotFoundError(f"Directory {input_dir} does not exist") from err

    for file_ in csv_files:
        df = pd.read_csv(path_to(input_dir, file_))
        new_filename = file_.split(".")[0] + ".parquet"
        df.to_parquet(path_to(output_dir, new_filename))


if __name__ == "__main__":
    main(*collect_args())
