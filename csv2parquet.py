import argparse
import os
import pandas as pd
import pathlib
import pyarrow


def path_to(*path_parts):
    return os.path.join(os.path.dirname(__file__), *path_parts)


def collect_args():
    parser = argparse.ArgumentParser(description='CSV to Parquet bulk converter')
    parser.add_argument('input_dir', type=str, help='Input directory with CSV files')
    parser.add_argument('output_dir', type=str, help='Output directory for Parquet files')
    args = parser.parse_args()
    return path_to(args.input_dir), path_to(args.output_dir)


def main(input_dir, output_dir):

    pathlib.Path(output_dir).mkdir(exist_ok=True)
    try:
        csv_files = [file_ for file_ in os.listdir(input_dir) if file_.endswith('.csv')]
    except FileNotFoundError as err:
        raise FileNotFoundError(f'Directory {input_dir} does not exist') from err

    for file_ in csv_files:
        df = pd.read_csv(path_to(input_dir, file_))
        new_filename = file_.split('.')[0] + '.parquet'
        df.to_parquet(path_to(output_dir, new_filename))


if __name__ == '__main__':
    main(*collect_args())
