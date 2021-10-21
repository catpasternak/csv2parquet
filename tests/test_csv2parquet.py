import os
import pytest
from csv2parquet import main, path_to


def test_output_data_format_is_parquet():
    input_dir, output_dir = path_to('csv_data'), path_to('parquet_data')
    main(input_dir, output_dir)
    for file_ in os.listdir(output_dir):
        assert file_.endswith('.parquet')


def test_error_is_raised_when_input_directory_doesnt_exist():
    input_dir, output_dir = path_to('folder_doesnt_exist'), path_to('parquet_data')
    with pytest.raises(FileNotFoundError):
        main(input_dir, output_dir)
