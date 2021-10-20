import os
from csv2parquet import main, path_to

input_dir, output_dir = path_to('csv_data'), path_to('parquet_data')

def test_output_data_format():
    main(input_dir, output_dir)
    for file_ in os.listdir(output_dir):
        assert file_.endswith('.parquet')
