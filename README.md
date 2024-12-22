# Problem - 3

For clarity and organization, my commits are stored in a separate repository. If you need access, please email me at hadysadegh@gmail.com, and I will grant you access.

## Running the Solution

To execute the solution, follow these steps:

0. Install Prerequisites
1. Clone this repository.
2. Open the Terminal in the same directory.
3. Run the following command:

   Suggestion: for files larger than 1GB num_proc >= file_size MB / 128 MB

   ```bash
   python3 splitWithSpark.py --id_col '"filename"' --max_rows 3 --header True --num_proc 5 --input_file shared/sample-data/sample.tsv --output_dir shared/sample-output
   ```

### Explanation of Parameters

- `--id_col` : Specify the name of the column to use for splitting the data (string wrap it in '').
- `--max_rows` : Set the maximum number of rows allowed in each output file (integer).
- `--header` : Choose whether to include headers in the output files (`True`/`False`).
- `--num_proc` : Define the number of parallel processes for Spark (number of repartitions). Adjust this based on the data size and system capability.
- `--input_file` : Provide the path to the input file.
- `--output_dir` : Specify the path to the output directory.

## Prerequisites

- **PySpark**
- **Python**
