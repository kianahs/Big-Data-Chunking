#### Import required libraries

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
from typing import Any
import argparse

# ###### Function chunk_input_file

# Defined a function to chunk the given input file based on the identifier column and writes each chunk to a separate .tsv file.

# #### Inputs:

# - **identifier_col**: Identifier column name.
# - **input_file_path**: Path of the input file.
# - **output_directory_path**: Path of the output directory.
# - **max_rows_per_chunk**: Maximum allowed rows per file.
# - **has_header**: If the output files have a header.
# - **parallel_partitions_number**: Number of parallel processes.

def chunk_input_file(identifier_col: Any , input_file_path: str, output_directory_path: str, max_rows_per_chunk: int, has_header: bool, parallel_partions_number: int):
    
    # A function to write the given chunk to .tsv file
    # input chunk: Data Frame
    # file_number: Integer
    def write_chunk_to_file(chunk: DataFrame, file_number: int):
        # for naming consistency
        file_number += 1
        # Output directory path for .csv method
        created_dir = f"{output_directory_path}/temporary_sample-part_{file_number}"
        # merge pieces created by spark prallel processors to a single .tsv 
        chunk.coalesce(1).write.option("delimiter", "\t").option("quote","\u0000").option("escape", "\u0000").mode("overwrite").csv(created_dir, header=has_header)
        # finding .tsv file created by .csv method and rename it
        output_file = f"{created_dir}/part-00000-*"
        output_path = f"{output_directory_path}/sample-part_{file_number}.tsv"
        # move the output file to correct path 
        os.system(f"mv {output_file} {output_path}")
        # remove the old directory
        os.system(f"rm -r {created_dir}")
    
    # create the output directory if it doesnt exists
    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path)
    
    # Initialize a spark session to process the large .tsv file in parallel
    spark = SparkSession.builder \
        .appName("Chunk given tsv") \
        .getOrCreate()
    
    # create a spark data frame of the input file
    spark_df = spark.read.option("delimiter", "\t").option("quote","\u0000").option("escape", "\u0000").csv(input_file_path, header=True)
    # repartition the spark dataframe to equal size partitions based on the 
    # parallel_partions_number variable (parallel processing each partition)
    spark_df = spark_df.repartition(parallel_partions_number)
    # Get unique identifier elements in the identifier column
    distinct_identifiers = spark_df.select(identifier_col).distinct()

    # set variables to store rows in chunk, its size, and split number
    chunk = None
    chunk_size = 0
    file_number = 0
    # Gather all distinct identifiers from distibuted data and iterate them
    for identifier_element in distinct_identifiers.collect(): 
        # extract the identifier value
        identifier_element_id = identifier_element[0]
        # find rows with the same identifier value in each data partition in parallel and then aggregate the results
        rows_with_identifier = spark_df.filter(spark_df[identifier_col] == identifier_element_id)
        # count the number of rows calculated in the previous step
        rows_with_identifier_count = rows_with_identifier.count()

        # check if adding filtered data to the current chunk may exceeds the row limit
        if chunk_size + rows_with_identifier_count > max_rows_per_chunk:
            # if we have a current chunk and we are exceeding the limit 
            # write the chunk and clear it
            # reset the size
            # increase file split number
            if chunk is not None:
                write_chunk_to_file(chunk, file_number)
                file_number += 1
                chunk = None
                chunk_size = 0

        # if our chunk is empty fill it with filtered data, otherwise append the data to it
        if chunk is None:
            chunk = rows_with_identifier
        else:
            chunk = chunk.union(rows_with_identifier)
        
        # update the size variable
        chunk_size += rows_with_identifier_count

    # if at the end of the process there is a remained chunk (because it has not exceed the limit)
    # write it to a separate chunk
    if chunk is not None:
        write_chunk_to_file(chunk, file_number)
    # STOP the session
    spark.stop()
    
    ### get the configurations from args and run the function

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="convert the input file to chunks")
    parser.add_argument("--id_col", type=str, required=True, help="input file path")
    parser.add_argument("--input_file", type=str, default="shared/sample-data/sample.tsv", help="input file path")
    parser.add_argument("--output_dir", type=str, default="shared/sample-output", help="output directory path")
    parser.add_argument("--max_rows", type=int, required=True, help="maximum number of rows per chunk")
    parser.add_argument("--header", type=lambda x: (str(x).lower() == 'true'), default=True, help="include header in output files?")
    parser.add_argument("--num_proc", type=int, required=True, help="number of the parallel processes")

    args = parser.parse_args()


    
    # run the function using config values as input parameters
    chunk_input_file(identifier_col= args.id_col, 
                    input_file_path= args.input_file, 
                    output_directory_path= args.output_dir, 
                    max_rows_per_chunk= args.max_rows, 
                    has_header= args.header, 
                    parallel_partions_number= args.num_proc)


# python3 splitWithSpark.py --id_col '"filename"'  --max_rows 3 --header True --num_proc 5 --input_file shared/sample-data/sample.tsv --output_dir shared/sample-output
