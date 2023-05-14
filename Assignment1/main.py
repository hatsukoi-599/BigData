# Author: Xiaoxiao Jiang & Shweta Deshmukh
# Date: 2023-04-23
# Description: Map Reduce algorithm to count top k frequent words in a text file
import os.path
from multi_processes import map_reduce_file
from test import map_reduce_file
import time


if __name__ == '__main__':
    # Set the path to the input file
    input_path = 'data_16GB.txt'
    # Set the number of processes to use for multiprocessing
    num_processes = 16
    # Set the chunk size for splitting the input file
    chunk_size = 1024 * 1024 * 128
    # Set the number of top words to count
    k = 10
    file_size = os.path.getsize(input_path)
    # Run the map reduce function
    time1 = time.time()
    print(map_reduce_file(input_path, chunk_size, k, num_processes))
    time2 = time.time()
    print(f"Time taken to execute the map_reduce_file function: {time2 - time1} seconds")






