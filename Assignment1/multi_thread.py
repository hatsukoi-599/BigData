# Author: Xiaoxiao Jiang & Shweta Deshmukh
# Date: 2023-04-23
# Description: Map Reduce algorithm to count top k frequent words in a text file
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Load stop words
stop_words = set()
with open('stopwords.txt', 'r') as f:
    for line in f:
        stop_words.add(line.strip())


def map_file(file_path):
    """
        Maps the given file path to its word counts

        Args:
            file_path (str): Path to the file to map

        Returns:
            Counter: Word counts of the file
        """
    with open(file_path, 'r') as f:
        word_counts = Counter()
        for line in tqdm(f,desc='Processing'):
            words = line.strip().split()
            for word in words:
                word = word.lower()
                if word not in stop_words:
                    word_counts[word] += 1
        return word_counts


def merge_counts(results):
    """
        Merges multiple counters into a single one

        Args:
            results (list of Counters): The counters to merge

        Returns:
            Counter: The merged counter
        """
    return sum(results, Counter())


def get_top_k(word_counts, k):
    """
       Returns the top k elements of a given counter

       Args:
           word_counts (Counter): The counter to extract top k elements from
           k (int): The number of top elements to extract

       Returns:
           list of tuples: The top k elements and their counts
       """
    return word_counts.most_common(k)


def split_file(input_path, chunk_size):
    """
        Splits a file into smaller files of given size

        Args:
            input_path (str): Path to the file to split
            chunk_size (int): Size of each chunk

        Returns:
            list of strings: Paths of the split files
        """
    file_paths = []
    with open(input_path, 'r') as f:
        while True:
            lines = f.readlines(chunk_size)
            if not lines:
                break
            file_path = f"{input_path}_{len(file_paths)}"
            file_paths.append(file_path)
            with open(file_path, 'w') as chunk:
                chunk.writelines(lines)
    return file_paths


def delete_file(file_path):
    """
        Deletes the given file

        Args:
            file_path (str): Path to the file to delete
        """
    os.remove(file_path)


def delete_files(file_paths):
    """
        Deletes multiple files in parallel

        Args:
            file_paths (list of str): Paths of the files to delete
        """
    with ThreadPoolExecutor() as executor:
        list(executor.map(delete_file, file_paths))


def map_reduce_file(input_path, chunk_size, k, num_threads):
    """
       Counts the word frequency in a given file using the MapReduce approach with multi-threading

       Args:
           input_path (str): Path to the file to count the word frequency from
           chunk_size (int): Size of each chunk
           k (int): Number of top frequent words to return
           num_threads (int): Number of threads to use for parallel processing

       Returns:
           list of tuples: The top k frequent words and their counts
       """
    file_paths = split_file(input_path, chunk_size)
    results = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_path = {executor.submit(map_file, path): path for path in file_paths}
        for future in as_completed(future_to_path):
            results.append(future.result())

    word_counts = merge_counts(results)
    delete_files(file_paths)
    return get_top_k(word_counts, k)

