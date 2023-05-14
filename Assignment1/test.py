import os
import multiprocessing as mp
from collections import Counter
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Load stop words
stop_words = set()
with open('stopwords.txt', 'r') as f:
    for line in f:
        stop_words.add(line.strip())

def read_chunk(file_path, start, size):
    """
        Read a chunk of file from the start position with the given size and
        count the frequency of each word

        Args:
            file_path (str): The path of the file to be read
            start (int): The start position of the chunk
            size (int): The size of the chunk

        Returns:
            Counter: A Counter object that counts the frequency of each word
    """
    with open(file_path, 'r') as f:
        f.seek(start)
        chunk = f.read(size)
        word_counts = Counter()
        for line in tqdm(chunk.strip().split('\n')):
            words = line.strip().split()
            for word in words:
                word = word.lower()
                if word not in stop_words:
                    word_counts[word] += 1
    return word_counts

def merge_counts(results):
    """
       Merge the Counter objects into one

       Args:
           results (list): A list of Counter objects

       Returns:
           Counter: A merged Counter object
       """
    return sum(results, Counter())

def get_top_k(word_counts, k):
    """
       Get the top k frequent words

       Args:
           word_counts (Counter): A Counter object that counts the frequency of each word
           k (int): The number of top frequent words to return

       Returns:
           list: A list of (word, frequency) tuples in descending order of frequency
       """
    return word_counts.most_common(k)

def map_reduce_file(input_path, chunk_size = 1024 * 1024 * 512, k = 10, num_processes = 16):
    file_size = os.path.getsize(input_path)
    num_chunks = (file_size + chunk_size - 1) // chunk_size
    args_list = [(input_path, i * chunk_size, chunk_size) for i in range(num_chunks)]

    with mp.Pool(num_processes) as pool:
        results = pool.starmap(read_chunk, args_list)

    word_counts = merge_counts(results)
    return get_top_k(word_counts, k)

