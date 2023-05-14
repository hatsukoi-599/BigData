# Author: Xiaoxiao Jiang & Shweta Deshmukh
# Date: 2023-04-23
# Description: Map Reduce algorithm to count top k frequent words in a text file

import collections
from tqdm import tqdm

# Load stop words
stop_words = set()
with open('stopwords.txt', 'r') as f:
    for line in f:
        stop_words.add(line.strip())

def count_words(file_path, top_k):
    # Create an empty Counter object to store word counts
    word_count = collections.Counter()
    with open(file_path, 'r', buffering=1024*1024*512) as f:
        for line in tqdm(f,desc='Processing'):
            # Iterate through each word in the line
            for word in line.strip().split():
                word = word.lower()
                # If the word is not a stop word, increment its count in the Counter object
                word = word.lower()
                if word not in stop_words:
                    word_count[word] += 1
    return word_count.most_common(top_k)

