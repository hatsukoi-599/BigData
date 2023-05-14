# Top-K Word Frequency Counter

This Python program analyzes a given text file and returns the top k most frequent words except stop words. It includes three different methods for counting word frequencies: brute force, multi-threading, and multi-processing. The main function uses the multi-processing method by default, as it is the fastest. Additionally, a `draw_plot.py` file is provided to generate comparison plots of the three methods' performance metrics.

## Dependencies

- Python 3.x (tested on Python 3.7)
- tqdm
- matplotlib
- psutil

## Installation

1. Download the zip file and extract it to your desired location.
2. Navigate to the extracted directory in the command line or terminal.
3. Install the required dependencies:

```
pip install tqdm matplotlib psutil
```



## Usage

### Top-K Word Frequency Counter

1. Open the `main.py` file in a Python IDE or text editor.
2. Modify the `input_path` variable to match the name of the text file you want to analyze and place the text file in the same directory as the program.
3. Modify the `k` variable to the desired number of top words you want to find. (Default k = 10)
4. Modify the `num_processes` variable to the maximum number of processes you want. (Default num_processes = 16)
5. Modify the `chunk_size` variable to the chunk size for splitting the input file (Default chunk_size = 512MB)
6. Save your changes and run the program.

For example:

```python
input_path = '/path/to/example.txt'
num_processes = 8
chunk_size = 1024 * 1024 * 512
k = 10
print(map_reduce_file(input_path,chunk_size,k,num_processes))
```

This will analyze the `example.txt` file and return the top 10 most frequent words.

### Performance Comparison Plots

1. Open the `drawplot.py` file in a Python IDE or text editor.
2. Modify the `input_filename` variable to match the name of the text file you want to analyze for performance comparison.
3. Save your changes and run the `draw_plot.py` program.
4. The program will generate a comparison plot of the three methods' running time, CPU usage, and memory usage, based on 10 iterations for each method.

## Output

The Top-K Word Frequency Counter program will display the top k most frequent words in the format:

```mathematica
[('word1',frequency1),('word2',frequency2),...,('wordK',frequencyK),]
```

The `draw_plot.py` program will generate a plot showing the performance comparison of the three methods.

## Notes

- The program ignores punctuation and capitalization when counting word frequencies.
- The program only supports plain text files with the `.txt` extension.
- Program performance is closely related to the parameters and your PC configuration
- Files under 500MB to test the `draw_plot.py` program are recommended.

## License

This project is released under the MIT License. See `LICENSE` file for more information.

