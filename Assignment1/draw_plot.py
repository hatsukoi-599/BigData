import time
from tqdm import tqdm
from matplotlib import pyplot as plt
from multi_processes import map_reduce_file as mp
from brute_force import count_words as bf
from multi_thread import map_reduce_file as mt
import psutil
import os

def draw_plot(input_path, file_name):
    file_size = os.path.getsize(input_path)
    file_size_to_mb = file_size // (1024 * 1024)
    maximum_size = 1024 * 1024 * 512
    num_processes = num_threads = 8
    k = 10
    chunk_size = file_size // 8 if file_size // 8 < maximum_size else maximum_size

    x_data = range(1, 11)
    mp_dict = {'cpu':[], 'memory':[],'time':[]}
    mt_dict = {'cpu': [], 'memory': [], 'time': []}
    bf_dict = {'cpu': [], 'memory': [], 'time': []}

    for i in tqdm(range(10),desc='Multi Processes'):
        timestamps1 = time.time()
        multi_processes = mp(input_path, chunk_size, k, num_processes)
        timestamps2 = time.time()
        mp_dict['cpu'].append(psutil.cpu_percent())
        mp_dict['memory'].append(psutil.virtual_memory().percent)
        mp_dict['time'].append(timestamps2 - timestamps1)

    for i in tqdm(range(10),desc='Multi Thread'):
        timestamps1 = time.time()
        multi_thread = mt(input_path, chunk_size, k, num_threads)
        timestamps2 = time.time()
        mt_dict['cpu'].append(psutil.cpu_percent())
        mt_dict['memory'].append(psutil.virtual_memory().percent)
        mt_dict['time'].append(timestamps2 - timestamps1)

    for i in tqdm(range(10),desc='Brute Force'):
        timestamps1 = time.time()
        brute_force = bf(input_path, k)
        timestamps2 = time.time()
        bf_dict['cpu'].append(psutil.cpu_percent())
        bf_dict['memory'].append(psutil.virtual_memory().percent)
        bf_dict['time'].append(timestamps2 - timestamps1)

    # Time Comparison Over Multiple Runs
    plt.plot(x_data, bf_dict['time'], label='Brute Force', color='blue')
    plt.plot(x_data, mp_dict['time'], label='Multi Processes', color='red')
    plt.plot(x_data, mt_dict['time'], label='Multi Thread', color='green')
    plt.title(f"Time Usage Comparison({file_name})")
    plt.xlabel('Iteration')
    plt.ylabel('Time (s)')
    plt.legend()
    plt.show()

    # CPU Usage
    plt.plot(x_data, bf_dict['cpu'], label='Brute Force', color='blue')
    plt.plot(x_data, mp_dict['cpu'], label='Multi Processes', color='red')
    plt.plot(x_data, mt_dict['cpu'], label='Multi Thread', color='green')
    plt.title(f"CPU Usage Comparison({file_name})")
    plt.xlabel('Iteration')
    plt.ylabel('CPU Usage (%)')
    plt.legend()
    plt.show()

    plt.plot(x_data, bf_dict['memory'], label='Brute Force', color='blue')
    plt.plot(x_data, mp_dict['memory'], label='Multi Processes', color='red')
    plt.plot(x_data, mt_dict['memory'], label='Multi Thread', color='green')
    plt.title(f"Memory Usage Comparison({file_name})")
    plt.xlabel('Iteration')
    plt.ylabel('Memory Usage (%)')
    plt.legend()
    plt.show()

    print(mp_dict['time'])
    print(mp_dict['cpu'])
    print(mp_dict['memory'])

    print(mt_dict['time'])
    print(mt_dict['cpu'])
    print(mt_dict['memory'])

    print(bf_dict['time'])
    print(bf_dict['cpu'])
    print(bf_dict['memory'])