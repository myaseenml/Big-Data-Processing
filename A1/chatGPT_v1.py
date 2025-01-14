import os
import re
import sys
import time
import signal
import concurrent.futures
from collections import defaultdict

def signal_handler(signal, frame):
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

def count_ngrams(file_path, n):
    with open(file_path, 'r') as f:
        text = f.read()
        tokens = tokenize(text)
        ngrams = [tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]
        return ngrams

def process_files(dir_path, n, results):
    for file_name in os.listdir(dir_path):
        file_path = os.path.join(dir_path, file_name)
        if os.path.isfile(file_path):
            class_id = os.path.basename(os.path.dirname(file_path))
            ngrams = count_ngrams(file_path, n)
            for ngram in ngrams:
                results[ngram][class_id] += 1

def calculate_salience_score(results, k):
    scores = defaultdict(int)
    for ngram, class_counts in results.items():
        for class_id, count in class_counts.items():
            scores[ngram] += count
    return sorted(scores, key=scores.get, reverse=True)[:k]

def main(dir_path, num_threads, n, k):
    results = defaultdict(lambda: defaultdict(int))
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        dirs = [os.path.join(dir_path, d) for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]
        futures = [executor.submit(process_files, d, n, results) for d in dirs]
        concurrent.futures.wait(futures)
    scores = calculate_salience_score(results, k)
    for score in scores:
        print(score)

if __name__ == '__main__':
    start_time = time.time()
    dir_path = sys.argv[1]
    num_threads = int(sys.argv[2])
    n = int(sys.argv[3])
    k = int(sys.argv[4])
    main(dir_path, num_threads, n, k)
    print(f"Time Taken: {round(time.time()-start_time, 2)}s | Num Threads: {num_threads}")
