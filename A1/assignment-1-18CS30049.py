import os
import re
import sys
import time
import math
import signal
import threading
from collections import defaultdict

#--------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#--------------------------------------------------------------------------------------------------------------------
def signal_handler(signal, frame):
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

def default_dict_lambda():
    return defaultdict(int)

def merge_dicts(dicts):
    result = {}
    for d in dicts:
        for k, v in d.items():
            result[k] = result.get(k, 0) + v
    return result


#--------------------------------------------------------------------------------------------------------------------
# CORE FUNCTIONS
#--------------------------------------------------------------------------------------------------------------------

# Returns list of tuples (file_path, class) and a dictionary of class frequency distribution: (class_name: #num_docs)
def getDocsDetails(dirs, docs_map, docs_per_class):
    for folder_name in dirs: # folder_name is class                                  
        folder_path = os.path.join(dir_path, folder_name)
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                docs_map.append((file_path, folder_name)) # (file_path, class)       
                docs_per_class[folder_name] +=1

# Tokenize text on non-alphanumerics
def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

# Calculates ngram for each text file
def count_ngrams(file_path, n):
    with open(file_path, 'r') as f:
        text = f.read()
        tokens = tokenize(text)
        ngrams = [tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]
        return ngrams

# Calculates ngrams at class level
def getClass_ngrams(docs_map, n, i, results_class_ngrams):
    results = defaultdict(default_dict_lambda)
    for item in docs_map:
        file_path = item[0]
        class_label = item[1]
        ngrams = count_ngrams(file_path, n)
        for ngram in ngrams:
            results[class_label][ngram] +=1

    results_class_ngrams[i] = results

# Aggregates Results
def aggregateResults(results_class_ngrams):
    final_result = defaultdict(default_dict_lambda)
    for d in results_class_ngrams:
        for outer_key, inner_dict in d.items():
            for k, v in inner_dict.items():
                final_result[outer_key][k] += v
    return dict(final_result)


# Calculates Class Salience Score of each ngram: (count of ngram in class)/ #docs in class
def getClassSalienceScore(class_ngrams, scores):
    for class_label in class_ngrams:
        for ngram in class_ngrams[class_label]:
            count = class_ngrams[class_label][ngram]
            salience_score = round(count/docs_per_class[class_label], 3)
            scores.append((ngram, class_label, salience_score))


# Returns top k unique ngrams
def getTopk(scores, k):
    scores_ = [(" ".join(list(t[0])), t[1], t[2]) for t in scores]
    sorted_scores = sorted(scores_, key=lambda x: -x[2])
    unique_list = list()
    seen = set()

    # This is to get the overall topk 'unique' n-grams
    # If a n-gram occurs in two classes, then I have only one (with max salience score) 
    # to ensure ngrams in final output are unique (as asked in the assignment)
    for tup in sorted_scores:
        if (tup[0]) not in seen:
            seen.add((tup[0]))
            unique_list.append(tup)
        if len(unique_list) >= k:
            break # Get only top-k

    return unique_list
    
    
#--------------------------------------------------------------------------------------------------------------------
# MAIN FUNCTION
#--------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    #----------------------------------------------------------------
    # Parsing Command Line Arguments
    #----------------------------------------------------------------
    dir_path = sys.argv[1]
    num_threads = int(sys.argv[2])
    n = int(sys.argv[3])
    k = int(sys.argv[4])
    start_time = time.time()

    #----------------------------------------------------------------
    # STEP 1: GET BASIC DOCUMENT DETAILS (HELPER DATA STRUCTURES)
    #----------------------------------------------------------------
    dirs = os.listdir(dir_path)
    docs_map = list()
    docs_per_class = defaultdict(int)
    getDocsDetails(dirs, docs_map, docs_per_class)

    #----------------------------------------------------------------
    # STEP 2: CALCULATE N-GRAMS OF EACH DOCUMENT (MULTITHREADED)
    #----------------------------------------------------------------
    threads = list()
    results_class_ngrams = [None]*(num_threads)
    chunk_size = math.ceil(len(docs_map)/num_threads)
    for i in range(num_threads):
        start =   (i)*chunk_size
        end   = (i+1)*chunk_size
        t = threading.Thread(target=getClass_ngrams, args=(docs_map[start:end], n, i, results_class_ngrams))
        t.start()
        threads.append(t)

    for t in threads:  # Wait for them all to finish
        t.join()

    #----------------------------------------------------------------
    # STEP 3: AGGREGATE PARTIAL RESULTS
    #----------------------------------------------------------------
    agg_results_class_ngrams = aggregateResults(results_class_ngrams)

    #----------------------------------------------------------------
    # STEP 4: CALCULATE SALIENCE SCORE
    #----------------------------------------------------------------
    scores = list()
    getClassSalienceScore(agg_results_class_ngrams, scores)

    #----------------------------------------------------------------
    # STEP 5: GET TOP-K NGRAMS
    #----------------------------------------------------------------
    answer = getTopk(scores, k)

    #----------------------------------------------------------------
    # Print Results
    #----------------------------------------------------------------
    end_time = time.time()
    ngrams_align_len = max([len(t[0]) for t in answer])
    class_align_len = max([len(t[1]) for t in answer])
    align_dash = "-"*(ngrams_align_len + class_align_len + 25)
    place_holder_text = '{0:<{1}} {2:<{3}} {4:<{5}} {6:<{7}} {8:<{9}} {10:<{11}} {12:<{13}} {14:<{15}} {16:<{17}}'
    print(align_dash)
    print(place_holder_text.format('|',1, 'Sr.No.',6,  '|',1, 'ngram',ngrams_align_len, '|',1, 'Class',class_align_len, '|',1, 'Score',6, '|',1))
    print(align_dash)
    for i, ans in enumerate(answer):
        print(place_holder_text.format('|',1, i+1,6,  '|',1,  ans[0],ngrams_align_len, '|',1, ans[1],class_align_len, '|',1, ans[2],6, '|',1))
    print(align_dash)
    print(f"\nTime Taken: {round(end_time-start_time,2)}s | Num Threads: {num_threads} | n:{n} | k:{k}")
