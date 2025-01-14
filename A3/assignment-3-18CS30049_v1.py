#-------------------------------------------------------------------------------------------------------------------
# BIG DATA PROCESSING ASSIGNMENT 3
# ABHINAV BOHRA 18CS30049
#-------------------------------------------------------------------------------------------------------------------
# Run the script using the command 
# spark-submit assignment-3-18CS30049.py <path to file> <query-word> <k> <stopword-file>
# spark-submit assignment-3-18CS30049.py input.txt Abhinav 5 stopword-list.txt
#-------------------------------------------------------------------------------------------------------------------

# Importing Required Libraries
import re
import sys
import math
from pyspark import SparkContext

#-------------------------------------------------------------------------------------------------------------------
# UTIL FUNCTIONS
#-------------------------------------------------------------------------------------------------------------------

# Function to Check if the word contains only English Letters
def is_english_letters_only(word):
    return bool(re.match('^[A-Za-z]+$', word))

# Function to Get Frequency of Words in a Line
def get_word_freq(line):
    word_freqs = {}
    for word in line:
        if word in word_freqs:
            word_freqs[word] += 1
        else:
            word_freqs[word] = 1
    freq_tuples = [(word, freq) for word, freq in word_freqs.items()]
    return freq_tuples

# Function to Calculate PMI (Pointwise Mutual Information) between Two Words
def pmi(line, occurrence, N):
    w1 = line[0][0]
    w2 = line[0][1]
    pw1w2 = line[1]/N
    pw1 = occurrence[w1]/N
    pw2 = occurrence[w2]/N
    pmi = math.log2(pw1w2 / (pw1 * pw2))
    return pmi

def printrdd(d):
	print('-'*50)
	print(d.collect())
	print('-'*50)
#-------------------------------------------------------------------------------------------------------------------
# MAIN FUNCTION
#-------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    # Parsing Command Line Arguments
    input_file = sys.argv[1]
    query_word = sys.argv[2].lower()
    k = int(sys.argv[3])
    stopword_file = sys.argv[4]

    # Create a SparkSession
    sc = SparkContext.getOrCreate()

    # Load Input File and Stopwords File
    file_content = sc.textFile(input_file)
    stopwords = sc.textFile(stopword_file).collect()

    # Preprocess Data
    d1 = file_content.flatMap(lambda line: line.split("\n"))
    d2 = d1.map(lambda line: [word.lower() for word in line.split(" ") if len(word) > 0])
    d3 = d2.map(lambda line: [word for word in line if word not in set(stopwords)])
    d4 = d3.map(lambda line: [word for word in line if is_english_letters_only(word)])

    # Calculate Word Frequency
    d5 = d4.flatMap(lambda line: get_word_freq(line)).groupByKey()
    d6 = d5.map(lambda line: (line[0], len(line[1])))

    # Calculate Co-occurrence Frequency
    d7 = d4.map(lambda line: [(line[i], line[i + 1]) for i in range(0, len(line) - 1)])
    d8 = d7.flatMap(lambda line: get_word_freq(line)).groupByKey()
    d9 = d8.map(lambda line: (line[0], len(line[1])))
    d10 = d9.filter(lambda line: line[0][0] == query_word or line[0][1] == query_word)

    # Compute PMI between Query Word and Other Words
    N = d4.count()
    if N==0:
    	exit()
    occurrence = dict(d6.collect())
    co_occurrence = d10.map(lambda line: (line[0], pmi(line, occurrence, N)))
    top_k_pos = co_occurrence.filter(lambda x: x[1] >= 0).sortBy(lambda x: -x[1]).take(k)
    top_k_neg = co_occurrence.filter(lambda x: x[1] < 0).sortBy(lambda x: x[1]).take(k)
    total_list = top_k_pos + top_k_neg
    printrdd(d4)
    printrdd(d6)
    printrdd(d10)
    print(query_word, k)
    # Print Results
    for words, pmi in total_list:
    	print(words[0], words[1], pmi)
