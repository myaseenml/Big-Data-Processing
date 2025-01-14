#-------------------------------------------------------------------------------------------------------------------
# BIG DATA PROCESSING ASSIGNMENT 3
# ABHINAV BOHRA 18CS30049
#-------------------------------------------------------------------------------------------------------------------
# Run the script using the command 
# spark-submit assignment-3-18CS30049.py <path to file> <query-word> <k> <stopword-file>
# spark-submit assignment-3-18CS30049.py input.txt pyspark 3 stopword-list.txt
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

def co_occur(query_word, line):
    result = list()
    line = list(set(line))
    if query_word in line:
        for word in line:
            if word != query_word:
                result.append((word, 1))
    return result

# Function to Calculate PMI (Pointwise Mutual Information) between Two Words
def pmi(line, query_word, occurrence, N):
    w1 = line[0]
    w2 = query_word
    pw1w2 = line[1]/N
    pw1 = occurrence[w1]/N
    pw2 = occurrence[w2]/N
    pmi = math.log2(pw1w2 / (pw1 * pw2))
    return pmi

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
    file_content = sc.textFile(input_file) # Create an RDD from the input file
    stopwords = sc.textFile(stopword_file).collect() # Create a list of stopwords from the stopword file

    # Preprocess Data
    d1 = file_content.flatMap(lambda line: line.split("\n")) # Split each line by newline character
    d2 = d1.map(lambda line: [word.lower() for word in line.split(" ") if len(word) > 0]) # Split each line by space character, convert to lowercase, and remove empty words
    d3 = d2.map(lambda line: [word for word in line if word not in set(stopwords)]) # Remove stopwords from each line
    d4 = d3.map(lambda line: [word for word in line if is_english_letters_only(word)]) # Remove non-English letter characters from each line
    d5 = d4.flatMap(lambda line: [(word, 1) for word in list(set(line))]) # Create a list of unique words and assign a count of 1 to each word
    d6 = d4.flatMap(lambda line: co_occur(query_word,line)) # Create a list of words that co-occur with the query word in each line
    occurrence = d5.reduceByKey(lambda x, y: x + y) # Reduce the counts of each word
    co_occurrence = d6.reduceByKey(lambda x, y: x + y) # Reduce the counts of co-occurring word pairs
    occurrence_dict = dict(occurrence.collect()) # Convert the RDD of word counts to a Python dictionary

    N = d4.count()
    if N==0:
      exit()    
    results = co_occurrence.map(lambda line: (line[0], pmi(line, query_word, occurrence_dict, N))) # Calculate PMI between each co-occurring word pair and the query word
    top_k_pos = results.filter(lambda x: x[1] >= 0).sortBy(lambda x: -x[1]).take(k)
    top_k_neg = results.filter(lambda x: x[1] < 0).sortBy(lambda x: x[1]).take(k)
    total_list = top_k_pos + top_k_neg

    # Print Results
    for word, pmi in total_list:
      print(word, pmi)

    print(co_occurrence.sortBy(lambda x: -x[1]).collect())
    print("\n\n")
    print(occurrence.sortBy(lambda x: -x[1]).collect())