import math
import time
import numpy as np
import matplotlib.pyplot as plt

import sys
sys.path.insert(0, "../indexation")

from collection import Collection

PATH_COLLECTION = r'..\collection_data\cacm.all'
PATH_COMMON_WORDS = r'../collection_data/common_words'

if __name__ == "__main__":
    start_time = time.time()

    collection = Collection(PATH_COLLECTION, "cacm")
    collection.calc_documents()
    collection.tokenize()
    tokens = []
    for document in collection.documents:
        tokens += document.tokens

    # Question 1
    print("Q1. Number of tokens: ")
    # print(tokens)
    print(len(tokens))
    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")
    print("-------------------------------------------------\n")

    # Question 2
    print("Q2. Size of vocabulary: ")
    collection.calc_vocabulary(PATH_COMMON_WORDS)
    voc_coll = collection.vocabulary
    print(len(voc_coll))
    t2 = time.time()
    print("Result calculated in " + str(round(t2 - start_time, 2)) + " s")
    print("-------------------------------------------------\n")

    # Question 3
    print("Q3. Nb of tokens and size of vocabulary for half the collection")
    half_collection = Collection(PATH_COLLECTION, "half-cacm")
    half_collection.calc_documents()
    index_half = int(len(half_collection.documents) / 2)
    half_collection.documents = half_collection.documents[:index_half]
    half_collection.tokenize()
    half_collection.calc_vocabulary(PATH_COMMON_WORDS)
    half_tokens = []
    for doc in half_collection.documents:
        half_tokens += doc.tokens
    half_voc = half_collection.vocabulary
    print("Number of tokens (half-collection):")
    print(len(half_tokens))
    print("Size of voc (half-collection) :")
    print(len(half_voc))
    b = math.log(len(voc_coll) / len(half_voc)) / \
        math.log(len(tokens) / len(half_tokens))
    k = len(voc_coll) / (len(tokens) ** b)
    print("K : ", k, " |  B : ", b)
    t3 = time.time()
    print("Result calculated in " + str(round(t3 - start_time, 2)) + " s")
    print("-------------------------------------------------\n")

    # Question 4
    print("Q4. Estimation of the size of a one-million-tokens-collection's vocabulary")
    result = math.floor(k*(1000000**b))
    print("Result : ", result)
    print("-------------------------------------------------\n")

    # Question 5
    print("Q5. Zipf Law : Graph Frequency vs Rank :")
    token_occurrence = {}
    for token in map(str.lower, tokens):
        if token in token_occurrence:
            token_occurrence[token] += 1
        else:
            token_occurrence[token] = 1

    ranks = range(1, len(token_occurrence) + 1)
    occurrences = []
    for value in sorted(token_occurrence.values(), reverse=True):
        occurrences.append(value)
    plt.plot(ranks, occurrences)
    plt.xlabel('Rank')
    plt.ylabel('Occurrence')
    plt.title("Frequency vs Rank")
    plt.show()
    print("Graph frequency vs inverse rank")
    inverse_rank = 1. / np.array(range(1, len(token_occurrence) + 1))
    plt.xlabel('1 / Rank')
    plt.ylabel('Occurrence')
    plt.title("Frequency vs inverse Rank")
    plt.plot(inverse_rank, occurrences)
    plt.show()
    print("Graph log(f) vs log(r)")
    log_ranks = []
    for rank_log_value in map(math.log, range(1, len(token_occurrence) + 1)):
        log_ranks.append(rank_log_value)

    log_freq = []
    for value in sorted(token_occurrence.values(), reverse=True):
        log_freq.append(math.log(value))

    plt.plot(log_ranks, log_freq)
    plt.xlabel('Log rank')
    plt.ylabel('Log occurrence')
    plt.title("log frequency vs log Rank")
    plt.show()
