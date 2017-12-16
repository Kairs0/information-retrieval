import math, time, numpy as np, matplotlib.pyplot as plt
from collection import Collection

if __name__ == "__main__":
    start_time = time.time()

    collection = Collection("cacm.all", "cacm")
    collection.calc_blocks()
    collection.tokenize()
    tokens = collection.tokens

    # Question 1
    print("Q1. Number of tokens: ")
    # print(tokens)
    print(len(tokens))
    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")
    print("-------------------------------------------------\n")

    # Question 2
    print("Q2. Size of vocabulary: ")
    collection.calc_vocabulary("common_words")
    voc_coll = collection.vocabulary
    print(len(voc_coll))
    t2 = time.time()
    print("Result calculated in " + str(round(t2 - start_time, 2)) + " s")
    print("-------------------------------------------------\n")

    # Question 3
    print("Q3. Nb of tokens and size of vocabulary for half the collection")
    half_collection = Collection("cacm.all", "half-cacm")
    half_collection.calc_blocks()
    index_half = int(len(half_collection.blocks)/2)
    half_collection.blocks = half_collection.blocks[:index_half]
    half_collection.tokenize()
    half_collection.calc_vocabulary("common_words")
    half_tokens = half_collection.tokens
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
    plt.show()
    print("Graph frequency vs inverse rank")
    inverse_rank = 1. / np.array(range(1, len(token_occurrence) + 1))
    plt.xlabel('1 / Rank')
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
    plt.show()
