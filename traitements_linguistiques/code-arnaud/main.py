from collection import *
import time

if __name__ == "__main__":
    start_time = time.time()

    collection = Collection("cacm.all", "cacm")
    collection.calc_blocks()
    collection.tokenize()
    tokens = collection.tokens

    # Question 1
    print("Q1. Number of tokens: ")
    print(len(tokens))
    t1 = time.time()
    print("Result calculated in " + str(t1 - start_time) + " s")

    # Question 2
    print("Q2. Size of vocabulary: ")
    collection.calc_vocabulary("common_words")
    voc_coll = collection.vocabulary
    print(len(voc_coll))
    t2 = time.time()
    print("Result calculated in " + str(t2 - start_time) + " s")

    # Question 3
    print("Q3. Nb of tokens and size of vocabulary for half the collection")
    half_collection = Collection("cacm.all", "half-cacm")
    half_collection.calc_blocks()
    index_half = int(len(half_collection.blocks)/2)
    half_collection.blocks = half_collection.blocks[:index_half]
    half_collection.tokenize()
    half_collection.calc_vocabulary("common_words")
    tokens_half = half_collection.tokens
    half_voc = half_collection.vocabulary
    print("Number of tokens:")
    print(len(tokens_half))
    print("Size of voc:")
    print(len(half_voc))
    t3 = time.time()
    print("Result calculated in " + str(t3 - start_time) + " s")


