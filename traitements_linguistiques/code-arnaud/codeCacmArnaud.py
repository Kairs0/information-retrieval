import nltk
from collection import *
from block import *
import time
from nltk.corpus import stopwords


# def tokenize(cacm_file):
#     tokens = []
#     with open(cacm_file) as f:
#         content = f.read()
#
#     data = content.split("\n.")
#
#     for block in data:
#         if block[0] == 'T' or block[0] == 'W' or block[0] == 'K':
#             tmp = block.replace('.', ' ')\
#                 .replace(',', ' ')\
#                 .replace(':', ' ')\
#                 .replace('(', ' ')\
#                 .replace(')', ' ')\
#                 .lower()
#             tokens += nltk.word_tokenize(tmp)
#
#     return tokens


# def vocabulary(tokens, common_words_file):
#     words_filtered = []
#     with open(common_words_file) as f:
#         content_common = f.read()
#
#     commons_words_list = content_common.split("\n")
#
#
#
#     # https://pythonspot.com/en/nltk-stop-words/
#     stopwords = set(commons_words_list)
#
#     for w in tokens:
#         if w not in stopwords:
#             words_filtered.append(w)
#
#     return words_filtered


if __name__ == "__main__":
    collection = Collection("cacm.all", "cacm")
    collection.calc_blocks()
    collection.tokenize()
    tokens = collection.tokens
    print(len(tokens))


    #### OLD
    # start_time = time.time()
    # # Question 1
    # tokens_result = tokenize("cacm.all")
    # print("Number of tokens: ")
    # print(len(tokens_result))
    # t1 = time.time()
    # print("Result calculated in " + str(t1 - start_time) + " s")
    #
    # # Question 2
    # filtereds = vocabulary(tokens_result, "common_words")
    # print("Size of vocabulary: ")
    # print(len(filtereds))
    # t2 = time.time()
    # print("Result calculated in " + str(t2 - start_time) + " s")
    #
    # # Question 3
    # # Generer nouveau ficher "semicacm.all" et effectuer le même traitement


