import json
from collections import defaultdict
from collection import Collection
from document import Document
import time

PATH_COLLECTION = r'..\collection_data\cacm.all'
PATH_DICTIONARY = r'..\fichiers_traitements\dictionary.json'
PATH_INVERSE_INDEX_SIMPLE = r'..\fichiers_traitements\inverse_index_simple.json'
PATH_INVERSE_INDEX_FREQ = r'..\fichiers_traitements\inverse_index_freq.json'
PATH_LIST_DOC_WEIGHT = r'..\fichiers_traitements\list_doc_weight.json'


if __name__ == "__main__":
    start_time = time.time()
    collection = Collection(PATH_COLLECTION, "cacm")
    collection.calc_documents()  # calc all documents inside the given collection
    collection.tokenize()  # generates the list of tokens for each document
    print("Tokenisation done : " + str(time.time() - start_time))

    posting_list, dictionary = collection.create_posting_list()
    # import pdb; pdb.set_trace()
    print("Dictionary + Posting List created : " + str(time.time() - start_time))

    docID_index = collection.create_docID_index(posting_list)
    print("Simple InverseIndex created : " + str(time.time() - start_time))
    
    docID_weight = collection.create_docID_weight(docID_index)
    print("Doc Weight table created : " + str(time.time() - start_time))


    with open(PATH_DICTIONARY, 'w') as json_terms:
        json.dump(dictionary, json_terms)

    with open(PATH_INVERSE_INDEX_SIMPLE, 'w') as json_index:
        json.dump(docID_index, json_index)
    
    with open(PATH_INVERSE_INDEX_FREQ, 'w') as json_index_with_freq:
        json.dump(posting_list, json_index_with_freq)

    with open(PATH_LIST_DOC_WEIGHT, 'w') as json_weight:
        json.dump(docID_weight, json_weight)

    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")