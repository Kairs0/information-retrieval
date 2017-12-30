import json
import time
from collection import Collection

PATH_COLLECTION = r'..\collection_data\cacm.all'
PATH_DICTIONARY = r'..\fichiers_traitements\dictionary.json'
PATH_INVERSE_INDEX_SIMPLE = r'..\fichiers_traitements\inverse_index_simple.json'
PATH_INVERSE_INDEX_FREQ = r'..\fichiers_traitements\inverse_index_freq.json'
PATH_LIST_DOC_WEIGHT = r'..\fichiers_traitements\list_doc_weight.json'


def str_time():
    return str(round(time.time() - START_TIME, 2)) + " s"


if __name__ == "__main__":
    START_TIME = time.time()
    collection = Collection(PATH_COLLECTION, "cacm")
    collection.calc_documents()  # calc all documents inside the given collection
    collection.tokenize()  # generates the list of tokens for each document
    print("Tokenisation done : " + str_time())

    print("Number of docs : " + str(len(collection.documents)))

    posting_list, dictionary = collection.create_posting_list()
    print("Dictionary + Posting List created : " + str_time())

    inverted_index = collection.create_inverted_index(posting_list)
    print("Inverted index created : " + str_time())
    
    docID_weight = collection.create_docID_weight(inverted_index)
    print("Doc Weight table created : " + str_time())

    with open(PATH_DICTIONARY, 'w') as json_terms:
        json.dump(dictionary, json_terms)

    with open(PATH_INVERSE_INDEX_SIMPLE, 'w') as json_index:
        json.dump(inverted_index, json_index)
    
    with open(PATH_INVERSE_INDEX_FREQ, 'w') as json_index_with_freq:
        json.dump(posting_list, json_index_with_freq)

    with open(PATH_LIST_DOC_WEIGHT, 'w') as json_weight:
        json.dump(docID_weight, json_weight)

    print("Files written on disk : " + str_time())
