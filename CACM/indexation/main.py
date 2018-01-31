import json
import time
from collection import Collection

PATH_COLLECTION = r'../collection_data/cacm.all'
PATH_DICTIONARY = r'../fichiers_traitements/dictionary.json'
PATH_POSTING_LIST = r'../fichiers_traitements/posting_list.json'
PATH_LIST_DOC_WEIGHT = r'../fichiers_traitements/list_doc_weight.json'


def str_time():
    return str(round(time.time() - START_TIME, 2)) + " s"


if __name__ == "__main__":
    START_TIME = time.time()

    # We create the Collection object. We load the collection data in memory.
    COLLECTION = Collection(PATH_COLLECTION, "cacm")

    # We split the collection data into Document objects,
    # then we tokenize the content of the Document objects.
    COLLECTION.calc_documents()
    print("Number of docs : " + str(len(COLLECTION.documents)))
    COLLECTION.tokenize()
    print("Tokenisation done : " + str_time())

    # We create the posting_list and the dictionary (No MapReduce nor BSBI)
    posting_list, dictionary = COLLECTION.create_posting_list()
    print("Dictionary + Posting List created : " + str_time())

    # We compute the total weight of each docs (for vector research).
    doc_weights = COLLECTION.create_doc_weights(posting_list)
    print("Doc Weight table created : " + str_time())

    # We write these information on disk.
    with open(PATH_DICTIONARY, 'w') as json_terms:
        json.dump(dictionary, json_terms)

    with open(PATH_POSTING_LIST, 'w') as json_index_with_freq:
        json.dump(posting_list, json_index_with_freq)

    with open(PATH_LIST_DOC_WEIGHT, 'w') as json_weight:
        json.dump(doc_weights, json_weight)

    print("Files written on disk : " + str_time())
