import json
from collections import defaultdict
from collection import Collection
from document import Document
import time

if __name__ == "__main__":
    start_time = time.time()

    collection = Collection("cacm.all", "cacm")
    collection.calc_documents()
    collection.tokenize()
    posting_list, dictionary = collection.create_posting_list()

    docID_index = collection.create_docID_index(posting_list, dictionary)


    with open("dic_terms.json", 'w') as json_terms:
        json.dump(dictionary, json_terms)
    
    with open("docID_index.json", 'w') as json_index:
        json.dump(docID_index, json_index)

    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")