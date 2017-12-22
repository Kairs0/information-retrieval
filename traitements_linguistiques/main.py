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
    print("Tokenisation done : " + str(time.time() - start_time))

    posting_list, dictionary = collection.create_posting_list()
    # import pdb; pdb.set_trace()
    print("Dictionary + Posting List created : " + str(time.time() - start_time))

    docID_index = collection.create_docID_index(posting_list)
    print("Simple InverseIndex created : " + str(time.time() - start_time))
    
    docID_weight = collection.create_docID_weight(docID_index)
    print("Doc Weight table created : " + str(time.time() - start_time))


    with open("dictionary.json", 'w') as json_terms:
        json.dump(dictionary, json_terms)

    with open("inverse_index_simple.json", 'w') as json_index:
        json.dump(docID_index, json_index)
    
    with open("inverse_index_freq.json", 'w') as json_index_with_freq:
        json.dump(posting_list, json_index_with_freq)

    with open("list_doc_weight.json", 'w') as json_weight:
        json.dump(docID_weight, json_weight)

    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")