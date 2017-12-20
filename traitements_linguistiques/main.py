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
    print("Tokenization done : " + str(time.time() - start_time))

    posting_list, dictionary = collection.create_posting_list()
    print("Dictionary + Posting List created : " + str(time.time() - start_time))

    docID_index = collection.create_docID_index(posting_list, dictionary)
    print("Simple InverseIndex created : " + str(time.time() - start_time))

    docID_index_with_frequency = collection.create_docID_index_with_frequency(posting_list, dictionary)
    print("InverseIndex w/ freq created : " + str(time.time() - start_time))
    
    docID_weight = collection.create_docID_weight(docID_index_with_frequency, dictionary)
    print("Doc Weight table created : " + str(time.time() - start_time))


    with open("dic_terms.json", 'w') as json_terms:
        json.dump(dictionary, json_terms)

    with open("docID_index.json", 'w') as json_index:
        json.dump(docID_index, json_index)
    
    with open("docID_index_with_freq.json", 'w') as json_index_with_freq:
        json.dump(docID_index_with_frequency, json_index_with_freq)

    with open("docID_weight.json", 'w') as json_weight:
        json.dump(docID_weight, json_weight)

    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")