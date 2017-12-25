import json
from collection import Collection
import time

if __name__ == "__main__":
    start_time = time.time()
    collection = Collection(r'.\collection_data\CACM\cacm.all', "cacm")
    block = collection.create_block()
    block.get_documents()
    block.tokenize()
    print("Tokenisation done : " + str(time.time() - start_time))

    posting_list, dictionary = block.create_posting_list()
    # import pdb; pdb.set_trace()
    print("Dictionary + Posting List created : " + str(time.time() - start_time))

    docID_index = block.create_docID_index(posting_list)
    print("Simple InverseIndex created : " + str(time.time() - start_time))
    
    docID_weight = block.create_docID_weight(docID_index)
    print("Doc Weight table created : " + str(time.time() - start_time))


    with open(r'.\collection_data\CACM\dictionary.json', 'w') as json_terms:
        json.dump(dictionary, json_terms)

    with open(r'.\collection_data\CACM\inverse_index_simple.json', 'w') as json_index:
        json.dump(docID_index, json_index)
    
    with open(r'.\collection_data\CACM\inverse_index_freq.json', 'w') as json_index_with_freq:
        json.dump(posting_list, json_index_with_freq)

    with open(r'.\collection_data\CACM\list_doc_weight.json', 'w') as json_weight:
        json.dump(docID_weight, json_weight)

    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")