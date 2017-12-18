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
    reverse_index, dic_termId_docId = collection.create_posting_list()

    with open("dic_termId_docId.json", 'w') as json_termIs_docIs:
        json.dump(dic_termId_docId, json_termIs_docIs)

    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")