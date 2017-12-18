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

    dic_termId_docId = {}

    with open("dic_terms.json", 'r') as json_dic_terms:
        dic_terms = json.load(json_dic_terms)

    # initialise dic
    for term in dic_terms:
        dic_termId_docId[dic_terms[term]] = []

    for term in dic_terms:
        for doc in collection.documents:
            if str.lower(term) in doc.tokens:
                dic_termId_docId[dic_terms[term]].append(doc.id)

    with open("dic_termId_docId.json", 'w') as json_termIs_docIs:
        json.dump(dic_termId_docId, json_termIs_docIs)

    t1 = time.time()
    print("Result calculated in " + str(round(t1 - start_time, 2)) + " s")
