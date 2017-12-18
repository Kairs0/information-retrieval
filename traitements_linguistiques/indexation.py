import json
from collections import defaultdict
from collection import Collection
from document import Document

if __name__ == "__main__":
    collection = Collection("cacm.all", "cacm")
    collection.calc_documents()
    collection.tokenize()

    
    dic_termId_docId = defaultdict(int)

    for doc in collection.documents:
        # doc.clean_words()
        # doc.tokenize()
        for tok_doc in doc.tokens:
            if
    """

