from collections import Counter

class Document:
    incremental_id = 1          # L'index des docs commence à 1 (CACM)

    def __init__(self, name, tokens):
        self.doc_id = Document.incremental_id
        self.name = name
        self.tokens = tokens
        self.vocabulary = Counter()    
        Document.incremental_id += 1
