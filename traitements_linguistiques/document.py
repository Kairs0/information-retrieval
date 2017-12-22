import nltk
from collections import Counter

class Document:
    incremental_id = 1          # L'index des docs commence à 1 (CACM)

    def __init__(self, content):
        self.doc_id = Document.incremental_id
        self.content = content
        self.clean_content = ""
        self.tokens = []
        self.vocabulary = Counter()    # On garde volontairement les doublons
        Document.incremental_id += 1

    def clean_words(self):
        self.clean_content = self.content.replace('.', ' ')\
                .replace('?', ' ') \
                .replace('!', ' ') \
                .replace(',', ' ')\
                .replace(':', ' ')\
                .replace('(', ' ')\
                .replace(')', ' ')\
                .replace('\'', ' ')\
                .replace('[', ' ')\
                .replace(']', ' ')

    def tokenize(self):
        tokens = nltk.word_tokenize(self.content)
        for tok in map(str.lower, tokens):
            self.tokens.append(tok)
