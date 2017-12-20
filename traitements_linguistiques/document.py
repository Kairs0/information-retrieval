import nltk


class Document:
    incremental_id = 0

    def __init__(self, content):
        self.id = Document.incremental_id
        self.content = content
        self.clean_content = ""
        self.tokens = []
        self.vocabulary = [] # On garde volontairement les doublons
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
        tokens = nltk.word_tokenize(self.clean_content)
        for tok in map(str.lower, tokens):
            self.tokens.append(tok)
