import nltk


class Document:
    incremental_id = 0

    def __init__(self, type_block, content):
        self.id = Document.incremental_id
        self.type_block = type_block
        self.content = content
        self.clean_content = ""
        self.tokens = []
        self.vocabulary = set()
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
        self.tokens = nltk.word_tokenize(self.clean_content)
