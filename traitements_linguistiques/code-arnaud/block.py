import nltk


class Block:

    def __init__(self, type_block, content):
        self.type_block = type_block
        self.content = content
        self.clean_content = ""
        self.tokens = []
        self.vocabulary = []

    def clean_words(self):
        self.clean_content = self.content.replace('.', ' ')\
                .replace(',', ' ')\
                .replace(':', ' ')\
                .replace('(', ' ')\
                .replace(')', ' ')\
                .replace('\'', ' ')\
                .replace('[', ' ')\
                .replace(']', ' ')

    def tokenize(self):
        self.tokens = nltk.word_tokenize(self.clean_content)

    def calc_vocabulary(self, common_words):
        stopwords = set(common_words)

        for word in self.tokens:
            if word not in stopwords:
                self.vocabulary.append(word.lower())
