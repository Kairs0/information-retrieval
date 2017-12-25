from collections import defaultdict
from nltk.stem import SnowballStemmer


class Collection:
    
    stemmer = SnowballStemmer("english")
    

    def __init__(self, path, title):
        self.title = title
        self.path = path
        self.blocks = []

        self.dictionary = defaultdict(int)

        with open(r'.\collection_data\CACM\common_words') as file:
            content_common = file.read()
        self.common_words_list = list(map(self.stemmer.stem, content_common.split("\n")))


    def create_block(self, i=None):
        from block import Block
        block_path = self.path if i is None else f'{self.path}\{i}'
        block = Block(self, block_path)
        self.blocks.append(block)
        return block
