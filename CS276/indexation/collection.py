from collections import OrderedDict
from nltk.stem import SnowballStemmer

PATH_COMMON_WORDS = r'../collection_data/common_words'

class Collection:

    stemmer = SnowballStemmer("english")

    def __init__(self, path, title):
        self.title = title
        self.path = path

        self.dictionary = OrderedDict()
        self.doc_id_offset = 0

        with open(PATH_COMMON_WORDS) as file:
            content_common = file.read()
        self.common_words_list = list(map(self.stemmer.stem, content_common.split("\n")))

    # def create_block(self, i=None):
    #     from block import Block
    #     block_path = self.path if i is None else f'{self.path}\{i}'
    #     block = Block(self, block_path)
    #     return block

    def create_mpblock(self, i=None):
        from mpblock import MpBlock
        block_path = self.path if i is None else f'{self.path}\{i}'
        block = MpBlock(self, block_path)
        return block
