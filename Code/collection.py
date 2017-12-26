from collections import defaultdict
from nltk.stem import SnowballStemmer

class Collection:

    stemmer = SnowballStemmer("english")

    def __init__(self, path, title):
        self.title = title
        self.path = path
        self.blocks = []

        self.dictionary = defaultdict(int)
        self.doc_id_offset = 0

        with open(r'.\collection_data\CACM\common_words') as file:
            content_common = file.read()
        self.common_words_list = list(map(self.stemmer.stem, content_common.split("\n")))

    def create_block(self, i=None):
        from block import Block
        block_path = self.path if i is None else f'{self.path}\{i}'
        block = Block(self, block_path)
        self.blocks.append(block)
        return block

    def create_mpblock(self, i=None, manager=None):
        from mpblock import MpBlock
        block_path = self.path if i is None else f'{self.path}\{i}'
        block = MpBlock(self, block_path, manager)
        self.blocks.append(block)
        return block
