from block import *


class Collection:

    def __init__(self, path_file, title):
        self.title = title
        self.path = path_file
        self.blocks = []
        self.tokens = []
        self.vocabulary = []
        with open(self.path) as f:
            self.content = f.read()

    def calc_blocks(self):
        data = self.content.split("\n.")
        for data_block in data:
            if data_block[0] == 'T' or data_block[0] == 'W' or data_block[0] == 'K':
                new_block = Block(data_block[0], data_block)
                self.blocks.append(new_block)

    def tokenize(self):
        for block in self.blocks:
            block.clean_words()
            block.tokenize()
            self.tokens += block.tokens

    def calc_vocabulary(self, common_words_file):
        with open(common_words_file) as f:
            content_common = f.read()

        common_words_list = content_common.split("\n")
        for block in self.blocks:
            block.calc_vocabulary(common_words_list)
            self.vocabulary += block.vocabulary
