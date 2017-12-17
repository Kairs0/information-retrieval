from block import Block


class Collection:

    def __init__(self, path_file, title):
        self.title = title
        self.path = path_file
        self.blocks = []
        self.tokens = []
        self.vocabulary = set()
        with open(self.path) as file:
            self.content = file.read()

    def calc_blocks(self):
        data = self.content.split("\n.")
        for data_block in data:
            if data_block[0] == 'T' or data_block[0] == 'W' or data_block[0] == 'K':
                new_block = Block(data_block[0], data_block[1:])
                self.blocks.append(new_block)

    def tokenize(self):
        for block in self.blocks:
            block.clean_words()
            block.tokenize()
            self.tokens += block.tokens

    def calc_vocabulary(self, common_words_file):
        with open(common_words_file) as file:
            content_common = file.read()

        common_words_list = content_common.split("\n")
        for token_lowered in set(map(str.lower, self.tokens)):
            if token_lowered not in common_words_list:
                self.vocabulary.add(token_lowered)