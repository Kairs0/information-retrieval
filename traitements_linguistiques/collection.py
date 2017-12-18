from document import Document


class Collection:

    def __init__(self, path_file, title):
        self.title = title
        self.path = path_file
        self.documents = []
        self.tokens = []
        self.vocabulary = set()
        with open(self.path) as file:
            self.content = file.read()

    def calc_documents(self):
        documents = self.content.split("\n.I")

        for document in documents:
            blocks = document.split("\n.")
            doc_content = ""
            for block in blocks:
                if block[0] == 'T' or block[0] == 'W' or block[0] == 'K':
                    doc_content += block[1:]
            new_doc = Document(doc_content)
            self.documents.append(new_doc)

    def tokenize(self):
        
        for block in self.documents:
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
