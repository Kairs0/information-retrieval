from collections import OrderedDict, Counter, deque
import glob
import string
import ctypes
import nltk
from nltk.stem import SnowballStemmer

from simplemapreduce import SimpleMapReduce
from mpdocument import Document

class MpBlock(object):

    def __init__(self, collection, path, manager):
        # manager = Manager()
        self.collection = collection
        self.documents = manager.list()
        self.doc_id = manager.Value(ctypes.c_uint, self.collection.doc_id_offset)
        self.lock = manager.RLock()

        self.path = path
        self.input_files = glob.glob(f'{path}/*')
        self.posting_list = OrderedDict()

    def __str__(self):
        return f'block.Block object. Collection: {self.collection.title}.Path: {self.path}'

    def file_to_words(self, filename):
        stemmer = SnowballStemmer("english")
        common_words_list = self.collection.common_words_list.copy()
        with self.lock:
            self.doc_id.value += 1
            doc_id = int(self.doc_id.value)

        TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

        output = deque()

        with open(filename, 'rt') as file:
            content = file.read()
            content = content.translate(TR) # Strip punctuation
            for token in nltk.word_tokenize(content):
                token = token.lower()
                if len(token) == 1:
                    continue
                stemmed_word = stemmer.stem(token)
                if stemmed_word in common_words_list:
                    continue
                output.append((stemmed_word, doc_id))

        doc = Document(filename, doc_id) # Faire calcul weight ici
        self.documents.append(doc)
        return output

    def count_words(self, item):
        stemmed_word, doc_id_list = item
        return (stemmed_word, OrderedDict(sorted(Counter(doc_id_list).items(), key=lambda x: x[0])))

    def create_posting_list(self):
        mapper = SimpleMapReduce(self.file_to_words, self.count_words, num_workers=None)
        posting_list = mapper(self.input_files, chunksize=2500)
        # posting_list.sort(key=operator.itemgetter(0))
        self.update_with_id(posting_list)

        # Finally
        self.posting_list = OrderedDict(posting_list)
        self.collection.doc_id_offset = int(self.doc_id.value)

    def update_with_id(self, posting_list):
        # Dictionary update here
        dictionary = self.collection.dictionary
        offset = max(dictionary.values(), default=-1)
        for i, pl_tuple in enumerate(posting_list):
            word = pl_tuple[0]
            try:
                term_id = dictionary[word]
            except KeyError:
                offset += 1
                term_id = offset
                self.collection.dictionary[word] = term_id
            posting_list[i] = (term_id, pl_tuple[1])
