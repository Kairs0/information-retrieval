from collections import OrderedDict, Counter, deque
import glob
import string
import ctypes
import nltk
from nltk.stem import SnowballStemmer

from simplemapreduce import MapDoubleReduce
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


    def create_posting_list(self):
        mapper = MapDoubleReduce(
            self.file_to_words, self.count_words, self.calc_doc_vec, num_workers=None)
        posting_list, doc_vecs = mapper(self.input_files, chunksize=2500) # Quad-core assumed
        # Finally
        posting_list = self.update_pl_with_id(posting_list)
        doc_vecs = self.update_dv_with_id(doc_vecs)
        self.posting_list = OrderedDict(sorted(posting_list, key=lambda x: x[0]))
        self.update_documents_with_dv(OrderedDict(doc_vecs))
        self.collection.doc_id_offset = int(self.doc_id.value)

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

    def calc_doc_vec(self, item):
        doc_id, stemmed_word_list = item
        return (doc_id, OrderedDict(sorted(Counter(stemmed_word_list).items(), key=lambda x: x[0])))

    def update_pl_with_id(self, posting_list):
        # Dictionary update here
        dictionary = self.collection.dictionary
        incremental_id = max(dictionary.values(), default=0)
        for i, pl_tuple in enumerate(posting_list):
            word = pl_tuple[0]
            try:
                word_id = dictionary[word]
            except KeyError:
                word_id = incremental_id
                incremental_id += 1
                dictionary[word] = word_id
            posting_list[i] = (word_id, pl_tuple[1])
        return posting_list
    
    def update_dv_with_id(self, doc_vecs):
        # Dictionary update here
        dictionary = self.collection.dictionary
        for i, dv_tuple in enumerate(doc_vecs):
            doc_id = dv_tuple[0]
            words_counter = dv_tuple[1]
            id_counter = Counter()
            for word in words_counter:
                word_id = dictionary[word]
                id_counter[word_id] = words_counter[word]
            doc_vecs[i] = (doc_id, id_counter)
        return doc_vecs

    def update_documents_with_dv(self, doc_vecs):
        for i, doc in enumerate(self.documents):
            try:
                doc.vector = doc_vecs[doc.doc_id]
                self.documents[i] = doc
            except KeyError: # The doc has no term (every tokens were thrown away)
                pass
