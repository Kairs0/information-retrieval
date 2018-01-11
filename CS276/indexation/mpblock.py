from collections import OrderedDict, Counter, deque
import glob
import string
# import cProfile
import nltk
from nltk.stem import SnowballStemmer

from simplemapreduce import MapDoubleReduce
from mpdocument import Document


class MpBlock(object):

    def __init__(self, collection, path):
        # manager = Manager()
        self.collection = collection
        self.doc_id = self.collection.doc_id_offset

        self.path = path
        self.input_files = glob.glob(f'{path}/*')
        self.posting_list = OrderedDict()
        self.documents = []

    def __str__(self):
        return f'block.Block object. Collection: {self.collection.title}.Path: {self.path}'

    def create_docs(self):
        new_doc_id = self.collection.doc_id_offset
        input_files_tuple = []
        for filename in self.input_files:
            new_doc_id += 1
            new_doc = Document(filename, new_doc_id)
            input_files_tuple.append((new_doc_id, filename))
            self.documents.append(new_doc)
        self.input_files = input_files_tuple
        self.collection.doc_id_offset = new_doc_id

    def create_posting_list(self):
        mapper = MapDoubleReduce(
            self.file_to_words, self.count_words, self.calc_doc_vec, num_workers=4)  # Quad-core
        posting_list, doc_vecs = mapper(self.input_files, chunksize=2450)
        # Finally
        posting_list = self.update_pl_with_id(posting_list)
        doc_vecs = self.update_dv_with_id(doc_vecs)
        self.posting_list = OrderedDict(sorted(posting_list, key=lambda x: x[0]))
        self.update_documents_with_dv(OrderedDict(doc_vecs))


    def file_to_words(self, filetuple):
        """
        Map function
        """
        stemmer = SnowballStemmer("english")
        common_words_list = self.collection.common_words_list.copy()

        TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

        output = deque()

        doc_id, filename = filetuple
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
        return output

    """
    # Map function profile
    def file_to_words_profile(self, filetuple):
        if filetuple[0] % 101 == 0:
            cProfile.runctx('self.file_to_words(filetuple)', globals(), locals(), 'prof%d.prof' %filetuple[0])
        else:
            self.file_to_words(filetuple)
    """


    def count_words(self, item):
        """
        Reduce Function 1
        """
        stemmed_word, doc_id_list = item
        return (stemmed_word, OrderedDict(sorted(Counter(doc_id_list).items(), key=lambda x: x[0])))


    def calc_doc_vec(self, item):
        """
        Reduce Function 2
        """
        doc_id, stemmed_word_list = item
        return (doc_id, OrderedDict(sorted(Counter(stemmed_word_list).items(), key=lambda x: x[0])))

    def update_pl_with_id(self, posting_list):
        """
        We update the incomplete posting list with the term id !
        """
        # Dictionary update here
        dictionary = dict(self.collection.dictionary.copy())
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
        self.collection.dictionary = OrderedDict(sorted(dictionary.items(), key=lambda x: x[0]))
        return posting_list

    def update_dv_with_id(self, doc_vecs):
        """
        We update the documents vectors with the term id !
        """
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
        """
        We update the document instances with their own vectors!
        """
        for i, doc in enumerate(self.documents):
            try:
                doc.vector = doc_vecs[doc.doc_id]
                self.documents[i] = doc
            except KeyError: # The doc has no term (every tokens were thrown away)
                pass
