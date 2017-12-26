from os import listdir
from os.path import isfile, join
from collections import defaultdict, OrderedDict
import nltk
from nltk.stem import SnowballStemmer

from document import Document

class Block(object):

    def __init__(self, collection, path):
        self.collection = collection
        self.path = path
        self.documents = []
        self.posting_list = OrderedDict()

    def __str__(self):
        return f'block.Block object. Collection: {self.collection.title}.Path: {self.path}'

    def get_documents(self):
        self.documents.clear()
        if isfile(self.path):
            with open(self.path, "r") as file:
                self.get_documents_cacm(file.read())
        else:
            document_paths = [f for f in listdir(self.path) if isfile(join(self.path, f))]
            for doc_path in document_paths:
                with open(join(self.path, doc_path)) as doc_file:
                    content = doc_file.read()
                    tokens = self.tokenize(self.clean_content(content))
                    new_doc = Document(doc_path, tokens)
                    self.documents.append(new_doc)

    def tokenize(self, content):
        tokens = nltk.word_tokenize(content)
        return list(map(str.lower, tokens))

    def clean_content(self, content):
        return content.replace('.', ' ')\
                .replace('?', ' ') \
                .replace('!', ' ') \
                .replace(',', ' ')\
                .replace(':', ' ')\
                .replace('(', ' ')\
                .replace(')', ' ')\
                .replace('\'', ' ')\
                .replace('[', ' ')\
                .replace(']', ' ')\
                .replace('_', ' ')

    def create_posting_list(self):
        """
        create a dictionary and the posting_list :
        - the dictionary:
            key = (string) term,
            value = (int) term_id
        - the posting-list:
            key = (int) term_id,
            value = (dict) {doc_id : freq} for doc in docs if doc contain term
        """
        self.posting_list.clear()
        dictionary = self.collection.dictionary
        posting_list = defaultdict(dict)
        stemmer = SnowballStemmer("english")
        common_words_list = self.collection.common_words_list

        incremental_id = max(dictionary.values(), default=0) # Initialisation des id term
        for document in self.documents:
            doc_id = document.doc_id
            for token in document.tokens:
                # On nettoye le token
                if len(token) == 1:
                    continue
                stemmed_word = stemmer.stem(token)
                if stemmed_word in common_words_list:
                    continue

                # On traite le token nettoyé
                if stemmed_word in dictionary:
                    # On récupère son id
                    term_id = dictionary[stemmed_word]
                else:
                    # On l'ajoute au dictionary
                    term_id = incremental_id
                    incremental_id += 1
                    dictionary[stemmed_word] = term_id
                # On màj la posting-list
                try:
                    posting_list[term_id][doc_id] += 1      # Incrément d'une key existante
                except KeyError:
                    posting_list[term_id][doc_id] = 1
                # Enfin on ajoute le term au voc du doc pour calculer son poids après
                document.vocabulary[term_id] += 1

        self.posting_list = OrderedDict(sorted(posting_list.items(), key=lambda x: x[0]))


    def create_docID_index(self, posting_list):
        docID_index = defaultdict(list)
        for term_id in posting_list.keys():
            docID_index[term_id] = list(posting_list[term_id].keys())
        return docID_index


    def create_docID_weight(self, docID_index):
        # We may need to use the Decimal type
        docID_weight = defaultdict(float)
        for document in self.documents:
            total_doc_weight = 0
            for term_id, freq in document.vocabulary.items():
                total_doc_weight += freq*len(self.documents)/len(docID_index[term_id])
            docID_weight[document.doc_id] = float("{0:.2f}".format(total_doc_weight))

        return docID_weight


    # Only used for CACM
    def get_documents_cacm(self, content):
        documents = content.split("\n.I")

        worker_list = []
        for document in documents:
            blocks = document.split("\n.")
            doc_content = ""
            for block in blocks:
                if block[0] == 'T' or block[0] == 'W' or block[0] == 'K':
                    doc_content += block[1:]
            tokens = self.tokenize(self.clean_content(doc_content))
            new_doc = Document(None, tokens)
            self.documents.append(new_doc)

        for worker in worker_list:
            worker.join()
