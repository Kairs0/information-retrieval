from os import listdir
from os.path import isfile, join
from threading import Thread
from collections import defaultdict, OrderedDict
from functools import reduce
import nltk
from document import Document

class DocWorker(Thread):

    def __init__(self, block, doc_path, content):
        Thread.__init__(self)
        self.block = block
        self.doc_path = doc_path
        self.content = content

    def run(self):
        tokens = self.tokenize(self.clean_content(self.content))
        new_doc = Document(self.doc_path,tokens)
        self.block.documents.append(new_doc)

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
                .replace('_',' ')


class Block:

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
            worker_list = []
            for doc_path in document_paths:
                with open(join(self.path, doc_path)) as doc_file:
                    worker_list.append(DocWorker(self, doc_path, doc_file.read()))
                    worker_list[-1].start()

            for worker in worker_list:
                worker.join()


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
        stemmer = self.collection.stemmer
        common_words_list = self.collection.common_words_list

        incremental_id = max(dictionary.values(), default=0) # Initialisation des id term
        for document in self.documents[:1000]:
            doc_id = document.doc_id
            if doc_id % 500 == 0:
                print(f"Document: {doc_id}/", end='')
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


    #
    ########################## MAP REDUCE FUNCTION ALTERNATIVES ####################################
    #

    def tokenize_mp(self):
        list(map(self.get_token, self.documents))

    def get_token(self, document: Document):
        document.clean_words()
        document.tokenize()


    def create_posting_list_mp(self):
        """
        create a dictionary and the posting_list :
        - the dictionary:
            key = (string) term,
            value = (int) term_id
        - the posting-list:
            key = (int) term_id,
            value = (dict) {doc_id : freq} for doc in docs if doc contain term
        """
        docs_pl = map(self.create_doc_pl, self.documents[:1000])

        self.posting_list = reduce(self.merge_doc_pl, docs_pl)


    def create_doc_pl(self, document: Document):
        doc_id = document.doc_id
        doc_posting_list = defaultdict(dict)
        if doc_id % 100 == 0:
            print("Document: ", doc_id)
        for token in document.tokens:
            # On nettoye le token
            if len(token) == 1:
                continue
            stemmed_word = self.collection.stemmer.stem(token)
            if stemmed_word in self.collection.common_words_list:
                continue

            # On traite le token nettoyé
            if stemmed_word not in self.collection.dictionary:
                # On l'ajoute au dictionary
                term_id = max(self.collection.dictionary.values(), default=-1)+1
                self.collection.dictionary[stemmed_word] = term_id
            else:
                # On récupère son id
                term_id = self.collection.dictionary[stemmed_word]
            # On màj la posting-list
            try:
                doc_posting_list[term_id][doc_id] += 1      # Incrément d'une key existante
            except KeyError:
                doc_posting_list[term_id][doc_id] = 1
            # Enfin on ajoute le term au voc du doc pour calculer son poids après
            document.vocabulary[term_id] += 1
        return doc_posting_list

    def merge_doc_pl(self, doc_pl0: defaultdict(dict), doc_pl1: defaultdict(dict)):
        for k in doc_pl1:
            try:
                doc_pl0[k] = {**doc_pl0[k], **doc_pl1[k]}
            except KeyError:
                doc_pl0[k] = doc_pl1[k]
        return doc_pl0



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
            worker_list.append(DocWorker(self, None, doc_content))
            worker_list[-1].start()

        for worker in worker_list:
            worker.join()
