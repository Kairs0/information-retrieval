from collections import defaultdict, OrderedDict, Counter
from nltk.stem import SnowballStemmer
from document import Document

PATH_COMMON_WORDS = r'../collection_data/common_words'
LOWER_LENGTH_FOR_WORD = 1


class Collection:

    stemmer = SnowballStemmer("english")

    def __init__(self, path_file, title):
        self.title = title
        self.path = path_file
        self.documents = []
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
        for document in self.documents:
            document.clean_words()
            document.tokenize()

    # https://stackoverflow.com/a/18262662
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
        with open(PATH_COMMON_WORDS) as file:
            content_common = file.read()
        common_words_list = list(map(self.stemmer.stem, content_common.split("\n")))

        dictionary = defaultdict(int)
        posting_list = defaultdict(dict)

        incremental_id = 0  # Initialisation des id term
        for document in self.documents:
            doc_id = document.doc_id
            for token in document.tokens:
                # On passe à la prochaine itération
                # de la boucle s'il s'agit d'un unique
                # caractère ou s'il est dans la liste des mot communs
                if len(token) == LOWER_LENGTH_FOR_WORD:
                    continue
                stemmed_word = self.stemmer.stem(token)
                if stemmed_word in common_words_list:
                    continue

                # On traite le token nettoyé
                if stemmed_word not in dictionary:
                    # On l'ajoute au dictionary
                    term_id = incremental_id
                    incremental_id += 1
                    dictionary[stemmed_word] = term_id
                else:
                    # On récupère son id
                    term_id = dictionary[stemmed_word]
                # On màj la posting-list
                if doc_id not in posting_list[term_id].keys():
                    posting_list[term_id][doc_id] = 1        # Ajout d'un nouveau docID
                else:
                    posting_list[term_id][doc_id] += 1      # Incrément de la frequence d'un doc
                # Enfin on ajoute le term au voc du doc pour calculer son poids après
                document.vocabulary[term_id] += 1

        return posting_list, OrderedDict(sorted(dictionary.items(), key=lambda x: x[0]))

    """
    From the posting list, creates the inverted index :
    key = (int) termID
    value = (list) (int) docs ids 
    """
    @staticmethod
    def create_inverted_index(posting_list):
        inverted_index = defaultdict(list)
        for term_id in posting_list.keys():
            documents_ids = list(posting_list[term_id].keys())
            inverted_index[term_id] = documents_ids
        return inverted_index

    """
    TODO Arnaud comprendre utilité
    From the inverted index, creates a weighted index for each doc (?)
    key: (int) 
    """
    def create_docID_weight(self, inverted_index):
        # We may need to use the Decimal type
        docID_weight = defaultdict(float)
        for document in self.documents:
            total_doc_weight = 0
            # Calculates a weight for each document.
            #
            for term_id, freq in document.vocabulary.items():
                total_doc_weight += freq*len(self.documents)/len(inverted_index[term_id])
            docID_weight[document.doc_id] = float("{0:.2f}".format(total_doc_weight))

        return docID_weight

    # USED in question_prealables
    def calc_vocabulary(self, common_words_file):
        with open(common_words_file) as file:
            content_common = file.read()

        common_words_list = content_common.split("\n")
        for document in self.documents:
            for token_lowered in set(map(str.lower, document.tokens)):
                if token_lowered not in common_words_list:
                    self.vocabulary.add(token_lowered)
