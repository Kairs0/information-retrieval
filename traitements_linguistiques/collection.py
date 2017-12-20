import json
from nltk.stem import SnowballStemmer
from collections import defaultdict, OrderedDict, Counter
from document import Document


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

    def create_posting_list(self):
        """
        Optimisation Mathieu inspired
        """
        with open("common_words") as file:
            content_common = file.read()
        common_words_list = content_common.split("\n")

        dictionary = defaultdict(int)
        posting_list = defaultdict(list)
        j = 0

        for document in self.documents:
            for token in document.tokens:
                stemmed_word = self.stemmer.stem(token)
                if stemmed_word in common_words_list:
                    continue
                if len(stemmed_word) == 1:
                    continue
                if stemmed_word not in dictionary:
                    termID = j
                    dictionary[stemmed_word] = termID
                    j += 1
                else:
                    termID = dictionary[stemmed_word]
                document.vocabulary.add(stemmed_word)
                posting_list[termID].append(int(document.id))
        
        return posting_list, OrderedDict(sorted(dictionary.items(), key= lambda x:x[0]))            

    def create_docID_index(self, posting_list, dictionary):
        docID_index = defaultdict(list)
        for termID in dictionary.values():
            docID_index[termID] = list(set(posting_list[termID]))
        return docID_index

    def create_docID_index_with_frequency(self, posting_list, dictionary):
        docID_index_with_frequency = defaultdict(list)
        for termID in dictionary.values():
            docID_index_with_frequency[termID] = list(Counter(posting_list[termID]).items())
        return docID_index_with_frequency

    def create_docID_weight(self, docID_index_with_frequency, dictionary):
        docID_weight = defaultdict(float)
        for document in self.documents:
            total_doc_weight = 0 
            for term in document.vocabulary:
                termID = dictionary[term]
                posting_list = Counter({i[0] : i[1] for i in docID_index_with_frequency[termID]})
                #import pdb; pdb.set_trace()
                total_doc_weight += posting_list[document.id]*len(self.documents)/len(posting_list)
            docID_weight[document.id] = total_doc_weight

        return docID_weight


    # OLD    
    def tokenize(self):
        for document in self.documents:
            document.clean_words()
            document.tokenize()

    # OLD  
    def calc_vocabulary(self, common_words_file):
        with open(common_words_file) as file:
            content_common = file.read()

        common_words_list = content_common.split("\n")
        for document in self.documents:
            for token_lowered in set(map(str.lower, document.tokens)):
                if token_lowered not in common_words_list:
                    self.vocabulary.add(token_lowered)

    # OLD  
    def create_dictionary(self):
        dic_term_termId = defaultdict(int)
        terms = self.vocabulary
        for i, term in enumerate(terms):
            dic_term_termId[term] = i
        # generate json file
        with open("dic_terms.json", 'w') as json_file:
            json.dump(dic_term_termId, json_file)

