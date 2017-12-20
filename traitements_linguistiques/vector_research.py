"""
TO-DO
"""
from collections import defaultdict, Counter, OrderedDict
import nltk

def process_query(query, dictionary, docID_index_with_frequency, docID_weight):
    docs_number = len(docID_weight)
    stemmer = nltk.stem.SnowballStemmer("english") # instantiate stemmer
    words = Counter(map(stemmer.stem, query.split()))
    request_weight = sum(words.values())

    scores = Counter()

    for word, freq in words.items():
        term_request_weight = freq
        try:
            wordID = dictionary[word]
            posting_list = Counter({i[0] : i[1] for i in docID_index_with_frequency[str(wordID)]})
            for docID in docID_weight:
                term_frequency = posting_list[int(docID)]
                #import pdb; pdb.set_trace()
                term_doc_weight = term_frequency*(docs_number/len(posting_list))
                scores[docID] += term_request_weight * term_doc_weight
        except KeyError:
            pass

    for docID, doc_weight in docID_weight.items():
        if doc_weight == 0:
            import pdb; pdb.set_trace()
        scores[docID] = scores[docID]/(doc_weight*request_weight)

    return scores.most_common(3)
