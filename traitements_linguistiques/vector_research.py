"""
TO-DO
"""
from collections import Counter, OrderedDict
import nltk

def process_query(query, dictionary, inverse_index_freq, list_doc_weight):
    dictionary = OrderedDict(dictionary)

    docs_number = len(list_doc_weight)
    stemmer = nltk.stem.SnowballStemmer("english") # instantiate stemmer
    words = Counter(map(stemmer.stem, query.split()))
    request_weight = sum(words.values())

    relevant_doc_list = set() # List of all documents containing at least one word of the search
    scores = Counter()

    for word, freq in words.items():
        term_request_weight = freq
        try:
            word_id = dictionary[word]
            # import pdb; pdb.set_trace()
            posting_list = Counter(inverse_index_freq[str(word_id)])
            for doc_id in posting_list:
                relevant_doc_list.add(doc_id)
                term_frequency = posting_list[str(doc_id)]
                term_doc_weight = term_frequency*(docs_number/len(posting_list))
                scores[doc_id] += term_request_weight * term_doc_weight
                # print("word_id: ", word_id, "doc_id: ", doc_id, ", score: ", scores[doc_id])
        except KeyError:
            pass

    for doc_id in relevant_doc_list:
        doc_weight = list_doc_weight[str(doc_id)]
        # import pdb; pdb.set_trace()
        # print("doc_id: ", doc_id, ", nn_score: ", scores[doc_id], ", doc_weight: ", doc_weight)
        scores[doc_id] = scores[doc_id]/(doc_weight*request_weight)

    return scores.most_common(3)
