from collections import Counter, OrderedDict
import nltk
import math


def calc_balanced_weight(number_occurrence_term_in_doc, number_docs=0, number_docs_with_term=0):
    if number_occurrence_term_in_doc == 0:
        return 0
    else:
        log_freq = math.log(number_occurrence_term_in_doc, 10)
        term_inverse_frequency = math.log(number_docs/number_docs_with_term, 10)
        return round((1 + log_freq)*term_inverse_frequency, 10)


def process_query_v2(query, dictionary, inverse_index_freq, list_doc_weight):
    """
    Same implementation mais avec pondération de la fréquence
    https://en.wikipedia.org/wiki/Vector_space_model#Example:_tf-idf_weights
    """
    dictionary = OrderedDict(dictionary)
    stemmer = nltk.stem.SnowballStemmer("english")  # instantiate stemmer
    query_words = Counter(map(stemmer.stem, query.split()))

    relevant_doc_list = set()
    scores = Counter()

    number_docs_total = len(list_doc_weight)

    request_weight = 0

    for word, freq in query_words.items():
        try:
            id_word = dictionary[word]
            documents_freq_for_term = Counter(inverse_index_freq[str(id_word)])
            nbr_docs_with_terms = len(documents_freq_for_term)
            weight_term_request = calc_balanced_weight(freq, number_docs_total, nbr_docs_with_terms)
            request_weight += weight_term_request
            for doc_id in documents_freq_for_term:
                relevant_doc_list.add(doc_id)
                scores[doc_id] += \
                    calc_balanced_weight(documents_freq_for_term[doc_id], number_docs_total, nbr_docs_with_terms) * \
                    weight_term_request
        except KeyError:
            pass

    for doc_id in relevant_doc_list:
        doc_weight = list_doc_weight[str(doc_id)]
        scores[doc_id] = scores[doc_id]/(doc_weight*request_weight)

    return scores.most_common(3)


def process_query(query, dictionary, inverse_index_freq, list_doc_weight):
    dictionary = OrderedDict(dictionary)
    number_docs = len(list_doc_weight)
    stemmer = nltk.stem.SnowballStemmer("english")  # instantiate stemmer
    query_words = Counter(map(stemmer.stem, query.split()))
    request_weight = sum(query_words.values())

    relevant_doc_list = set()  # List of all documents containing at least one word of the search
    scores = Counter()

    for word, freq in query_words.items():
        term_request_weight = freq
        try:
            word_id = dictionary[word]
            # import pdb; pdb.set_trace()
            documents_freq_for_term = Counter(inverse_index_freq[str(word_id)])
            for doc_id in documents_freq_for_term:
                relevant_doc_list.add(doc_id)
                term_frequency_in_doc = documents_freq_for_term[str(doc_id)]
                number_docs_in_which_term_appears = len(documents_freq_for_term)
                term_doc_weight = term_frequency_in_doc*(number_docs/number_docs_in_which_term_appears)
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

def process_query_useless(query, dictionary, inverse_index_freq):
    """
    Implementation (plus simple) algo stanford.edu sans utiliser le poids total
    https://nlp.stanford.edu/IR-book/html/htmledition/computing-vector-scores-1.html
    Avec pondération tf-idf pour le poids au lieu de la somme du nb d'occurence
    """
    dictionary = OrderedDict(dictionary)
    scores = Counter()
    stemmer = nltk.stem.SnowballStemmer("english")
    query_words = Counter(map(stemmer.stem, query.split()))
    request_weight = sum(map(calc_balanced_weight, query_words.values()))
    for term in query_words:
        try:
            term_id = dictionary[term]
            posting_list = inverse_index_freq[str(term_id)]
            for doc_id, freq in posting_list.items():
                scores[doc_id] += calc_balanced_weight(int(freq)) * request_weight
        except KeyError:
            # case user searches for a word not present in our collection
            pass

    doc_numbers = len(scores)
    for document in scores:
        scores[document] = scores[document] / doc_numbers

    return scores.most_common(3)