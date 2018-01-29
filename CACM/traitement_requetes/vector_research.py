import math
from collections import Counter, OrderedDict, deque
import string
import nltk

PATH_COMMON_WORDS = r'../collection_data/common_words'
STEMMER = nltk.stem.SnowballStemmer("english")
with open(PATH_COMMON_WORDS) as COMMON_WORDS_FILE:
    COMMON_WORDS_LIST = list(map(STEMMER.stem, COMMON_WORDS_FILE.read().split("\n")))

def calc_balanced_weight(number_occurrence_term_in_doc, number_docs, number_docs_with_term):
    if number_occurrence_term_in_doc == 0:
        return 0
    else:
        log_freq = math.log(number_occurrence_term_in_doc, 10)
        term_inverse_frequency = math.log(number_docs/number_docs_with_term, 10)
        return round((1 + log_freq)*term_inverse_frequency, 10)

def query_to_words(query):
    """
    Same treatements on the query as on the tokens in the collection
    """
    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

    output = deque()
    query = query.translate(TR) # Strip punctuation
    for token in nltk.word_tokenize(query):
        token = token.lower()
        if len(token) == 1:
            continue
        stemmed_word = STEMMER.stem(token)
        if stemmed_word in COMMON_WORDS_LIST:
            continue
        output.append(stemmed_word)
    return output


def process_query_v2(query, dictionary, inverse_index_freq, list_doc_weight, number_doc_expected):
    """
    Same implementation mais avec pondération de la fréquence
    https://en.wikipedia.org/wiki/Vector_space_model#Example:_tf-idf_weights
    """
    dictionary = OrderedDict(dictionary)
    query_words = Counter(query_to_words(query))

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

    return scores.most_common(number_doc_expected)


def process_query(query, dictionary, inverse_index_freq, list_doc_weight, number_doc_expected):
    """
    Made by Silvestre, pondération hasardeuse
    """
    dictionary = OrderedDict(dictionary)
    number_docs = len(list_doc_weight)
    query_words = Counter(query_to_words(query))
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
        scores[doc_id] = scores[doc_id]/(doc_weight*request_weight)

    return scores.most_common(number_doc_expected)
