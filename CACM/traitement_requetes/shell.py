import json
import sys
import getopt
import timeit
import boolean_research
import vector_research
import matplotlib.pyplot as plot

PATH_COLLECTION = r'../collection_data/cacm.all'
PATH_DICTIONARY = r'../fichiers_traitements/dictionary.json'
PATH_COMPLETE_POSTING_LIST = r'../fichiers_traitements/posting_list.json'
PATH_LIST_DOC_WEIGHT = r'../fichiers_traitements/list_doc_weight.json'
PATH_QUERY_LIST = r'../collection_data/query.text'
PATH_RESULT_LIST = r'../collection_data/qrels.text'


def print_usage():
    """
    prints the proper command usage
    """
    print("usage: " + sys.argv[0] + " -m model | -t")
    print("Options and arguments:")
    print("-m --model\t: research model. ['b','boolean', 'v', 'vector']")
    print("-t\t\t: enable the time record.")
    print("-e --eval\t: run evalution.")


def research(search_type, query_string, number_doc_expected=3):
    """
    Basic search function, called with a search_type (or search model)
    and a query (a string).
    """
    if search_type == 'b' or search_type == "boolean":
        return boolean_research.process_query(query_string, dictionary, posting_list, doc_id_list)
    elif search_type == 'v' or search_type == "vector":
        return vector_research.process_query(query_string, dictionary, posting_list, doc_weights, number_doc_expected)


def calc_queries():
    """
    Return the list of the test queries.
    For each query, the different blocks are concatenated.
    """
    with open(PATH_QUERY_LIST, "r") as file:
        content = file.read()

    unclean_queries = content.split(".I")
    query_list = []
    for unclean_query in unclean_queries:
        if not unclean_query.__contains__(".W"):
            continue

        unclean_blocks = unclean_query.split(".N")
        clear_first_part = unclean_blocks[0].replace(".W\n", "")[3:]
        clear_second_part = unclean_blocks[1][4:]
        query_list.append(
            "".join([clear_first_part.replace("\n", " "),
                     clear_second_part.replace("\n", " ")])
        )

    return query_list


def calc_result():
    """
    Return the dict of the test queries' result.
    k, v = query_id, [(results)]
    The differents results for a query form a list.
    """
    with open(PATH_RESULT_LIST, "r") as file:
        content = file.read()
    raw_results = content.split("\n")

    results = {}
    for raw_result in raw_results:
        elems = raw_result.split()
        if int(elems[0]) not in results:
            results[int(elems[0])] = [int(elems[1])]
        else:
            results[int(elems[0])].append(int(elems[1]))
    return results


def evaluate(search_type):
    """
    Fonction utilisée pour l'évalution de notre modèle de recherche de documents
    """
    query_list = calc_queries()
    results = calc_result()

    eval_recall = []
    eval_precision = []

    count_key_error = 0
    count_division_error = 0

    for i, query in enumerate(query_list):
        try:
            expected = results[i + 1]

            if search_type == "b" or search_type == "boolean":
                query_words = query.split(' ')
                clean_words_query = []
                for word in query_words:
                    if word != ' ' and word != '':
                        clean_words_query.append(word.replace(' ', '')
                                                        .replace('.', '')
                                                        .replace('?', '') \
                                                        .replace('!', '') \
                                                        .replace(',', '')\
                                                        .replace(':', '')\
                                                        .replace('(', '')\
                                                        .replace(')', '')\
                                                        .replace('\'', '')\
                                                        .replace('[', '')\
                                                        .replace(']', ''))

                query_and = " AND ".join([word for word in clean_words_query if word != ''])

                result_request = research(search_type, query_and)
            else:
                result_request = research(search_type, query, 3)

            relevant_obtained = []
            for result in result_request:
                index = int(result[0])
                if index in expected:
                    relevant_obtained.append(int(result[0]))

            recall = len(relevant_obtained) / len(expected)
            precision = len(relevant_obtained) / len(result_request)

            eval_recall.append(recall)
            eval_precision.append(precision)
            # print("Recall for this request: " + str(recall))
            # print("Precision for this request: " + str(precision))

        except KeyError:
            # print("No relevant result has been provided for this request.")
            count_key_error += 1
        except ZeroDivisionError:
            # print("No result obtained for this request")
            count_division_error += 1

    # print("Values eval_recall:")
    # print(sorted(eval_recall)[::-1])
    #
    # print("Values eval_precision:")
    # print(sorted(eval_precision)[::-1])

    plot.plot(
        sorted([value for value in eval_recall if value != 0])[::-1],
        sorted([value for value in eval_precision if value != 0])
    )
    plot.xlabel('Recall')
    plot.ylabel('Precision')
    plot.title('Recall/Precision graph')
    plot.show()


if __name__ == "__main__":
    RECORD_TIME = False
    SEARCH_TYPE = None
    EVALUATION = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'm:te', ['model', 't', 'eval'])

    # if illegal arguments, print usage to user and exit
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)

    # for each option parsed
    for o, a in opts:
        if o == '-m' or o == '--model':
            SEARCH_TYPE = a
        elif o == '-t' or o == '--time':
            RECORD_TIME = True
        elif o == '-e' or o == '--eval':
            EVALUATION = True
        else:
            assert False, "unhandled option"

    if SEARCH_TYPE is None:
        print_usage()
        sys.exit(2)

    shell_open = True
    print("Loading indexes...", end="")

    with open(PATH_DICTIONARY, "r") as f:
        dictionary = json.load(f)

    with open(PATH_COMPLETE_POSTING_LIST, "r") as f3:
        posting_list = json.load(f3)

    with open(PATH_LIST_DOC_WEIGHT, "r") as f4:
        doc_weights = json.load(f4)

    doc_id_list = doc_weights.keys()

    print(" => Indexes loaded.")

    while shell_open:

        if EVALUATION:
            print(f"Evaluation of the model {SEARCH_TYPE}")
            evaluate(SEARCH_TYPE)
            shell_open = False
            continue

        str_query = input(">> ")
        if str_query == "exit()":
            shell_open = False
        else:
            if RECORD_TIME:
                start = timeit.default_timer()                                      # start time

            print(research(SEARCH_TYPE, str_query))

            if RECORD_TIME:
                stop = timeit.default_timer()                                       # stop time
            if RECORD_TIME:
                print('Query time: ' + str(round(stop - start, 3) * 10e3) + ' ms')  # print time
