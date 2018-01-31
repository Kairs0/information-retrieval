import json
import sys
import getopt
import timeit
import boolean_research
import vector_research

PATH_COLLECTION = r'../collection_data/cacm.all'
PATH_DICTIONARY = r'../fichiers_traitements/dictionary.json'
PATH_SIMPLE_POSTING_LIST = r'../fichiers_traitements/simple_posting_list.json'
PATH_COMPLETE_POSTING_LIST = r'../fichiers_traitements/complete_posting_list.json'
PATH_LIST_DOC_WEIGHT = r'../fichiers_traitements/list_doc_weight.json'
PATH_QUERY_LIST = r'../collection_data/query.text'
PATH_RESULT_LIST = r'../collection_data/qrels.text'


def print_usage():
    """
    prints the proper command usage
    """
    print("usage: " + sys.argv[0] + " -m model | -t")
    print("Options and arguments:")
    print("-m --model\t: research model. ['b','boolean', 'v', 'vector', 'v2', 'vector v2']")
    print("-t\t\t: enable the time record.")
    print("-e --eval\t: run evalution.")


def research(search_type, query_string):
    """
    Basic search function, called with a search_type (or search model)
    and a query (a string).
    """
    if search_type == 'b' or search_type == "boolean":
        return boolean_research.process_query(query_string, dictionary, simple_posting_list, doc_id_list)
    elif search_type == 'v2':
        return vector_research.process_query_v2(query_string, dictionary, posting_list, list_doc_weight)
    elif search_type == 'v' or search_type == "vector":
        return vector_research.process_query(query_string, dictionary, posting_list, list_doc_weight)

def calc_queries():
    """
    Return the list of the test queries.
    The differents blocks are concatenated.
    """
    with open(PATH_QUERY_LIST, "r") as file:
        content = file.read()
    raw_queries = content.split("\n.I")

    query_list = []
    for raw_query in raw_queries:
        query = ""
        blocks = raw_query.split("\n.")
        for block in blocks:
            query += block[1:]
        query_list.append(query)

    return query_list

def calc_result():
    """
    Return the dict of the test queries' result.
    k, v = query_id, [(results)]
    The differents results for a query form a list.
    """
    with open(PATH_RESULT_LIST, "r") as file:
        content = file.read() #TODO Faire le parsing de ce fichier
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
    Fonction utiliser pour évalution notre modèle de recherche de documents !
    """
    query_list = calc_queries()
    results = calc_result()

    for i, query in enumerate(query_list):
        print(research(search_type, query))
        try:
            print(results[i+1])
        except KeyError:
            print("No results known.")

if __name__ == "__main__":
    RECORD_TIME = False
    SEARCH_TYPE = None
    EVALUATION = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'm:te', ['model', 't', 'eval'])
        # import pdb; pdb.set_trace()

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

    with open(PATH_SIMPLE_POSTING_LIST, "r") as f2:
        simple_posting_list = json.load(f2)

    with open(PATH_COMPLETE_POSTING_LIST, "r") as f3:
        posting_list = json.load(f3)

    with open(PATH_LIST_DOC_WEIGHT, "r") as f4:
        list_doc_weight = json.load(f4)

    doc_id_list = list_doc_weight.keys()

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
