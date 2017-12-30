import json
import sys
import getopt
import timeit
import boolean_research
import vector_research

PATH_COLLECTION = r'..\collection_data\cacm.all'
PATH_DICTIONARY = r'..\fichiers_traitements\dictionary.json'
PATH_INVERSE_INDEX_SIMPLE = r'..\fichiers_traitements\inverse_index_simple.json'
PATH_INVERSE_INDEX_FREQ = r'..\fichiers_traitements\inverse_index_freq.json'
PATH_LIST_DOC_WEIGHT = r'..\fichiers_traitements\list_doc_weight.json'


def print_usage():
    """
    prints the proper command usage
    """
    print("usage: " + sys.argv[0] + " -m model | -t")
    print("Options and arguments:")
    print("-m --model\t: research model chosen for the search. ['b','boolean', 'v', 'vector', 'v2', 'vector v2']")
    print("-t\t\t: enable the time record.")
    # print("-r --request\t: request (main argument).")


def research(search_type, query):
    if search_type == 'b' or search_type == "boolean":
        return boolean_research.process_query(query, dictionary, inverse_index_simple, doc_id_list)
    elif search_type == 'v2':
        return vector_research.process_query_v2(query, dictionary, inverse_index_freq)
    elif search_type == 'v' or search_type == "vector":
        return vector_research.process_query(query, dictionary, inverse_index_freq, list_doc_weight)


if __name__ == "__main__":
    RECORD_TIME = False
    search_type = None
    query = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'm:tr:', ['model', 't', 'request'])
        # import pdb; pdb.set_trace()

    # if illegal arguments, print usage to user and exit
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)

    # for each option parsed
    for o, a in opts:
        if o == '-m' or o == '--model':
            search_type = a
        elif o == '-t' or o == '--time':
            RECORD_TIME = True
        # elif o == '-r' or o == '--request':
        #     query = a
        else:
            assert False, "unhandled option"

    if search_type is None:
        print_usage()
        sys.exit(2)

    shell_open = True
    print("Loading indexes...", end="")

    with open(PATH_DICTIONARY, "r") as f:
        dictionary = json.load(f)

    with open(PATH_INVERSE_INDEX_SIMPLE, "r") as f2:
        inverse_index_simple = json.load(f2)

    with open(PATH_INVERSE_INDEX_FREQ, "r") as f3:
        inverse_index_freq = json.load(f3)

    with open(PATH_LIST_DOC_WEIGHT, "r") as f4:
        list_doc_weight = json.load(f4)

    doc_id_list = list_doc_weight.keys()

    print(" => Indexes loaded.")

    while shell_open:
        str_query = input(">> ")
        if str_query == "exit()":
            shell_open = False
        else:
            if RECORD_TIME:
                start = timeit.default_timer()                                      # start time

            print(research(search_type, str_query))

            if RECORD_TIME:
                stop = timeit.default_timer()                                       # stop time
            if RECORD_TIME:
                print('Query time: ' + str(round(stop - start, 3) * 10e3) + ' ms')      # print time taken
