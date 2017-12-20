import json
import sys
import getopt
import timeit
import boolean_research
import vector_research


def print_usage():
    """
    prints the proper command usage
    """
    print("usage: " + sys.argv[0] + " -m model | -t | -r request")
    print("Options and arguments:")
    print("-m --model\t: research model chosen for the search. ['b','boolean', 'v', 'vector']")
    print("-t\t\t: enable the time record.")
    print("-r --request\t: request (main argument).")


if __name__ == "__main__":
    with open("dic_terms.json", "r") as f:
        dictionary = json.load(f)

    with open("docID_index.json", "r") as f2:
        docID_index = json.load(f2)

    with open("docID_index_with_freq.json", "r") as f3:
        docID_index_with_frequency = json.load(f3)

    with open("docID_weight.json", "r") as f4:
        docID_weight = json.load(f4)

    docID_list = set()
    for docID_term_list in docID_index.values():
        for docID in docID_term_list:
            docID_list.add(docID)

    RECORD_TIME = False
    search_type = None
    query = None


    try:
        opts, args = getopt.getopt(sys.argv[1:], 'm:tr:', ['model', 't', 'request'])
        #import pdb; pdb.set_trace()

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
        elif o == '-r' or o == '--request':
            query = a
        else:
            assert False, "unhandled option"

    if (search_type == None or query == None):
        print_usage()
        sys.exit(2)

    if (RECORD_TIME):
        start = timeit.default_timer()                                          # start time

    if search_type == 'b' or search_type == "boolean":                          # do the research
        print(boolean_research.process_query(query, dictionary, docID_index, docID_list))
    elif search_type == 'v' or search_type == "vector":
        print(vector_research.process_query(query, dictionary, docID_index_with_frequency, docID_weight))

    if (RECORD_TIME):
        stop = timeit.default_timer()                                           # stop time
    if (RECORD_TIME):
        print('Indexing time:' + str(stop - start))                            # print time taken
