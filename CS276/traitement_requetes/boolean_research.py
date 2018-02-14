"""
This module implements the boolean research. The main function is 'process_query' which is called
by the shell main function.

We transform the query before trying to find matching documents.
For example, the query:     'bill OR Gates AND (vista OR XP) AND NOT mac'
will be transform into:     'bill Gates OR vista XP OR AND mac NOT AND'
We split it in the postfix_queue.

Then we process it from the left to the right.

And finally we get a list of doc_id which match the query. This is our result.
"""
from collections import deque, OrderedDict
import nltk


def simple_request(term):
    """
    Check whether 'term' is a list of doc_id (a intermediate result) or a word not treated yet.

    If term is a list of doc_id, we return the list,
    Else we try to get the list of doc_id containing this term.
    """
    try:
        if isinstance(term, list):
            return term
        return list(POSTING_LIST[str(DICTIONARY[term])].keys())
    except KeyError:
        return list()


def process_query(query, dictionary, posting_list, doc_id_list):
    """
    Main function of this module.

    :param query (string) : 'bill OR Gates AND (vista OR XP) AND NOT mac'
    :param dictionary,
    :param posting_list
    :param doc_id_list (list): list of every doc id of the collection
    :return result : list of doc_id matching the query.

    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    """
    global DICTIONARY
    DICTIONARY = OrderedDict(dictionary)
    global POSTING_LIST
    POSTING_LIST = posting_list

    stemmer = nltk.stem.SnowballStemmer("english") # instantiate stemmer
    # prepare query list
    query = query.replace('(', '( ')
    query = query.replace(')', ' )')
    query = query.split(' ')

    results_stack = []
    postfix_queue = deque(shunting_yard(query)) # get query in postfix notation as a queue

    while postfix_queue:
        token = postfix_queue.popleft()
        result = [] # the evaluated result at each stage
        # if operand, add postings list for term to results stack
        if token != 'AND' and token != 'OR' and token != 'NOT':
            token = stemmer.stem(token) # stem the token
            # default empty list if not in dictionary
            # import pdb; pdb.set_trace()
            if token in dictionary:
                result = simple_request(token)

        # else if AND operator
        elif token == 'AND':
            right_operand = results_stack.pop()
            left_operand = results_stack.pop()
            # print(left_operand, 'AND', left_operand) # check
            result = boolean_AND(left_operand, right_operand)   # evaluate AND

        # else if OR operator
        elif token == 'OR':
            right_operand = results_stack.pop()
            left_operand = results_stack.pop()
            # print(left_operand, 'OR', left_operand) # check
            result = boolean_OR(left_operand, right_operand)    # evaluate OR

        # else if NOT operator
        elif token == 'NOT':
            right_operand = results_stack.pop()
            # print('NOT', right_operand) # check
            result = boolean_NOT(right_operand, doc_id_list) # evaluate NOT

        # push evaluated result back to stack
        results_stack.append(result)
        # print ('result', result) # check

    # NOTE: at this point results_stack should only have one item and it is the final result
    if len(results_stack) != 1:
        print("ERROR: results_stack. Please check valid query")  # check for errors

    return sorted(results_stack.pop(), key=lambda x: int(x))


def shunting_yard(infix_tokens):
    """
    :return list of postfix tokens converted from the given infix expression
    :params:
        infix_tokens: list of tokens in original query of infix notation

    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    """
    # define precedences
    precedence = {}
    precedence['NOT'] = 3
    precedence['AND'] = 2
    precedence['OR'] = 1
    precedence['('] = 0
    precedence[')'] = 0

    # declare data structures
    output = []
    operator_stack = []

    # while there are tokens to be read
    for token in infix_tokens:

        # if left bracket
        if token == '(':
            operator_stack.append(token)

        # if right bracket, pop all operators from operator stack onto output until we hit left bracket
        elif token == ')':
            operator = operator_stack.pop()
            while operator != '(':
                output.append(operator)
                operator = operator_stack.pop()

        # if operator, pop operators from operator stack to queue if they are of higher precedence
        elif token in precedence:
            # if operator stack is not empty
            if operator_stack:
                current_operator = operator_stack[-1]
                while operator_stack and precedence[current_operator] > precedence[token]:
                    output.append(operator_stack.pop())
                    if operator_stack:
                        current_operator = operator_stack[-1]

            operator_stack.append(token) # add token to stack

        # else if operands, add to output list
        else:
            output.append(token.lower())

    # while there are still operators on the stack, pop them into the queue
    while operator_stack:
        output.append(operator_stack.pop())
    print('postfix:', output)  # check
    return output


def boolean_NOT(right_operand, doc_id_list):
    """
    :return list of doc_id: the compliment of given right_operand
    :params:
        right_operand:  sorted list of docIDs to be complimented
        indexed_docIDs: sorted list of all docIDs indexed

    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    Deeply changed
    """
    return [docID for docID in doc_id_list if docID not in simple_request(right_operand)]


def boolean_OR(left_operand, right_operand):
    """
    :return list of doc_id: result from 'OR' operation between left and right operands
    :params:
        left_operand:   partial_doc_id_list, or a stemmed word not treated yet
        right_operand:  partial_doc_id_list, or a stemmed word not treated yet

    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    Deeply changed
    """
    return list(set(simple_request(left_operand) + simple_request(right_operand)))


def boolean_AND(left_operand, right_operand):
    """
    :return list of doc_id: result from 'AND' operation between left and right operands
    :params:
        left_operand:   partial_doc_id_list, or a stemmed word not treated yet
        right_operand:  partial_doc_id_list, or a stemmed word not treated yet

    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    Deeply changed
    """
    return list(set(simple_request(left_operand)).intersection(simple_request(right_operand)))
