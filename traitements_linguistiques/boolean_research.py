"""
TO-DO
"""
import json, math
from collections import deque, OrderedDict, defaultdict
import nltk

DICTIONARY = OrderedDict()
DOCID_INDEX = defaultdict(set)

def simple_request(term):
    try:
        if type(term) is list:
            return term
        #import pdb; pdb.set_trace()
        return DOCID_INDEX[str(DICTIONARY[term])]
    except KeyError:
        return list()


def process_query(query, dictionary, docID_index, docID_list):
    """
    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py

    returns the list of docIDs in the result for the given query
    params:
    query:          the query string e.g. 'bill OR Gates AND (vista OR XP) AND NOT mac'
    dictionary:     the dictionary in memory
    indexed_docIDs: the list of all docIDs indexed (used for negations)
    """
    global DICTIONARY
    DICTIONARY = dictionary
    global DOCID_INDEX
    DOCID_INDEX = docID_index

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
        if (token != 'AND' and token != 'OR' and token != 'NOT'):
            token = stemmer.stem(token) # stem the token
            # default empty list if not in dictionary
            if (token in dictionary): 
                result = simple_request(token)
        
        # else if AND operator
        elif (token == 'AND'):
            right_operand = results_stack.pop()
            left_operand = results_stack.pop()
            # print(left_operand, 'AND', left_operand) # check
            result = boolean_AND(left_operand, right_operand)   # evaluate AND

        # else if OR operator
        elif (token == 'OR'):
            right_operand = results_stack.pop()
            left_operand = results_stack.pop()
            # print(left_operand, 'OR', left_operand) # check
            result = boolean_OR(left_operand, right_operand)    # evaluate OR

        # else if NOT operator
        elif (token == 'NOT'):
            right_operand = results_stack.pop()
            # print('NOT', right_operand) # check
            result = boolean_NOT(right_operand, docID_list) # evaluate NOT

        # push evaluated result back to stack
        results_stack.append(result)                        
        # print ('result', result) # check

    # NOTE: at this point results_stack should only have one item and it is the final result
    if len(results_stack) != 1: 
        print ("ERROR: results_stack. Please check valid query") # check for errors
    
    return results_stack.pop()


def shunting_yard(infix_tokens):
    """
    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py

    returns the list of postfix tokens converted from the given infix expression
    params:
        infix_tokens: list of tokens in original query of infix notation
    """
    # define precedences
    precedence = {}
    precedence['NOT'] = 3
    precedence['AND'] = 2
    precedence['OR'] = 1
    precedence['('] = 0
    precedence[')'] = 0    

    # declare data strucures
    output = []
    operator_stack = []

    # while there are tokens to be read
    for token in infix_tokens:
        
        # if left bracket
        if (token == '('):
            operator_stack.append(token)
        
        # if right bracket, pop all operators from operator stack onto output until we hit left bracket
        elif (token == ')'):
            operator = operator_stack.pop()
            while operator != '(':
                output.append(operator)
                operator = operator_stack.pop()

        # if operator, pop operators from operator stack to queue if they are of higher precedence
        elif (token in precedence):
            # if operator stack is not empty
            if (operator_stack):
                current_operator = operator_stack[-1]
                while (operator_stack and precedence[current_operator] > precedence[token]):
                    output.append(operator_stack.pop())
                    if (operator_stack):
                        current_operator = operator_stack[-1]

            operator_stack.append(token) # add token to stack

        # else if operands, add to output list
        else:
            output.append(token.lower())

    # while there are still operators on the stack, pop them into the queue
    while (operator_stack):
        output.append(operator_stack.pop())
    print('postfix:', output)  # check
    return output

def boolean_NOT(right_operand, docID_list):
    """
    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    Strongly simplified

    returns the list of docIDs which is the compliment of given right_operand
    params:
        right_operand:  sorted list of docIDs to be complimented
        indexed_docIDs: sorted list of all docIDs indexed
    """
    return [docID for docID in docID_list if docID not in simple_request(right_operand)]

def boolean_OR(left_operand, right_operand):
    """
    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    Strongly simplified

    returns list of docIDs that results from 'OR' operation between left and right operands
    params:
        left_operand:   docID list on the left
        right_operand:  docID list on the right
    """
    #import pdb; pdb.set_trace()
    return list(set(simple_request(left_operand) + simple_request(right_operand)))

def boolean_AND(left_operand, right_operand):
    """
    Source: https://github.com/spyrant/boolean-retrieval-engine/blob/master/search.py
    Strongly simplified

    returns list of docIDs that results from 'AND' operation between left and right operands
    params:
        left_operand:   docID list on the left
        right_operand:  docID list on the right
    """
    return list(set(simple_request(left_operand)).intersection(simple_request(right_operand)))
