import json

if __name__ == "__main__":
    with open("voc.txt") as file:
        voc = file.read()

    terms = voc.split(" ")
    dic_term_termId = {}
    for i in range(0, len(terms)):
        term = terms[i]
        dic_term_termId[term] = i

    # generate json file
    with open("dic_terms.json", 'w') as json_file:
        json.dump(dic_term_termId, json_file)


