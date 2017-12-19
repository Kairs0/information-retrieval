import json

if __name__ == "__main__":
    with open("dic_terms.json", "r") as f:
        dictionary = json.load(f)

    with open("docID_index.json", "r") as f2:
        docID_index = json.load(f2)

    shell_open = True
    while shell_open:
        request = input(">> ")
        if request == "exit()":
            shell_open = False

        # pour l'instant : requête de 1 mot
        word_to_find = request
        print(word_to_find)
        # get wordId
        termID = dictionary[word_to_find]
        print(termID)
        # get docs
        docID_list = docID_index[str(termID)]

        print(docID_list)

        # words_OR = request.split(" OU ")
        #
        # words_AND = request.split(" ET ")
        #
        # words_NOT = request.split(" NOT ")



