import json

if __name__ == "__main__":
    with open("dic_terms.json", "r") as f:
        json_dic_term = json.load(f)

    with open("dic_termId_docId.json", "r") as f2:
        dic_termsId_docId = json.load(f2)

    shell_open = True
    while shell_open:
        request = input(">> ")
        if request == "exit()":
            shell_open = False

        # pour l'instant : requête de 1 mot
        word_to_find = request
        print(word_to_find)
        # get wordId
        id_word_to_find = json_dic_term[word_to_find]
        print(id_word_to_find)
        # get docs
        docs_id = dic_termsId_docId[str(id_word_to_find)]

        print(docs_id)

        # words_OR = request.split(" OU ")
        #
        # words_AND = request.split(" ET ")
        #
        # words_NOT = request.split(" NOT ")



