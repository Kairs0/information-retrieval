import io

if __name__ == "__main__":
    with open("../traitements_linguistiques/voc.txt") as file:
        voc = file.read()

    terms = voc.split(" ")
    table = {}
    for i in range(0, len(terms)):
        term = terms[i]
        table[term] = i

    print(table)
