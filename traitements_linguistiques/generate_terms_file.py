from collection import Collection
import io

if __name__ == "__main__":
    # Preparation pour construction index: extraction des tokens dans file
    collection = Collection("cacm.all", "cacm")
    collection.calc_blocks()
    collection.tokenize()
    collection.calc_vocabulary("common_words")
    voc_coll = collection.vocabulary

    voc_file = io.open("voc.txt", 'w')
    for term in map(str.lower, voc_coll):
        voc_file.write(term)
        voc_file.write(" ")

    voc_file.close()
