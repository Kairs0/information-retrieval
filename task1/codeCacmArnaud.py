import nltk
from nltk.corpus import stopwords


def tokenize(cacm_file):
    tokens = []
    with open(cacm_file) as f:
        content = f.read()

    data = content.split("\n.")

    for block in data:
        if block[0] == 'T' or block[0] == 'W' or block[0] == 'K':
            tmp = block.replace('.', ' ')\
                .replace(',', ' ')\
                .replace(':', ' ')\
                .replace('(', ' ')\
                .replace(')', ' ')\
                .lower()
            tokens += nltk.word_tokenize(tmp)

    return tokens


def vocabulary(tokens, common_words_file):
    with open(common_words_file) as f:
        content_common = f.read()

    stop_words = set(common_words_file.re)





if __name__ == "__main__":
    tokens_result = tokenize("cacm.all")

    # Question 1
    print(len(tokenize("cacm.all")))

    # Question 2

