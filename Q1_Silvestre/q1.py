import datetime
import math
import nltk


def get_indexed_blocks():
    with open("collection_data//CACM//cacm.all", 'r') as infile:
        data = infile.read()

    blocks = data.split("\n.")
    indexed_blocks = [block[2:]
                      for block in blocks if block[0] in ['T', 'W', 'k']]
    return indexed_blocks


def get_common_words():
    with open("collection_data//CACM//common_words", 'r') as infile:
        data = infile.read()

    return data.split("\n")


def get_tokens(indexed_blocks):
    tokens = []
    for block in indexed_blocks:
        tokens += nltk.word_tokenize(block)

    ponct = ['.', ',', '(', ')', ':']
    for i, token in enumerate(tokens):
        if token in ponct:
            del tokens[i]
    return tokens


def get_vocabulary(tokens):
    tokens = [token.lower() for token in tokens]
    words = list(set(tokens))
    common_words = get_common_words()
    for i, word in enumerate(words):
        if word in common_words:
            del words[i]
    return words


def main():
    starting_time = datetime.datetime.now()
    all_blocks = get_indexed_blocks()
    all_tokens = get_tokens(all_blocks)
    all_voc = get_vocabulary(all_tokens)
    half_blocks = all_blocks[len(all_blocks) // 2:]
    half_tokens = get_tokens(half_blocks)
    half_voc = get_vocabulary(half_tokens)

    b = math.log(len(all_voc) / len(half_voc)) / \
        math.log(len(all_tokens) / len(half_tokens))
    k = len(all_voc) / (len(all_tokens) ** b)
    print("All Tokens :", len(all_tokens))
    print("All Vocabulary :", len(all_voc))
    print("Half Tokens :", len(half_tokens))
    print("Half Vocabulary :", len(half_voc))

    print("b :", b)
    print("k :", k)
    print("Time needed : ", datetime.datetime.now() - starting_time)
    print(k * (1000000**b))


if __name__ == '__main__':
    main()
