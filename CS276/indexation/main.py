"""
This module implements the BSBI algorithm.

The first and the third functions are used to format the json files
which go through the whole BSBI algorithm.

The second function is the 'index_one_block' function.

The fourth is the 'merge_blocks' function.
"""
import gc
import math
import os.path
import time
import io
from collections import defaultdict

import ijson
import ujson

from collection import Collection

PATH_COLLECTION = r'..\collection_data'
PATH_FOLDER_JSONS = r'..\fichiers_traitements'


def init_indexation():
    """
    Create and init two files
    (
        future {doc.path : doc.id} and
        future {doc.id : {word.id : frequency}}
    )
    """
    with \
        open(f'{PATH_FOLDER_JSONS}/doc_index.json', 'wb') as doc_index_file, \
            open(f'{PATH_FOLDER_JSONS}/doc_vecs.json', 'wb') as doc_vecs_file:

        doc_index_file.write(b'{')
        doc_vecs_file.write(b'{')


def index_block(collection, block_i):
    """
    Index one block

    This function mainly call the block.create_posting_list method

    It also ensure the posting list, document index and document vectors writting on disk.

    It also force the python garbage collector to get ride of the block object
    so that the memory necessary remains at the same level through the whole indexing process

    It also writes the dictionnary on disk at every file so that if something goes wrong
    everything is not lost.
    """
    inter_time = time.time()

    block = collection.create_mpblock(block_i)
    block.create_docs()
    print(f"Block {block_i}: ", end='\n\t')

    block.create_posting_list()
    print("Posting List created : " + str(time.time() - inter_time), end='\n\t')
    inter_time = time.time()

    with open(f'{PATH_FOLDER_JSONS}/posting_list_block{block_i}.json', 'w') as json_index:
        ujson.dump(block.posting_list, json_index)

    doc_index = {doc.filename: doc.doc_id for doc in sorted(block.documents, key=lambda x: x.doc_id)}
    with open(f'{PATH_FOLDER_JSONS}/doc_index.json', 'ab') as doc_index_file:
        doc_index_file.write(bytes(ujson.dumps(doc_index), 'utf8')[1:-1] + b',')

    doc_vecs = {doc.doc_id: doc.vector for doc in sorted(block.documents, key=lambda x: x.doc_id)}
    with open(f'{PATH_FOLDER_JSONS}/doc_vecs.json', 'ab') as doc_vecs_file:
        doc_vecs_file.write(bytes(ujson.dumps(doc_vecs), 'utf8')[1:-1] + b',')

    del block
    gc.collect()
    print(f"Json written, memory released: "+ str(time.time() - inter_time))

    with open(f'{PATH_FOLDER_JSONS}/dictionary.json', 'w') as json_index:
        ujson.dump(collection.dictionary, json_index)


def end_indexation():
    """
    Terminate the tow files initialized at the beginning of indexation
    so that they are correct json files.
    """
    with \
        open(f'{PATH_FOLDER_JSONS}/doc_index.json', 'r+b') as doc_index_file, \
            open(f'{PATH_FOLDER_JSONS}/doc_vecs.json', 'r+b') as doc_vecs_file:
        doc_index_file.seek(-1, 2)
        doc_vecs_file.seek(-1, 2)
        doc_vecs_file.write(b'}')
        doc_index_file.write(b'}')

def merge_blocks_on_disk():
    """
    BSBI merge the different partial posting list from the blocks.

    N.B. The partial posting list are sorted from the lowest term id to the highest
    N.B. We see each partial posting list like a byte stream. We use ijson to do that easily

    We read every stream first key encountered (the lowest term is in the posting list)
    We load the lowest key values in memory.
    (now the streams which contained this key are at their next key)

    We do it again until we load the first 4096 term id posting list in memory
    then we write it on disk

    And we repeat this operation as many time as necessary to read all the partial posting list
    """
    start_time = time.time()
    print("\n\n=========================================\nStarting BSBI Merging: ")
    with \
        open(f'{PATH_FOLDER_JSONS}/posting_list_block0.json', mode='rb') as pl0,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_block1.json', mode='rb') as pl1,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_block2.json', mode='rb') as pl2,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_block3.json', mode='rb') as pl3,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_block4.json', mode='rb') as pl4,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_block5.json', mode='rb') as pl5,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_block6.json', mode='rb') as pl6, \
        open(f'{PATH_FOLDER_JSONS}/posting_list_block7.json', mode='rb') as pl7, \
        open(f'{PATH_FOLDER_JSONS}/posting_list_block8.json', mode='rb') as pl8,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_block9.json', mode='rb') as pl9,\
        open(f'{PATH_FOLDER_JSONS}/posting_list_complete.json', mode='wb') as plc_file:

        pl_list = [pl0, pl1, pl2, pl3, pl4, pl5, pl6, pl7, pl8, pl9]    # BufferedReaders' list
        parser_list = [ijson.parse(pl, buf_size=io.DEFAULT_BUFFER_SIZE) for pl in pl_list]
        curr_keys = {parser: None for parser in parser_list}
        # We init curr_keys
        for parser in parser_list:
            for prefix, event, value in parser:
                if (prefix, event) == ('', 'map_key'):
                    curr_keys[parser] = value
                    break

        # We init plc_file
        plc_file.write(b'{')

        print(f"Every files/streams opened and initialized : " + str(time.time() - start_time), end='\n\t')
        inter_time = time.time()

        plc = defaultdict(dict)
        # import pdb; pdb.set_trace()
        while parser_list:
            # While every pl is not fully processed, do:
            buff = 0  # Init writing buff
            while buff < 4 * 1024:
                # While buff is not full, do:
                # We process the smallest keys
                min_key = min(list(map(int, curr_keys.values())))
                parser_to_del = []
                for parser in parser_list:
                    if curr_keys[parser] == str(min_key):
                        curr_pl = {}
                        curr_doc_id = None
                        # We rebuild the pl for this term_id
                        for prefix, event, value in parser:
                            if (prefix, event) == ('', 'map_key'):
                                curr_keys[parser] = value
                                break
                            elif (prefix, event) == (f'{min_key}', 'map_key'):
                                curr_doc_id = value
                            elif (prefix, event) == (f'{min_key}.{curr_doc_id}', 'number'):
                                curr_pl[curr_doc_id] = value
                            elif (prefix, event) == ('', 'end_map'):
                                parser_to_del.append(parser)
                                break
                        # We merge the curr_pl with plc
                        try:
                            plc[min_key] = {**plc[min_key], **curr_pl}
                        except KeyError:
                            plc[min_key] = curr_pl
                # We increment the writing buffer
                buff += 1
                # We clean the parser list
                for parser in parser_to_del:
                    parser_list.remove(parser)
                    curr_keys.pop(parser)
                if not parser_list:
                    break
            # We write on the disk the current processed keys
            # import pdb; pdb.set_trace()
            plc_file.write(bytes(ujson.dumps(plc), 'utf8')[1:-1] + b',')
            if min_key % (8 * 4 * 1024) == 4 * 1024 - 1:
                print(f'Posting List (term_id <= {min_key}) written on disk: ' +
                      str(time.time() - inter_time), end='\n\t')
                inter_time = time.time()
            plc = defaultdict(dict)

        plc_file.seek(-1, 2)
        plc_file.write(b'}')

    # Files closed
    end_time = time.time()
    print("Posting list complete generated in " + str(round(end_time - start_time, 2)) + " s")

def generate_list_weight_docs():
    print("\n\n=========================================\nStarting generation of weight per doc")
    start_time = time.time()
    with open(f'{PATH_FOLDER_JSONS}/doc_vecs.json', mode='rb') as vecs_file:
        parser = ijson.parse(vecs_file)
        docs_weight_list = defaultdict(int)
        for prefix, event, value in parser:
            if event == 'number':
                doc_id = int(prefix.split('.')[0])
                freq = value
                docs_weight_list[doc_id] += freq**2

        for key in docs_weight_list:
            docs_weight_list[key] = math.sqrt(docs_weight_list[key])

    with open(f'{PATH_FOLDER_JSONS}/list_doc_weight.json', mode='w') as doc_weight:
        ujson.dump(docs_weight_list, doc_weight)

    print("List weight docs generated in " + str(round(time.time() - start_time, 2)) + " s")


if __name__ == "__main__":
    if not gc.isenabled():
        gc.enable()
    start_time = time.time()

    print("Initialing the CS276 Collection ... ", end='')
    COLLECTION = Collection(PATH_COLLECTION, "cs276")

    # check si dictionary.json existe pour l'ouvrir(a la premiere lecture ce n'est pas le cas)
    if os.path.exists(f'{PATH_FOLDER_JSONS}/dictionary.json'):
        with open(f'{PATH_FOLDER_JSONS}/dictionary.json', 'r') as dictionary_file:
            COLLECTION.dictionary = ujson.load(dictionary_file)

    print("[Done]")

    # BSBI, c'est parti !
    init_indexation()

    for i in range(10):
        index_block(COLLECTION, i)

    end_indexation()

    print(f"All blocks indexed: " + str(time.time() - start_time))

    # BSBI, on merge ici !
    merge_blocks_on_disk()

    # Generation fichier docs_weights
    generate_list_weight_docs()
