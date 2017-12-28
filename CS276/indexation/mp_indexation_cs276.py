import gc
import time
import io
from collections import defaultdict
import ujson
import ijson
from collection import Collection

# PATH = r'.\collection_data\CS276\pa1-data'
PATH = r'..\collection_data'

def init_indexation():
    with \
        open(f'{PATH}\doc_index.json', 'wb') as doc_index_file,\
        open(f'{PATH}\doc_vecs.json', 'wb') as doc_vecs_file:

        doc_index_file.write(b'{')
        doc_vecs_file.write(b'{')

def index_block(collection, block_i):
    inter_time = time.time()

    with \
        open(f'{PATH}\doc_index.json', 'ab') as doc_index_file,\
        open(f'{PATH}\doc_vecs.json', 'ab') as doc_vecs_file:

        block = collection.create_mpblock(block_i)
        block.create_docs()
        print(f"Block {block_i}: ", end='\n\t')

        print("Posting List creation ... ", end=' ')
        block.create_posting_list()
        print("[DONE] : " + str(time.time() - inter_time), end='\n\t')
        inter_time = time.time()

        with open(f'{PATH}\posting_list_block{block_i}.json', 'w') as json_index:
            ujson.dump(block.posting_list, json_index)

        doc_index = {doc.filename: doc.doc_id for doc in sorted(block.documents, key=lambda x: x.doc_id)}
        doc_index_file.write(bytes(ujson.dumps(doc_index), 'utf8')[1:-1] + b',')

        doc_vecs = {doc.doc_id: doc.vector for doc in sorted(block.documents, key=lambda x: x.doc_id)}
        doc_vecs_file.write(bytes(ujson.dumps(doc_vecs), 'utf8')[1:-1] + b',')

        del block
        gc.collect()
        print(f"Json written, memory released: "+ str(time.time() - inter_time))

    with open(f'{PATH}\dictionary.json', 'w') as json_index:
        ujson.dump(collection.dictionary, json_index)

def end_indexation():
    with \
        open(f'{PATH}\doc_index.json', 'r+b') as doc_index_file,\
        open(f'{PATH}\doc_vecs.json', 'r+b') as doc_vecs_file:
        doc_index_file.seek(-1, 2)
        doc_vecs_file.seek(-1, 2)
        doc_vecs_file.write(b'}')
        doc_index_file.write(b'}')

##############################################################################
############### BSBI MERGE ###################################################
##############################################################################

def merge_blocks_on_disk():
    start_time = time.time()
    with \
        open(f'{PATH}\posting_list_block0.json', mode='rb') as pl0,\
        open(f'{PATH}\posting_list_block1.json', mode='rb') as pl1,\
		open(f'{PATH}\posting_list_block2.json', mode='rb') as pl2,\
        open(f'{PATH}\posting_list_block3.json', mode='rb') as pl3,\
		open(f'{PATH}\posting_list_block4.json', mode='rb') as pl4,\
        open(f'{PATH}\posting_list_block5.json', mode='rb') as pl5,\
		open(f'{PATH}\posting_list_block6.json', mode='rb') as pl6,\
        open(f'{PATH}\posting_list_block7.json', mode='rb') as pl7,\
		open(f'{PATH}\posting_list_block8.json', mode='rb') as pl8,\
        open(f'{PATH}\posting_list_block9.json', mode='rb') as pl9,\
        open(f'{PATH}\posting_list_complete.json', mode='wb') as plc_file:

        pl_list = [pl0, pl1, pl2, pl3, pl4, pl5, pl6, pl7, pl8, pl9]    # BufferedReaders' list
        parser_list = [ijson.parse(pl, buf_size=io.DEFAULT_BUFFER_SIZE) for pl in pl_list]
        curr_keys = {parser: None for parser in parser_list}
        # We init curr_keys
        for parser in parser_list:
            for prefix, event, value in parser:
                if  (prefix, event) == ('', 'map_key'):
                    curr_keys[parser] = value
                    break

        # We init plc_file
        plc_file.write(b'{')

        print(f"Every files/streams opened and initialized : "+ str(time.time() - start_time), end='\n\t')
        inter_time = time.time()

        plc = defaultdict(dict)
        # import pdb; pdb.set_trace()
        while parser_list:
            # While every pl is not fully processed, do:
            buff = 0 # Init writing buff
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
                print(f'Posting List (term_id <= {min_key}) written on disk: '\
                    + str(time.time() - inter_time), end='\n\t')
				inter_time = time.time()
            plc = defaultdict(dict)

        plc_file.seek(-1, 2)
        plc_file.write(b'}')

    # Files closed
    end_time = time.time()
    print("Result calculated in " + str(round(end_time - start_time, 2)) + " s")

if __name__ == "__main__":
    if not gc.isenabled():
        gc.enable()
    start_time = time.time()

    print("Initialing the CS276 Collection ... ", end='')
    collection = Collection(PATH, "cs276")
    with open(f'{PATH}\dictionary.json', 'r') as dictionary_file:
        collection.dictionary = ujson.load(dictionary_file)
    print("[Done]")

    # BSBI, c'est parti !
    init_indexation()

    for i in range(10):
        index_block(collection, i)

    end_indexation()

    print(f"All blocks indexed: "+ str(time.time() - start_time))

    # BSBI, on merge ici !	
    print("\n\n=========================================\nStarting BSBI Merging: ")
    merge_blocks_on_disk()
    # Il faut calculer le poid total de chaque doc ici pour pouvoir faire la recherche vectoriel.
    # Attention le fichier doc_vecs est très lourd, le prendre comme un stream est une bonne idée, (quitte à le parcourir plusieurs fois)
    # import pdb; pdb.set_trace()
