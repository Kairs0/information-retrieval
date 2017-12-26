import gc
import time
import io
from collections import defaultdict
from multiprocessing import Manager
import ujson
import ijson
from collection import Collection

PATH = r'.\collection_data\CS276\pa1-data'

def create_blocks(n):
    if not gc.isenabled():
        gc.enable()
    start_time = time.time()

    print("Initialing the CS276 Collection ... ", end='')
    collection = Collection(PATH, "cs276")
    print("[Done]")
    manager = Manager()
    inter_time = time.time()

    with \
        open(f'{PATH}\doc_index.json', 'wb') as doc_index_file,\
        open(f'{PATH}\doc_vecs.json', 'wb') as doc_vecs_file:
        doc_index_file.write(b'{')
        doc_vecs_file.write(b'{')

        for block_id in range(n):
            block = collection.create_mpblock(block_id, manager)
            print(f"Block {block_id}: ", end='\n\t')

            print("Posting List creation ... ", end='')
            block.create_posting_list()
            print("[DONE] : " + str(time.time() - inter_time), end='\n\t')
            inter_time = time.time()

            with open(f'{PATH}\posting_list_block{block_id}.json', 'w') as json_index:
                ujson.dump(block.posting_list, json_index)

            doc_index = {doc.filename: doc.doc_id for doc in sorted(block.documents, key=lambda x: x.doc_id)}
            doc_index_file.write(bytes(ujson.dumps(doc_index), 'utf8')[1:-1] + b',')

            doc_vecs = {doc.doc_id: doc.vector for doc in sorted(block.documents, key=lambda x: x.doc_id)}
            doc_vecs_file.write(bytes(ujson.dumps(doc_vecs), 'utf8')[1:-1] + b',')

            del block
            gc.collect()
            print(f"Json written, memory released: "+ str(time.time() - inter_time))
            inter_time = time.time()

        doc_index_file.seek(-1, 2)
        doc_index_file.write(b'}')
        doc_vecs_file.seek(-1, 2)
        doc_vecs_file.write(b'}')

    with open(f'{PATH}\dictionary.json', 'w') as json_index:
        ujson.dump(collection.dictionary, json_index)

    print(f"All blocks indexed: "+ str(time.time() - start_time))
    inter_time = time.time()
    del collection
    manager.shutdown()
    gc.collect()

def merge_blocks_on_disk():
    start_time = time.time()
    with \
        open(f'{PATH}\posting_list_block0.json', mode='rb') as pl0,\
        open(f'{PATH}\posting_list_block1.json', mode='rb') as pl1, \
        open(f'{PATH}\posting_list_complete.json', mode='wb') as plc_file:

        pl_list = [pl0, pl1]    # BufferedReaders' list
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

        print(f"Every file stream opened and initialized : "+ str(time.time() - start_time), end='\n\t')
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
            print(f'Posting List (term_id <= {min_key}) written on disk: '\
                + str(time.time() - inter_time), end='\n\t')
            inter_time = time.time()
            plc = defaultdict(dict)

        plc_file.seek(-1, 2)
        plc_file.write(b'}')
        print(f'[DONE] '+ str(time.time() - inter_time))
        inter_time = time.time()

    # Files closed
    end_time = time.time()
    print("Result calculated in " + str(round(end_time - start_time, 2)) + " s")

if __name__ == "__main__":
    ##############################################
    n = 5 # Nombre de bloc sur lequel on travaille.
    ##############################################
    create_blocks(n)

    merge_blocks_on_disk()
    # import pdb; pdb.set_trace()
