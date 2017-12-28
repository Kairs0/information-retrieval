import gc
import time
import io
from collections import defaultdict
import ujson
import ijson
from collection import Collection

if __name__ == "__main__":
    ##############################################
    n = 1 # Nombre de bloc sur lequel on travaille.
    ##############################################
    if not gc.isenabled():
        gc.enable()
    start_time = time.time()
    PATH = r'.\collection_data\CS276\pa1-data'

    print("Initialing the CS276 Collection ... ", end='')
    collection = Collection(PATH, "cs276")
    print("Done")
    inter_time = time.time()

    with open(f'{PATH}\doc_index.json', 'w') as doc_index:
        for block_id in range(n):
            block = collection.create_block(block_id)
            print(f"Block {block_id} created. ", end='\n\t')
            block.get_documents()
            print("Documents loaded : " + str(time.time() - inter_time), end='\n\t')
            inter_time = time.time()

            block.create_posting_list()
            print("Posting List created : " + str(time.time() - inter_time), end='\n\t')
            inter_time = time.time()

            with open(f'{PATH}\posting_list_block{block_id}.json', 'w') as json_index:
                ujson.dump(block.posting_list, json_index)

            ujson.dump({doc.name: doc.doc_id for doc in block.documents}, doc_index)

            del block
            gc.collect()
            print(f"Json written, memory released: {block_id}"+ str(time.time() - inter_time))
            inter_time = time.time()

    with open(f'{PATH}\dictionary.json', 'w') as json_index:
        ujson.dump(collection.dictionary, json_index)
    del collection
    gc.collect()
    print(f"All blocks indexed: "+ str(time.time() - start_time))
    inter_time = time.time()

    import pdb; pdb.set_trace()
    # Do the merging here !
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

        print(f"Every file stream opened and initialized"+ str(time.time() - inter_time), end='\n\t')
        inter_time = time.time()
        
        plc = defaultdict(dict)
        # import pdb; pdb.set_trace()
        while parser_list:
            # While every pl is not fully processed, do:
            buff = 0 # Init writing buff
            while buff < 4 * 1024:
                # While buff is not full, do:
                # We process the smallest keys
                min_key = min(list(map(int,curr_keys.values())))
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
            plc_file.write(bytes(ujson.dumps(plc), 'utf8')[1:-1] + b',') # Attention on ne veut écrire qu'un seul dico >>.<<
            print(f'Posting List (term_id <= {min_key}) written on disk: ' + str(time.time() - inter_time), end='\n\t')
            inter_time = time.time()
            plc = defaultdict(dict)

        plc_file.seek(-1,2)
        plc_file.write(b'}')
        print(f'[DONE] '+ str(time.time() - inter_time))
        inter_time = time.time()

    # Files closed
    end_time = time.time()
    print("Result calculated in " + str(round(end_time - start_time, 2)) + " s")

    # import pdb; pdb.set_trace()
