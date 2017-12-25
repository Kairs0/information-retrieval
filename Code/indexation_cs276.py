import gc
import time
import io
from collections import defaultdict
import ujson
import ijson
from collection import Collection

if __name__ == "__main__":
    start_time = time.time()
    PATH = r'.\collection_data\CS276\pa1-data'
    """
    print("Initialing the CS276 Collection ... ", end='')
    collection = Collection(PATH, "cs276")
    print("Done")

    for block_id in range(2):
        block = collection.create_block(block_id)
        print(f"Block {block_id} created. ")
        inter_time = time.time()
        block.get_documents()
        print("Documents loaded : " + str(time.time() - inter_time))
        inter_time = time.time()

        block.create_posting_list()
        print("Posting List created : " + str(time.time() - inter_time))
        inter_time = time.time()

        with open(f'{PATH}\posting_list_block{block_id}.json', 'w') as json_index:
            ujson.dump(block.posting_list, json_index)

        with open(f'{PATH}\doc_index.json', 'a') as json_index:
            ujson.dump({doc.name: doc.doc_id for doc in block.documents}, json_index)

        del block
        if not gc.isenabled():
            gc.enable()
        gc.collect()
        print(f"Memory released for block{block_id}")
    """

    # Do the merging here !
    with \
        open(f'{PATH}\posting_list_block0.json', mode='rb') as pl0,\
        open(f'{PATH}\posting_list_block1.json', mode='rb') as pl1, \
        open(f'{PATH}\posting_list_complete.json', mode='a') as plc_file:
        pl_list = [pl0, pl1]
        pl_processed = set()

        plc = defaultdict(dict)
        import pdb; pdb.set_trace()
        term_id = 0
        while True:
            if len(pl_processed) == 2:
                break
            buff = 0
            while buff < 1024:
                for pl in pl_list:
                    try:
                        objects = ijson.items(pl, f"{term_id}")
                        pl_termid_pl = [obj for obj in objects][0]
                        try:
                            plc[term_id] = {**plc[term_id], **pl_termid_pl}
                        except KeyError:
                            plc[term_id] = pl_termid_pl
                        buff += 1
                    except StopIteration:
                        # the current posting list was fully processed
                        pl_processed.add(pl)
                    import pdb; pdb.set_trace()
                term_id += 1
            ujson.dump(plc, plc_file)
                        
            


    end_time = time.time()
    print("Result calculated in " + str(round(end_time - start_time, 2)) + " s")

    import pdb; pdb.set_trace()
