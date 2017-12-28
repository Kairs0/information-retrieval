
class Document(object):

    def __init__(self, filename, doc_id):
        self.filename = filename.split("\\")[-1]
        self.doc_id = doc_id
        self.vector = []
