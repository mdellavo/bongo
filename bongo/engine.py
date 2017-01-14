import os
import json
import uuid
import glob
import logging

log = logging.getLogger(__name__)

FSYNC = True
PK = "_id"
DOCUMENT_PREFIX = "doc"
DOCUMENT_EXTENSION = "json"


class Encoder(object):
    def encode(self, document):
        raise NotImplemented()

    def decode(self, data):
        raise NotImplemented()


class JsonEncoder(Encoder):
    def encode(self, document):
        return json.dumps(document, indent=4)

    def decode(self, data):
        return json.loads(data)


class Cursor(object):
    def __init__(self, docs_iter):
        self.docs_iter = docs_iter

    def __iter__(self):
        return self

    def next(self):
        return self.docs_iter.next()


class Storage(object):
    def create_database(self):
        raise NotImplemented()

    def create_collection(self, collection):
        raise NotImplemented()

    def store_document(self, collection, document_id, document):
        raise NotImplemented()

    def load_document(self, collection, document_id):
        raise NotImplemented()

    def documents_iter(self, collection):
        raise NotImplemented()


class FileStorage(Storage):
    def __init__(self, path, encoder):
        self.path = path
        self.encoder = encoder

    def get_database_path(self):
        return self.path

    def get_collection_path(self, collection):
        return os.path.join(self.path, collection)

    def get_document_prefix(self):
        return DOCUMENT_PREFIX

    def get_document_ext(self):
        return DOCUMENT_EXTENSION

    def get_document_pattern(self):
        return "{}-*.{}".format(self.get_document_prefix(), self.get_document_ext())

    def get_document_path(self, collection, document_id):
        filename = "{}-{}.{}".format(self.get_document_prefix(), document_id, self.get_document_ext())
        return os.path.join(self.path, collection, filename)

    def create_database(self):
        db_path = self.get_database_path()
        if not os.path.exists(db_path):
            os.makedirs(db_path)

    def create_collection(self, collection):
        collection_path = self.get_collection_path(collection)
        if not os.path.exists(collection_path):
            os.makedirs(collection_path)

    def store_document(self, collection, document_id, document):
        doc_path = self.get_document_path(collection, document_id)
        tmp_path = doc_path + ".tmp"
        with open(tmp_path, "w") as f:
            f.write(self.encoder.encode(document))
            f.flush()
            if FSYNC:
                os.fsync(f.fileno())
        os.rename(tmp_path, doc_path)

    def load_by_path(self, path):
        with open(path) as f:
            return self.encoder.decode(f.read())

    def load_document(self, collection, document_id):
        doc_path = self.get_document_path(collection, document_id)
        return self.load_by_path(doc_path)

    def documents_iter(self, collection):
        pattern = os.path.join(self.get_collection_path(collection), self.get_document_pattern())
        for path in glob.iglob(pattern):
            yield self.load_by_path(path)


class DocumentId(object):
    def __init__(self, value=None):
        self.value = value or uuid.uuid4().hex

    def __str__(self):
        return str(self.value)


class Bongo(object):
    def __init__(self, storage):
        self.storage = storage

    @classmethod
    def json_file(cls, path):
        return cls(FileStorage(path, JsonEncoder()))

    def create_database(self):
        self.storage.create_database()

    def create_collection(self, collection):
        self.storage.create_collection(collection)

    def save(self, collection, document):
        document_id = document.get("_id")
        if not document_id:
            document_id = str(DocumentId())
            document[PK] = document_id

        self.storage.store_document(collection, document_id, document)

        return document_id

    def load(self, collection, document_id):
        return self.storage.load_document(collection, document_id)

    def find(self, collection, filter=None):
        return Cursor(self.storage.documents_iter(collection))
