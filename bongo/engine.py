import os
import json
import uuid
import glob
import logging
import collections
import datetime

log = logging.getLogger(__name__)

PK = "_id"
DOCUMENT_PREFIX = "doc"
DOCUMENT_EXTENSION = "json"


# FIXME uuid4 is terrible, an objectid would be better
class DocumentId(object):
    def __init__(self, value=None):
        self.value = value or uuid.uuid4().hex

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "<DocumentId({})>".format(self)

DEFAULT_ENCODERS = [
    (DocumentId, lambda o: str(o)),
    ((datetime.date, datetime.datetime), lambda o: o.isoformat(" "))
]


class JsonEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super(JsonEncoder, self).__init__(*args, **kwargs)
        self.encoders = []
        self.encoders.extend(DEFAULT_ENCODERS)

    def add_encoder(self, cls, func):
        self.encoders.append((cls, func))

    def default(self, o):
        for cls, func in self.encoders:
            if isinstance(o, cls):
                return func(o)


class Serializer(object):
    def serialize(self, document):
        raise NotImplemented()

    def deserialize(self, data):
        raise NotImplemented()


class JsonSerializer(Serializer):
    def __init__(self, encoder=None):
        self.encoder = encoder or JsonEncoder

    def serialize(self, document):
        return json.dumps(document, sort_keys=True, indent=4, cls=self.encoder)

    def deserialize(self, data):
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

    def get_collections(self):
        raise NotImplemented()

    def store_document(self, collection, document_id, document):
        raise NotImplemented()

    def load_document(self, collection, document_id):
        raise NotImplemented()

    def documents_iter(self, collection):
        raise NotImplemented()


class FileStorage(Storage):
    def __init__(self, path, serializer):
        self.path = path
        self.serializer = serializer

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

    def get_collections(self):
        db_path = self.get_database_path()
        dirs = [e for e in os.listdir(db_path) if os.path.isdir(self.get_collection_path(e))]
        return sorted(dirs)

    def store_document(self, collection, document_id, document):
        doc_path = self.get_document_path(collection, document_id)
        tmp_path = doc_path + ".tmp"
        with open(tmp_path, "w") as f:
            f.write(self.serializer.serialize(document))
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_path, doc_path)

    def load_by_path(self, path):
        with open(path) as f:
            return self.serializer.deserialize(f.read())

    def load_document(self, collection, document_id):
        doc_path = self.get_document_path(collection, document_id)
        return self.load_by_path(doc_path)

    def documents_iter(self, collection):
        pattern = os.path.join(self.get_collection_path(collection), self.get_document_pattern())
        for path in glob.iglob(pattern):
            yield self.load_by_path(path)


class Index(object):
    def __init__(self):
        self.index = collections.defaultdict(list)

    def get_key(self, document):
        pass

    def add_documment(self, document):
        self.index[self.get_key(document)] = document[PK]


class Collection(object):
    def __init__(self, storage, collection):
        self.storage = storage
        self.name = collection

    def __repr__(self):
        return "<Collection(name={})>".format(self.name)

    def get_name(self):
        return self.name

    def save(self, document):
        document_id = document.get("_id")
        if not document_id:
            document_id = DocumentId()
            document[PK] = document_id

        self.storage.store_document(self.name, document_id, document)
        return document_id

    def load(self, document_id):
        return self.storage.load_document(self.name, document_id)

    def find(self):
        return Cursor(self.storage.documents_iter(self.name))


class Bongo(object):
    def __init__(self, storage):
        self.storage = storage

    def create_database(self):
        self.storage.create_database()

    def create_collection(self, collection):
        self.storage.create_collection(collection)

    def get_collections(self):
        return [Collection(self.storage, name) for name in self.storage.get_collections()]

    def get_collection(self, name):
        for collection in self.get_collections():
            if collection.get_name() == name:
                return collection


def json_file(path):
    return Bongo(FileStorage(path, JsonSerializer()))
