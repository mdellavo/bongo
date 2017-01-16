import pprint
import datetime

from bongo.engine import json_file

db = json_file("testdb")
db.create_database()

db.create_collection("foo")
print db.get_collections()

doc = {
    "foo": True,
    "bar": datetime.datetime.utcnow(),
    "baz": [1, 2, 3]
}

Foo = db.get_collection("foo")

doc_id = Foo.save(doc)
print doc_id

doc = Foo.load(doc_id)
pprint.pprint(doc)

docs = Foo.find()
pprint.pprint(list(docs))
