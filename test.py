import pprint

from bongo.engine import Bongo

db = Bongo.json_file("testdb")
db.create_database()

db.create_collection("foo")

doc = {"foo": "bar", "baz": [1,2,3]}

doc_id = db.save("foo", doc)
print doc_id

doc = db.load("foo", doc_id)
pprint.pprint(doc)

docs = db.find("foo")
pprint.pprint(list(docs))

