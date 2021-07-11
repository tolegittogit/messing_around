from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
from datetime import datetime
import json
import re

j = "/home/thisguy/data/sotu/sotu.json"
with open(j, "r") as f:
    data = json.load(f)

for d in data:
    d["DateTime"] = datetime.strptime(d["Date"], "%B %d, %Y")

##mongo
client = MongoClient()
db = client['nlp']
sotu = db['sotu']
result = sotu.insert_many(data)
result.inserted_ids

##dask - just messing around
from dask.distributed import Client, progress, LocalCluster
import dask.bag as dbag

cluster = LocalCluster()
client = Client(cluster)

bag = dbag.read_text(j).map(json.loads).flatten()
data = bag.compute()


##elasticsearch
def connect_elasticsearch():
    elastic_conn = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if not elastic_conn.ping():
        print('Could not connect to elastic search')
        sys.exit(1)
    return elastic_conn

elastic_conn = connect_elasticsearch()
elastic_conn.indices.create(index='addresses', ignore=400)

for d in data:
    date = datetime.strptime(d["Date"], "%B %d, %Y")
    d["DateTime"] = date.strftime("%Y-%m-%dT%H:%M:%SZ")

#probably should specify mapping
resp = helpers.bulk(
elastic_conn,
data,
index = "addresses",
doc_type = "_doc"
)
