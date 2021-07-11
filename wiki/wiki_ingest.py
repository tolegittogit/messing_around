#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 25 01:36:32 2021

@author: thisguy
"""

from elasticsearch import Elasticsearch, helpers
import sqlite3 as sql
import pymongo as mongo

#elasticsearch
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
    d.pop("_id")

resp = helpers.bulk(
elastic_conn,
data,
index = "addresses",
doc_type = "_doc"
)