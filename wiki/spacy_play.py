#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 24 06:55:08 2021

@author: thisguy
"""

import spacy
from spacy import displacy
import glob
import re
import json
from collections import defaultdict

#import os

fdir = "/home/thisguy/data/enwiki20201020/"
test_file = fdir + "0aaf9aae-5557-466c-8c50-13170a3cbec8-modified.json"
with open(test_file, "r") as f:
    data = json.load(f)
    
# Load English tokenizer, tagger, parser and NER
nlp = spacy.load("en_core_web_lg")

# Process whole documents
text = data[0]['text']
doc = nlp(text)

# entities and corresponding labels
for ent in doc.ents:
    print(ent, ent.label_)

# specifically geoparsing

#entity disaplay
#displacy.render(doc, jupyter=True, style='ent')
#displacy.serve(doc, style="ent")

#dependency display
#displacy.render(doc, style='dep', jupyter = True, options = {'distance': 120})
#displacy.serve(doc, style="dep")

#do a little cleaning
category_regex = re.compile('Category\:([\s\w]+)(?:Category\:)')
references_regex = re.compile('[\=\s]{1,}References[\=\s]{1,}')
#whole json
def clean_text(text_dict):
    text_dict['raw_text'] = text_dict['text']
    text_dict['text'] = re.split(references_regex, text)[0]
    text_dict['cats'] = [x.strip() for x in re.split(category_regex, text_dict['raw_text'])[1:]]
    
    return text_dict
 

#parallelize this?
from dask.distributed import Client, progress, LocalCluster
import dask.bag as dbag

cluster = LocalCluster()
client = Client(cluster)

bag = dbag.read_text(test_file).map(json.loads).flatten()
#nvm, this gets sad. need to setup a spacy server. otherwise, it's single-threaded
records_bag = bag.map(clean_text).compute()


#using spacy nlp.pipe streams the documents. supposedly faster.    
texts = [item['text'] for item in records_bag]
nlp = spacy.load("en_core_web_lg")

ents_list = []
for doc in nlp.pipe(texts, disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"], batch_size=50):
    d = defaultdict(list)
    for ent in doc.ents:
        d[ent.label_].append(ent.text)
        
    ent_types = list(d.keys())
    d['ent_types'] = ent_types
    ents_list.append(d)
    
#combine the NER results with the cleaned text
for num, record in enumerate(records_bag):
    to_add = ents_list[num]
    record.update(to_add)
    


#some smart thinking needs to be done about sqlite tables
import sqlite3 as sql
import elasticsearch as es
import pandas as pd

from sqlalchemy import create_engine
engine = create_engine('sqlite:///save_pandas.db', echo=True)
sqlite_connection = engine.connect()


sqlite_table = "wiki"
sqlite_connection.close()
#import pymongo as mongo
#mc = mongo.MongoClient()
#db = mc['wiki_test']
#results = db.records.insert_many(records_bag)







# scale it up
files = [f for f in glob.glob(fdir+"*modified.json")]
wiki_subset = files[0:100]

def flatten(record):
    
    




