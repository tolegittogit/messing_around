import requests
from bs4 import BeautifulSoup
import re

base = 'http://stateoftheunion.onetwothree.net/texts/'
index = 'http://stateoftheunion.onetwothree.net/texts/index.html'

#get links from index
header = {"User-Agent":'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1'}
r = requests.get(index, headers=header)
soup = BeautifulSoup(r.text)

link_list = []
for link in soup.find_all("li")[5:]:
    name_date = link.find('a').text
    name, date = name_date.split(",", 1)
    url = link.find('a').get('href').strip()
    url = base + url
    link_list.append({"President":name.strip(),
                      "Date":date.strip(),
                      "URL":url})

#get text from links
for item in link_list:
    url = item['URL']
    req = requests.get(url, headers=header)
    lines = [line.decode() for line in req.iter_content()]
    doc_text = "".join(lines)
    soup = BeautifulSoup(doc_text)
    sotu_text = soup.find('body').text
    content = [doc for doc in sotu_text.split("\n\n") if doc.strip()][3]
    clean_content = content.strip()
    item["Content"] = clean_content

import json
with open("/home/thisguy/data/sotu/sotu.json", 
                "w", encoding="utf-8") as f:
    json.dump(link_list, f)