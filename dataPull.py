import urllib2
import requests
from elasticsearch import Elasticsearch
from datetime import datetime
import time
import re
#
es = Elasticsearch(
     hosts=[{'host': 'search-stocksearch1-p2vrrsp4hvk7jtrxtxirbfl6jy.us-west-2.es.amazonaws.com', 'port': 443}],
     use_ssl=True,
    )



while True:

    page = requests.get('http://finance.yahoo.com/d/quotes.csv?s=AMZN+GOOG+TSLA+SNAP+SNE&f=sl')


    for line in page.iter_lines():

        #tokenize and extract
        tokenized = [x.strip() for x in line.split(',')]
        symbol = tokenized[0][1:-1]

        #extract price
        price = [x.strip() for x in tokenized[1].split('-')][1]
        price = re.sub(r'<b>', '', price)
        price = re.sub(r'</b>"', '', price)

        # assemble json for ES
        doc = {'symbol': symbol, 'price': float(price)}

        # # insert into ES
        es.index(index='stock', doc_type='ticker', body=doc, id= symbol + "|" + datetime.today().strftime("%Y%m%d") + '@' + price)
        print(doc)
    print('\n')

    # recheck for new data in 1.5 minutes
    time.sleep(120)