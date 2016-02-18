__author__ = 'chuqiao'

import csv
import urllib2
import pysolr


def init(csvUrl):
    getDataFromCsv(csvUrl)
    docs = getDataFromCsv(csvUrl)
    syncIannSolr(docs)

def getDataFromCsv(csvUrl):
    url = csvUrl
    response = urllib2.urlopen(url)
    csvReader = csv.reader(response)
    csvReader.next()
    header = ['id', 'title', 'subtitle', 'start', 'end', 'description',
              'category', 'keyword', 'field', 'venue', 'city', 'country', 'postcode',
              'link']
    data = []
    for i, row in enumerate(csvReader):

           drow = dict(zip(header, row))
           drowRemoveEmptyValue = dict((k, v) for k, v in drow.iteritems() if v)
           data.append(drowRemoveEmptyValue)

    return data


def syncIannSolr(docs):
    solr = pysolr.Solr('http://localhost:8982/solr/iann', timeout=10)

    solr.add(
        docs
    )

init('http://localhost:8984/solr/event_portal/select?q=*:*&fl=nid,title,subtitle,startdate,endate,description,type,keywords,scientifictype,location_name,location_city,location_country,location_postcode,url,&rows=2147483647&wt=csv')

