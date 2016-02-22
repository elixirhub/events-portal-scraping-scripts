__author__ = 'chuqiao'

import csv
import urllib2
import pysolr
from apscheduler.schedulers.blocking import BlockingScheduler
import time

def init(csvUrl,schedule):
    if schedule == True:
        scheduleUpdateSolr(csvUrl)
    else:
        syncSolr(csvUrl)


def syncSolr(csvUrl):
    getDataFromCsv(csvUrl)
    docs = getDataFromCsv(csvUrl)
    pushToIannSolr(docs)


def getDataFromCsv(csvUrl):
    """
    crawling data form the url and generate a Iann Solr data structure
    """
    url = csvUrl
    response = urllib2.urlopen(url)
    csvReader = csv.reader(response)
    # start from next remove the header
    csvReader.next()

    # create the new header
    header = ['id', 'title', 'subtitle', 'start', 'end', 'description',
              'category', 'keyword', 'field', 'venue', 'city', 'country', 'postcode',
              'link']
    data = []
    for i, row in enumerate(csvReader):

           drow = dict(zip(header, row))

           # remove the keys within the empty values
           drowRemoveEmptyValue = dict((k, v) for k, v in drow.iteritems() if v)
           data.append(drowRemoveEmptyValue)

    return data


def pushToIannSolr(docs):
    """
    Adds data to a Iann SOLR from a SOLR data structure
    """
    solr = pysolr.Solr('http://localhost:8982/solr/iann', timeout=10)

    solr.add(
        docs
    )


def scheduleUpdateSolr(url):
    """

    """
    # logger.info('***Starting update every minute***')
    sched = BlockingScheduler()
    sched.add_job(syncSolr, 'interval', minutes=1, args=[url])
    sched.start()
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        pass


init("http://localhost:8984/solr/event_portal/select?q=*:*&fl=nid,title,subtitle,startdate,endate,description,type,keywords,scientifictype,location_name,location_city,location_country,location_postcode,url,&rows=2147483647&wt=csv", False)

