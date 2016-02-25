__author__ = 'chuqiao'

import csv
import urllib2
import pysolr
import logging
import datetime
import logging
# logging.basicConfig()
from apscheduler.schedulers.blocking import BlockingScheduler

def logger():
    """
       Function that initialises logging system
    """
    global logger
    # create logger with 'syncsolr'
    logger = logging.getLogger('syncsolr')
    logger.setLevel(logging.DEBUG)

    # specifies the lowest severity that will be dispatched to the appropriate destination

    # create file handler which logs even debug messages
    fh = logging.FileHandler('syncsolr.log')
    # fh.setLevel(logging.WARN)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    # StreamHandler instances send messages to streams
    # ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)


def init(csvUrl):

    logger()
    logger.info('Starting to push data from a url of CSV file to IANN solr')
    syncSolr(csvUrl)

def syncSolr(csvUrl):

    getDataFromCsv(csvUrl)
    docs = getDataFromCsv(csvUrl)
    deleteDataInSolr()
    pushToIannSolr(docs)
    logger.info('Finishing to push data to IANN solr')



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

           # print drowRemoveEmptyValue
           data.append(drowRemoveEmptyValue)

    return data
def deleteDataInSolr():
    """
    delete all the Solr data
    """
    solrUrl = 'http://localhost:8982/solr/iann'
    solr = pysolr.Solr(solrUrl, timeout=10)
    query = '*:*'
    solr.delete(q='%s' % query)

def pushToIannSolr(docs):
    """
    Adds data to Iann SOLR from a SOLR data structure
    """
    solr = pysolr.Solr('http://localhost:8982/solr/iann', timeout=10)

    solr.add(
        docs
    )



# def scheduleUpdateSolr(csvUrl):
#     """
#
#     """
#     # logger.info('***Starting update every minute***')
#     sched = BlockingScheduler()
#     sched.add_job(syncSolr, 'interval', seconds= 20, args=[csvUrl])
#     sched.start()
#     try:
#         # Keeps the main thread alive.
#         while True:
#             time.sleep(20)
#
#     except (KeyboardInterrupt, SystemExit):
#         pass


# scheduleUpdateSolr("http://localhost:8984/solr/event_portal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,url,&rows=2147483647&wt=csv""http://localhost:8984/solr/event_portal/select?q=*:*&fl=eventId,name,alternateName,startDate,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,url,&rows=2147483647&wt=csv")

init("http://localhost:8984/solr/event_portal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,url,&rows=2147483647&wt=csv")
# init()
