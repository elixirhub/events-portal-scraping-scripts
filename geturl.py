__author__ = 'chuqiao'
import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import re
import logging
import pysolr
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import arrow
from urlparse import urlparse


def logger():
    """
       Function that initialises logging system
    """
    global logger
    # create logger with 'event_portal'
    logger = logging.getLogger('new_portal')
    logger.setLevel(logging.DEBUG)

    # specifies the lowest severity that will be dispatched to the appropriate destination

    # create file handler which logs even debug messages
    fh = logging.FileHandler('newportal.log')
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

def init(url, patternUrl,schedule):
    """
       Get the URL and start the widget
    """
    logger()
    logger.info('Connecting to the URL of the Events portal')
    if schedule == True:
        scheduleUpdateSolr(url)
    else:
        updateSolr(url, patternUrl)



def updateSolr(eventsPortalUrl,patternUrl):
    """
       automatic update Solr index at 01:00 everyday.
       should be called by  scheduleUpdateSolr(url)
    """

    # logger.debug(test)
    try:
        deleteDataInSolr()

        currentEventsUrls = getEventsUrls(eventsPortalUrl,patternUrl)

        paginationUrls = getPaginationUrls(currentEventsUrls)

        allNextEventsUrls = getAllNextEventsUrls(paginationUrls,patternUrl)

        allEventsUrls = set(currentEventsUrls + allNextEventsUrls)


        data = getEventData(allEventsUrls)
        addDataToSolr(data)


        logger.info('***Finishing update***')

    except Exception as e:

        logger.error('Can not update Solr')

def getEventsUrls(eventsPortalUrl,patternUrl):
    """
       scrape the link start with events/ with bs4 in html
       convert the path from relative to absolute
    """
    root = urllib2.urlopen(eventsPortalUrl)
    html = root.read()

    # extract thr base url form the events portal url
    # get base URL from input string. Use regular expression

    parsedUrl = urlparse(eventsPortalUrl)
    baseUrl = '{uri.scheme}://{uri.netloc}/'.format(uri=parsedUrl)
    pathUrl = urlparse(patternUrl).path

    # find all 'a'in html/
    soup = BeautifulSoup(html, "lxml")


    if soup.find_all('a', href=re.compile(pathUrl)) != None:
        links = soup.find_all('a', href=re.compile(pathUrl))
        # start with http:"localhost/events" (patternUrl)
    elif soup.find_all('a', href=re.compile(patternUrl)) != None:
        links = soup.find_all('a', href=re.compile(patternUrl))
    else:
        # find all urls
        links = soup.find_all('a')


    # find all urls in links and convert the path from relative to absolute
    results = []
    for row in links:
        link = row.get('href')
        linkNew =urljoin(baseUrl, link)
        results.append(linkNew)

    # removing duplicates from results lists and return to resultsNew list
    resultsNew = list(set(results))
    return resultsNew


def getPaginationUrls(currentEventsUrls):

    """
       scrape the pagination links from current Events list url
    """

    nextPageUrlsResults = []

    for resultNew in currentEventsUrls:
        if re.search('page=', resultNew):
            nextPageUrlsResults.append(resultNew)

    # remove the duplicate in the list
    nextPageUrlsResultsNew = list(set(nextPageUrlsResults))


    return nextPageUrlsResultsNew

def getAllNextEventsUrls(paginationUrls, patternUrl):
    """
       get all next pages events urls base on looping the pagination urls
    """

    allNextEventsUrls = []
    for paginationUrl in paginationUrls:

        nextPagesEventsUrls = getEventsUrls(paginationUrl, patternUrl)

        allNextEventsUrls.append(nextPagesEventsUrls)

    # merged the multiple list to one list
    merged = sum(allNextEventsUrls, [])

    # remove the duplicate in the list
    mergedNew = list(set(merged))


    return  mergedNew

def getEventData(allEventsUrls):
    """
       Get the URL and start the widget
    """
    fields = []
    for eventUrl in allEventsUrls:
        root = urllib2.urlopen(eventUrl)
        html = root.read()

        soup = BeautifulSoup(html,"lxml")
        schema = soup.find_all(typeof="schema:Event sioc:Item foaf:Document")
        # filter the link by typeof ="schema:Event"
        if len(schema) != 0:
            for property in schema:
                title = soup.find(property="schema:name")
                startDate = soup.find('span', {'property': 'schema:startDate'})
                enDate = soup.find('span', {'property': 'schema:enDate'})
                type = soup.find(rel="schema:type")
                scientificType = soup.find(rel="schema:scientificType")
                description = soup.find(property="schema:description")
                url = soup.find( property="schema:url")
                id = soup.find(property="schema:id")
                location = soup.find(property="schema:location")

                field = {}
                field["nid"] = id.text
                field["title"] = title['content']
                field["startdate"] = arrow.get(startDate['content']).datetime.replace(tzinfo=None)
                if enDate != None:
                  field["endate"] = arrow.get(enDate['content']).datetime.replace(tzinfo=None)
                field["type"] = type.text
                field["scientifictype"] =scientificType.text
                field["url"] = url.text
                field["description"] = description.text
                field["location"]= location.text
                fields.append(field.copy())

    return fields


def addDataToSolr(fields):
    """
        Setup a Solr instance. The timeout is optional.
        Index data to solr by solr.add function in pysolr
    """
    solrUrl = 'http://localhost:8983/solr/event_portal'
    solr = pysolr.Solr(solrUrl, timeout=10)
    solr.add(
        fields
            )

def deleteDataInSolr():
    """
      delete all the previous index in Solr
    """
    logger.info('Deleting the data in solr')
    try:
        solrUrl = 'http://localhost:8983/solr/event_portal'
        solr = pysolr.Solr(solrUrl, timeout=10)
        solr.delete(q='*:*')
    except:
        logger.error('Error:Cannot delete data in solr' + solrUrl)

def scheduleUpdateSolr(url):
    """
       Schedule the updataSolr() function to make it running every on hour.
    """
    logger.info('***Starting update every minute***')
    sched = BlockingScheduler()
    sched.add_job(updateSolr, 'interval', minutes=1, args=[url])
    sched.start()
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        pass
        # logger.info('Stopping update')
        # sched.shutdown()
        #To shut down the scheduler




init ("http://localhost/events/events-list?state=published&field_type_tid=All", "http://localhost/events", False)







