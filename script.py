__author__ = 'chuqiao'
import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import re
import logging
import pysolr
from apscheduler.schedulers.blocking import BlockingScheduler
import time
from urlparse import urlparse
import urllib



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

# def init(sourceUrl, patternUrl,schedule):
def init():
    """
       Get the URL and start the widget
    """
    logger()
    logger.info('Connecting to the URL of the Events portal')
    # if schedule == True:
    #     scheduleUpdateSolr(sourceUrl)
    # else:
    #     updateSolr(sourceUrl, patternUrl)

def addDataToSolrFromUrl(sourceUrl,patternUrl):
    """
    add data to a Solr index crawling events from a URL
    """
    logger.info('Add data to a Solr index crawling events from a URl "%s"', sourceUrl)
    try:
        currentEventsUrls = getEventsUrls(sourceUrl, patternUrl)
        paginationUrls = getPaginationUrls(currentEventsUrls)
        allNextEventsUrls = getAllNextEventsUrls(paginationUrls, patternUrl)
        allEventsUrls = set(currentEventsUrls + allNextEventsUrls)
        data = getEventData(allEventsUrls, sourceUrl)
        addDataToSolr(data)
    except Exception as e:
        logger.error('Can not update Solr')

def updateSolr(sourceUrl,patternUrl):
    """
       Deletes data from a source URL and updates with new content
    """
    deleteDataInSolrFromUrl(sourceUrl)
    addDataToSolrFromUrl(sourceUrl,patternUrl)
    logger.info('***Finishing update***')


def getEventsUrls(sourceUrl,patternUrl):
    """
       scrape the link start with events/ with bs4 in html
       convert the path from relative to absolute
    """
    root = urllib2.urlopen(sourceUrl)
    html = root.read()

    # extract thr base url form the events portal url
    # get base URL from input string. Use regular expression

    parsedUrl = urlparse(sourceUrl)
    baseUrl = '{uri.scheme}://{uri.netloc}/'.format(uri=parsedUrl)
    pathUrl = urlparse(patternUrl).path

    # find all links tag in html and get url,convert the path from relative to absolute
    soup = BeautifulSoup(html, "lxml")

    results = []

        #start with events/
    if soup.find_all('a', href=re.compile(pathUrl)) != None:
        links = soup.find_all('a', href=re.compile(pathUrl))
        results = []
        for row in links:
            link = row.get('href')
            linkNew =urljoin(baseUrl, link)
            results.append(linkNew)
        # start with http:"localhost/events" (patternUrl)
    elif soup.find_all('a', href=re.compile(patternUrl)) != None:
        links = soup.find_all('a', href=re.compile(patternUrl))
        for row in links:
            link = row.get('href')
            results.append(link)
    else:
        # find all urls
        links = soup.find_all('a')
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

def getEventData(allEventsUrls,sourceUrl):
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
                # startDate = soup.find('span', {'property': 'schema:startDate'})
                # enDate = soup.find('span', {'property': 'schema:enDate'})
                # type = soup.find(rel="schema:type")
                # scientificType = soup.find(rel="schema:scientificType")
                description = soup.find(property="schema:description")
                # url = soup.find( property="schema:url")
                id = soup.find(property="schema:id")
                location = soup.find(property="schema:location")

                field = {}
                field["nid"] = id.text

                field["title"] = title['content']
                # field["startdate"] = arrow.get(startDate['content']).datetime.replace(tzinfo=None)
                # if enDate != None:
                #   field["endate"] = arrow.get(enDate['content']).datetime.replace(tzinfo=None)
                # field["type"] = type.text
                # field["scientifictype"] =scientificType.text
                # field["url"] = url.text
                field["description"] = description.text
                field["location"] = location.text
                field["source"]= sourceUrl
                fields.append(field.copy())

    return fields


def addDataToSolr(docs):
    """
    Adds data to a SOLR from a SOLR data structure (documents)
    """
    solrUrl = 'http://localhost:8983/solr/event_portal'
    solr = pysolr.Solr(solrUrl, timeout=10)
    solr.add(
        docs
            )

def deleteDataInSolr():
    """
    delete all the Solr data
    """
    logger.info('Deleting ALL data in SOLR')
    try:
        solrUrl = 'http://localhost:8983/solr/event_portal'
        solr = pysolr.Solr(solrUrl, timeout=10)
        query = '*:*'
        solr.delete(q='%s' % query)
        logger.info('deleting sourceUrl objects from solr index: "%s"', query)
    except:
        logger.error('Error:Cannot delete data in solr ' + solrUrl)

def deleteDataInSolrByQuery(query):
    """
      delete all the SOLR data using a LUCENE query
    """
    solrUrl = 'http://localhost:8983/solr/event_portal'
    solr = pysolr.Solr(solrUrl, timeout=10)
    solr.delete(q='%s' % query)
    logger.info('deleting sourceUrl objects from solr index: "%s"', query)


def deleteDataInSolrFromUrl(sourceUrl):
    """
      delete all the SOLR data equal to sourceUrl
    """
    logger.info('Deleting data in SOLR by sourceUrl')
    try:
        # encode the sourceUrl
        # sourceUrlNew = urllib.quote_plus(sourceUrl)
        if re.search('[? &]',sourceUrl):
            splitUrl= re.split('[? &]', sourceUrl)
            sourceUrlSplit = '"%s"' % splitUrl[0]+" "+"AND" + " " + '"%s"' % splitUrl[1]+" " + "AND" + " " + '"%s"' % splitUrl[2]
        else:
            sourceUrlSplit = '"%s"' % sourceUrl

        query = 'source:(%s)' %sourceUrlSplit
        deleteDataInSolrByQuery(query)
    except:
        logger.error('Error:Cannot delete data in solr ')

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


init()

# init("http://localhost/ep/events?state=published&field_type_tid=All", "http://localhost/ep/events", False)
# init("http://www.elixir-europe.org:8080/events", "http://www.elixir-europe.org:8080/events", False)






