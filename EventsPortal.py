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
import arrow
from httplib import BadStatusLine
from socket import error as SocketError
import errno
import ConfigParser





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


def init():
    """
       Get the URL and start the widget
    """
    logger()
    logger.info('Connecting to the URL of the Events portal')

def addDataToSolrFromUrl(sourceUrl,patternUrl,solrUrl):
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
        addDataToSolr(data,solrUrl)
    except Exception as e:
        logger.error('Can not update Solr')

def getAllEventsData(sourceUrl,patternUrl):
    """
    get all events data crawling from a URL
    """
    logger.info('crawling events from a URl "%s"', sourceUrl)
    try:
        currentEventsUrls = getEventsUrls(sourceUrl, patternUrl)
        paginationUrls = getPaginationUrls(currentEventsUrls)
        allNextEventsUrls = getAllNextEventsUrls(paginationUrls, patternUrl)
        allEventsUrls = set(currentEventsUrls + allNextEventsUrls)
        data = getEventData(allEventsUrls, sourceUrl)

    except Exception as e:
        logger.error('Can not crawling')

    return data


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

    # extract the base url form the events portal url
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
       Get the URL and crawling data from url
    """
    fields = []
    for eventUrl in allEventsUrls:
        try:
            root = urllib2.urlopen(eventUrl)
            html = root.read()
        except urllib2.HTTPError, error:
            html = error.read()
        except BadStatusLine:
            logger.info ("could not fetch %s" % eventUrl)
        except SocketError as e:
            if e.errno != errno.ECONNRESET:
                raise # Not error we are looking for
            else:# Handle error here.
                logger.info ("SocketError %s" % eventUrl)



        soup = BeautifulSoup(html,"lxml")
        schema = soup.find_all(typeof="schema:Event sioc:Item foaf:Document")
        # filter the link by typeof ="schema:Event"
        if len(schema) != 0:
            title = soup.find(property="schema:name")
            startDate = soup.find('span', {'property': 'schema:startDate'})
            endDate = soup.find('span', {'property': 'schema:enDate'})
            eventType = soup.find(rel="schema:type")
            scientificType = soup.find(rel="schema:scientificType")
            description = soup.find(property="schema:description")
            url = soup.find( property="schema:url")
            id = soup.find(property="schema:id")
            keywords = soup.find(rel="schema:keywords")
            subtitle = soup.find(property="schema:alternateName")
            hostInstitution = soup.find(rel="schema:organization")
            contactName = soup.find(property="schema:contactName")
            contactEmail = soup.find(property="schema:email")


            locationName = soup.find('span', {'itemprop' : 'name'})
            locationStreet = soup.find('span', {'itemprop' : 'streetAddress'})
            locationCity = soup.find('span',  {'itemprop': 'addressLocality'})
            locationCountry = soup.find(itemprop= "addressCountry")
            locationPostcode = soup.find('span', {'itemprop': 'postalCode'})
            # locationStreet = soup.find(itemprop="streetAddress")
            latitude = soup.find('abbr', {'class': 'latitude'})
            longitude = soup.find('abbr', {'class': 'longitude'})

            field = {}
            field["eventId"] = id.text
            field["name"] = title['content']
            field["startDate"] = arrow.get(startDate['content']).datetime.replace(tzinfo=None)
            if endDate != None:
              field["endDate"] = arrow.get(endDate['content']).datetime.replace(tzinfo=None)
            if eventType != None:
             field["eventType"] = eventType.text
            if scientificType != None:
             field["topic"] =scientificType.text
            field["url"] = url.text
            if description != None:
             field["description"] = description.text
            if keywords != None:
             field["keywords"] = keywords.text
            if subtitle != None:
              field["alternateName"] = subtitle.text
            if hostInstitution != None:
                field["hostInstitution"] = hostInstitution.text
            if contactName != None:
                field["contactName"]= contactName.text
            if contactEmail != None:
                field["contactEmail"] = contactEmail.text


            if locationName != None:
               field["locationName"] = locationName.text
            if locationStreet != None:
               field["locationStreet"] = locationStreet.text
            field["locationCity"] = locationCity.text.strip()
            field["locationCountry"] = locationCountry.text
            if locationPostcode != None:
               field["locationPostcode"] = locationPostcode.text
            if latitude != None:
                field['latitude']= latitude['title']
            if longitude != None:
                field['longitude']= latitude['title']


            field["source"]= sourceUrl
            fields.append(field.copy())

    return fields


def addDataToSolr(docs,solrUrl):
    """
    Adds data to a SOLR from a SOLR data structure (documents)
    """
    # solrUrl = 'http://localhost:8984/solr/event_portal'
    # solrUrl = 'http://139.162.217.53:8983/solr/eventsportal'
    solr = pysolr.Solr(solrUrl, timeout=10)
    solr.add(
        docs
            )


def getSolrAdminUrl(solrUrl):

    # read ConfigFile.properties to get admin username and password
    user= None
    passw = None
    logger.info("Authentication...")
    try:
        config = ConfigParser.RawConfigParser()
        config.read('ConfigFile.properties')
        usertemp = config.get('AuthenticationSection','solarealm.username') ;
        passwtemp = config.get('AuthenticationSection','solarealm.password') ;
        user = usertemp
        passw = passwtemp

    except Exception as e:
        logger.info ("authenticated users ")

    # combine two urls
    solrUrlAuth = "http://%s:%s@" % (user,passw)
    solrUrlBase = solrUrl
    solrUrlAdmin = solrUrlAuth + solrUrlBase

    return solrUrlAdmin

def deleteDataInSolr(solrUrl):
    """
    delete all the Solr data
    """
    logger.info('start deleting ALL data in SOLR')
    try:

        solr = pysolr.Solr(solrUrl, timeout=10)
        query = '*:*'
        solr.delete(q='%s' % query)
        logger.info('finished deleting ALL data in SOLR: "%s"', query)
    except:
        logger.error('Error:Cannot delete data in solr ' + solrUrl)


def deleteDataInSolrByQuery(query,solrUrl):
    """
      delete all the SOLR data using a LUCENE query
    """

    logger.info('start deleting data by query : %s', query)
    solrUrlAdmin = getSolrAdminUrl(solrUrl)

    solr = pysolr.Solr(solrUrlAdmin, timeout=10)
    solr.delete(q='%s' % query)
    logger.info('Finished deleting data by query :%s', query)


def deleteDataInSolrFromUrl(sourceUrl):
    """
      delete all the SOLR data equal to sourceUrl
    """
    logger.info('start deleting data in SOLR by %s',sourceUrl)
    try:
        splitUrl= re.split('[? &]', sourceUrl)
        sourceUrlSplit = ''
        _and = ' AND '
        for urlPart in splitUrl:
            sourceUrlSplit += '"' + urlPart + '"'
            sourceUrlSplit += _and
        sourceUrlSplit = sourceUrlSplit[:-len(_and)]
        query = 'source:(%s)' %sourceUrlSplit
        deleteDataInSolrByQuery(query)
        logger.info('finished deleting data in SOLR by %s', sourceUrl)
    except:
        logger.error('Error:Cannot delete data in solr ')

# def scheduleUpdateSolr(url):
#     """
#        Schedule the updataSolr() function to make it running every on hour.
#     """
#     logger.info('***Starting update every minute***')
#     sched = BlockingScheduler()
#     sched.add_job(updateSolr, 'interval', minutes=1, args=[url])
#     sched.start()
#     try:
#         # Keeps the main thread alive.
#         while True:
#             time.sleep(2)
#     except (KeyboardInterrupt, SystemExit):
#         pass
#         # logger.info('Stopping update')
#         # sched.shutdown()
#         #To shut down the scheduler

init()






