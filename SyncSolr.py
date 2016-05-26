__author__ = 'chuqiao'

import csv
import urllib2
import pysolr
import logging
import sys
reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(filename='syncsolr.log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S', filemode='w', level=logging.DEBUG)

# def logger():
#     """
#        Function that initialises logging system
#     """
#     global logger
#     # create logger with 'syncsolr'
#     logger = logging.getLogger('syncsolr')
#     logger.setLevel(logging.DEBUG)
#
#     # specifies the lowest severity that will be dispatched to the appropriate destination
#
#     # create file handler which logs even debug messages
#     fh = logging.FileHandler('syncsolr.log')
#     # fh.setLevel(logging.WARN)
#
#     # create console handler and set level to debug
#     ch = logging.StreamHandler()
#     # StreamHandler instances send messages to streams
#     # ch.setLevel(logging.DEBUG)
#
#     # create formatter and add it to the handlers
#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     fh.setFormatter(formatter)
#     ch.setFormatter(formatter)
#     # add the handlers to the logger
#     logger.addHandler(ch)
#     logger.addHandler(fh)


def init(csvUrl,iannSolrUrl):
    # logger()

    logging.info('****Starting synchronizing***')
    syncSolr(csvUrl,iannSolrUrl)


def syncSolr(csvUrl,iannSolrUrl):

    try:

        logging.info("push data from a url of CSV file to IANN solr")
        getDataFromCsv(csvUrl)
        docs = getDataFromCsv(csvUrl)
        deleteDataInSolr(iannSolrUrl)
        pushToIannSolr(docs,iannSolrUrl)
        logging.info('***Finished synchronizing***')

    except Exception as e:
        logging.error('***Synchronize failed*** \n%s' % str(sys.exc_info()))


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
    header = ['id', 'title', 'subtitle', 'start', 'end', 'provider', 'description',
              'category', 'keyword', 'field', 'venue', 'city', 'country', 'postcode','latitude','longitude',
              'link']
    data = []
    for column in csvReader:


           drow = dict(zip(header, column))

           # transfer different values
           if drow['category'] == 'Receptions and networking':
               drow['category'] = "meeting"

           elif drow['category']== 'Meetings and conferences':
               drow['category'] = 'meeting'

           elif drow['category']=='Awards and prizegivings':
               drow['category'] ='meeting'
           else:
               drow['category'] ='course'

           # give the start date value to end date if the end date is none
           if drow['end'] != '':
              drow['end'] = drow['end']
           else:
               drow['end']= drow['start']


           # replace slash  to none in keyword string
           keywordValue = drow['keyword']

           listKeywordValue = keywordValue.replace('\\,',',')

           myKeywordlist = listKeywordValue.replace(' ','').split(',')
           drow['keyword'] = myKeywordlist


           fieldValue = drow['field']

           listFieldValue = fieldValue.replace('\\,',',')

           myFieldlist = listFieldValue.replace(' ','').split(',')
           drow['field'] = myFieldlist

           # replace slash  to none in Venue string
           venueValue = drow['venue']
           listVenueValue = venueValue.replace('\\,',',')
           drow['venue'] = listVenueValue


           # insert value events into category
           categoryValue = drow['category']
           listCategories = [categoryValue, "event"]
           drow['category'] = listCategories


           # remove the keys with the empty values
           drowRemoveEmptyValue = dict((k, v) for k, v in drow.iteritems() if v)


           # add dict to a data list
           data.append(drowRemoveEmptyValue)


    return data
def deleteDataInSolr(iannSolrUrl):
    """
    delete all the Solr data
    """
    # solrUrl = 'http://localhost:8982/solr/iann'
    solr = pysolr.Solr(iannSolrUrl, timeout=10)
    query = '*:*'
    solr.delete(q='%s' % query)



def pushToIannSolr(docs,iannSolrUrl):
    """
    Adds data to Iann SOLR from a SOLR data structure
    """
    solr = pysolr.Solr(iannSolrUrl, timeout=10)

    solr.add(
        docs
    )



# if len(sys.argv) == 3:
#     args = sys.argv
#     init(args[1],args[2])
# else:
#     init(
#         "http://139.162.217.53:8983/solr/eventsportal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,hostInstitution,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,latitude,longitude,url,&rows=2147483647&wt=csv",
#         "http://iann.pro/solrdev/iann"
#     )


if __name__ == '__main__':

    init(
        "http://139.162.217.53:8983/solr/eventsportal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,hostInstitution,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,latitude,longitude,url,&rows=2147483647&wt=csv",
        "http://iann.pro/solr/iann"


    )