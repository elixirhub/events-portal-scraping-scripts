__author__ = 'chuqiao'
import EventsPortal
from datetime import datetime
import logging


def logger():
    """
       Function that initialises logging system
    """
    global logger
    # create logger with 'syncsolr'
    logger = logging.getLogger('adddata')
    logger.setLevel(logging.DEBUG)

    # specifies the lowest severity that will be dispatched to the appropriate destination

    # create file handler which logs even debug messages
    fh = logging.FileHandler('adddata.log')
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



# EventsPortal.addDataToSolrFromUrl("http://www.elixir-europe.org:8080/events", "http://www.elixir-europe.org:8080/events");
logger()
logger.info('start at %s' % datetime.now())
# EventsPortal.addDataToSolrFromUrl("http://bioevents-portal.org/eventsfull/test?state=published&field_type_tid=All", "http://bioevents-portal.org/events","http://139.162.217.53:8983/solr/eventsportal");
# EventsPortal.addDataToSolrFromUrl("http://bioevents-portal.org/eventsfull", "http://bioevents-portal.org/events","http://139.162.217.53:8983/solr/eventsportal");
# EventsPortal.addDataToSolrFromUrl("http://bioevents-portal.org/eventsfull/upcoming?state=published&field_type_tid=All", "http://bioevents-portal.org/events","http://139.162.217.53:8983/solr/eventsportal");



# EventsPortal.addDataToSolrFromUrl("http://bioevents-portal.org/eventsfull", "http://bioevents-portal.org/events","http://localhost:8983/solr/event_portal");

EventsPortal.addDataToSolrFromUrl("http://bioevents-portal.org/eventsfull/test", "http://bioevents-portal.org/events","localhost:8983/solr/event_portal")
logger.info('finish at %s' % datetime.now())


if  __name__ == '__main__':

    EventsPortal.addDataToSolrFromUrl("http://bioevents-portal.org/eventsfull/test", "http://bioevents-portal.org/events","localhost:8983/solr/event_portal")
