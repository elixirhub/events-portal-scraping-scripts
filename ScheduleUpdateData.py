__author__ = 'chuqiao'


from apscheduler.schedulers.blocking import BlockingScheduler
import EventsPortal
import sys
import logging



def logger():
    """
       Function that initialises logging system
    """
    global logger
    # create logger with 'syncsolr'
    logger = logging.getLogger('scheduleAddData')
    logger.setLevel(logging.DEBUG)

    # specifies the lowest severity that will be dispatched to the appropriate destination

    # create file handler which logs even debug messages
    fh = logging.FileHandler('scheduleUpdateData.log')
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



def scheduleUpdateSolr(sourceUrl,patternUrl,solrUrl):
    """

    """
    logger()
    logger.info('***start updating every hour***')
    sched = BlockingScheduler()
    sched.add_job(EventsPortal.updateSolr, 'interval', minutes= 5, args=[sourceUrl,patternUrl,solrUrl])
    sched.start()


    try:
        # Keeps the main thread alive.
        while True:

            time.sleep(20)

    except (KeyboardInterrupt, SystemExit):
        logger.error('Can not schedule add data to solr  \n%s' % str(sys.exc_info()))





if  __name__ == '__main__':

    scheduleUpdateSolr(
                       # "http://bioevents-portal.org/events/upcoming?state=published&field_type_tid=All",
                       "http://bioevents-portal.org/eventsfull/test?state=published&field_type_tid=All",
                       "http://bioevents-portal.org/events",
                       # "139.162.217.53:8983/solr/eventsportal/"
                       "localhost:8983/solr/event_portal"
    )

    # scheduleUpdateSolr(sys.argv[1],sys.argv[2])
