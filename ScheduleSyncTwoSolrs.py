__author__ = 'chuqiao'

from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import SyncSolr
import sys


def logger():
    """
       Function that initialises logging system
    """
    global logger
    # create logger with 'syncsolr'
    logger = logging.getLogger('updatesolr')
    logger.setLevel(logging.DEBUG)

    # specifies the lowest severity that will be dispatched to the appropriate destination

    # create file handler which logs even debug messages
    fh = logging.FileHandler('ScheduleSyncTwoSolrs.log')
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



def scheduleUpdateSolr(csvUrl,iannSolrUrl):
    """

    """
    logger()
    logger.info('***Start updating every hour***')
    sched = BlockingScheduler()
    sched.add_job(SyncSolr.syncSolr, 'interval', minutes= 60, args=[csvUrl,iannSolrUrl])
    sched.start()
    logger.info('***Finished updating every hour***')
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(20)

    except (KeyboardInterrupt, SystemExit):
        pass



# if len(sys.argv) == 3:
#     args = sys.argv
#     scheduleUpdateSolr(args[1],args[2])
# else:
#     scheduleUpdateSolr(
#
#          'http://139.162.217.53:8983/solr/eventsportal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,hostInstitution,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,latitude,longitude,url,&rows=2147483647&wt=csv',
#
#          'http://localhost:8982/solr/iann'
#     )


if  __name__ == '__main__':

    scheduleUpdateSolr("http://139.162.217.53:8983/solr/eventsportal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,hostInstitution,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,latitude,longitude,url,&rows=2147483647&wt=csv",
                       "http://iann.pro/solrdev/iann"
    )

    # scheduleUpdateSolr(sys.argv[1],sys.argv[2])