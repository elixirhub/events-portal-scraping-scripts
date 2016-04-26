__author__ = 'chuqiao'


from apscheduler.schedulers.blocking import BlockingScheduler
import EventsPortal
import sys




def scheduleUpdateSolr(sourceUrl,patternUrl,solrUrl):
    """

    """

    # logger.info('***Starting update every hour***')
    sched = BlockingScheduler()
    sched.add_job(EventsPortal.addDataToSolrFromUrl, 'interval', minutes= 60, args=[sourceUrl,patternUrl,solrUrl])
    sched.start()
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(20)

    except (KeyboardInterrupt, SystemExit):
        pass


if  __name__ == '__main__':

    scheduleUpdateSolr("http://bioevents-portal.org/eventsfull/test?state=published&field_type_tid=All",
                       "http://bioevents-portal.org/events",
                       "139.162.217.53:8983/solr/eventsportal/"
    )

    # scheduleUpdateSolr(sys.argv[1],sys.argv[2])
