__author__ = 'chuqiao'

from apscheduler.schedulers.blocking import BlockingScheduler
import logging
logging.basicConfig()
import SyncSolr




def scheduleUpdateSolr(csvUrl):
    """

    """
    # logger.info('***Starting update every minute***')
    sched = BlockingScheduler()
    sched.add_job(SyncSolr.syncSolr, 'interval', seconds= 20, args=[csvUrl])
    sched.start()
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(20)

    except (KeyboardInterrupt, SystemExit):
        pass


scheduleUpdateSolr("http://139.162.217.53:8983/solr/eventsportal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,url,&rows=2147483647&wt=csv")



