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
    sched.add_job(SyncSolr.syncSolr, 'interval', seconds= 10, args=[csvUrl])
    sched.start()
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(20)

    except (KeyboardInterrupt, SystemExit):
        pass


scheduleUpdateSolr("http://localhost:8984/solr/event_portal/select?q=*:*&fl=eventId,name,alternateName,startDate,endDate,description,eventType,keywords,topic,locationName,locationCity,locationCountry,locationPostcode,url,&rows=2147483647&wt=csv")



