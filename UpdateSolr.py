__author__ = 'chuqiao'

from apscheduler.schedulers.blocking import BlockingScheduler
import logging
logging.basicConfig()
import SyncSolr
import sys




def scheduleUpdateSolr(csvUrl,iannSolrUrl):
    """

    """
    # logger.info('***Starting update every minute***')
    sched = BlockingScheduler()
    sched.add_job(SyncSolr.syncSolr, 'interval', minutes= 60, args=[csvUrl,iannSolrUrl])
    sched.start()
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