__author__ = 'chuqiao'
import EventsPortal
# DELETE ALL DATA
# EventsPortal.deleteDataInSolr()
# ADD DATA FROM 2 SORCES

# EventsPortal.addDataToSolrFromUrl("http://www.elixir-europe.org:8080/events", "http://www.elixir-europe.org:8080/events")
# EventsPortal.addDataToSolrFromUrl("http://bioevents-portal.org/eventsfull/test?state=published&field_type_tid=All", "http://bioevents-portal.org/events","localhost:8983/solr/event_portal")

# DELETE DATA FROM 1 SOURCE

EventsPortal.deleteDataInSolrFromUrl("http://bioevents-portal.org/eventsfull/test?state=published&field_type_tid=All","139.162.217.53:8983/solr/eventsportal")



# EventsPortal.deleteDataInSolrByQuery('source:("http://www.elixir-europe.org:8080/events")')
# EventsPortal.deleteDataInSolrByQuery('source:("http://localhost/ep/events" AND "state=published" AND "field_type_tid=All")')