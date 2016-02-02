__author__ = 'chuqiao'
import script
# DELETE ALL DATA
script.deleteDataInSolr()
# ADD DATA FROM 2 SORCES
script.addDataToSolrFromUrl("http://www.elixir-europe.org:8080/events", "http://www.elixir-europe.org:8080/events");
script.addDataToSolrFromUrl("http://localhost/ep/events?state=published&field_type_tid=All", "http://localhost/ep/events");
# DELETE DATA FROM 1 SOURCE
# script.deleteDataInSolrFromUrl("http://www.elixir-europe.org:8080/events")
script.deleteDataInSolrFromUrl("http://localhost/ep/events?state=published&field_type_tid=All")
# script.deleteDataInSolrByQuery('source:("http://www.elixir-europe.org:8080/events")')
# script.deleteDataInSolrByQuery('source:("http://localhost/ep/events" AND "state=published" AND "field_type_tid=All")')