__author__ = 'chuqiao'
import EventsPortal
EventsPortal.addDataToSolrFromUrl("http://www.elixir-europe.org:8080/events", "http://www.elixir-europe.org:8080/events");
EventsPortal.addDataToSolrFromUrl("http://localhost/ep/events?state=published&field_type_tid=All", "http://localhost/ep/events");
