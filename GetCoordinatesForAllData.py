__author__ = 'chuqiao'

from geopy.geocoders import Nominatim




import EventsPortal




def getCoordinatesForAllData(sourceUrl,patternUrl):

    data = EventsPortal.getAllEventsData(sourceUrl,patternUrl)


    for docs in data:
        latitudeField = docs['latitude']

        if latitudeField == None:

            geocity = docs['locationCity']

            geolocator = Nominatim()
            locatorCity = geocity

            location = geolocator.geocode(locatorCity)

            latitude = location.latitude

            longitude = location.longitude

            docs['latitudetest'] = latitude

            docs['longitudetest'] = longitude

        else:
            pass

    return  data



def addAllDataToSolrFromUrl(sourceUrl,patternUrl,solrUrl):
    """
    add data to a Solr index crawling events from a URL
    """

    data = getCoordinatesForAllData(sourceUrl,patternUrl)

    EventsPortal.addDataToSolr(data, solrUrl)


if __name__ == '__main__':

 addAllDataToSolrFromUrl("http://bioevents-portal.org/eventsfull/test?state=published&field_type_tid=All",
                      "http://bioevents-portal.org/events",
                      "139.162.217.53:8983/solr/eventsportal/")