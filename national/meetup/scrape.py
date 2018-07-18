from citynames import names_for_metro
from scraper import Scraper
import datetime
import urllib
import requests

API_KEY = '<Use your api key>' #API key needs to get from Meetup

# Number of days in advance of a meetup that we import/publish the data.
DAYS_IN_ADVANCE = 8

class MeetupScraper(Scraper):
    schema = 'meetups'
    primary_key = ('unique_id',)

    def data(self):
        cities = names_for_metro('chicago')
        next_url = 'https://api.meetup.com/2/open_events.xml/?country=us&state=%s&city=%s&key=%s&time=1d,%sd' % ('il', urllib.quote_plus('chicago'), API_KEY, DAYS_IN_ADVANCE)

        while next_url is not None:

            #page = self.cache_get('data', 'xml', next_url, headers={
            #    'Accept-Charset': 'utf-8'})
            # WTF Meetup? Some aspect of the normal get request isn't liked by Meetup
            # Until we figure that out just use requests.
            page = requests.get(next_url).text.encode('utf-8')

            # Determine whether there's a next URL. The API is nice and
            # includes the next URL if there's more data. If not, it'll be
            # None.
            next_url = list(self.xpath(page, '//next'))[0].text

            for meetup in self.xpath(page, '//item'):
                meeting_name = meetup.find('name').text

                # The utc_time is given in milliseconds since the epoch, instead of seconds,
                # so divide by 1000.
                item_datetime = datetime.datetime.fromtimestamp(int(meetup.find('time').text) / 1000)
                item_date = item_datetime.date()

                # Skip meetups in the suburbs.
                if meetup.find('venue/city') is None or meetup.find('venue/city').text not in cities:
                    continue

                if meetup.find('venue/name') is not None:
                    location_name = meetup.find('venue/name').text
                else:
                    continue
                if meetup.find('group/name') is not None:
                    group_name = meetup.find('group/name').text
                else:
                    continue

                # Figure out the long/lat. First check for venue-specific data,
                # then fall back to group-specific data.
                location = None
                if meetup.find('venue/lat') is not None and meetup.find('venue/lon') is not None:
                    try:
                        location = self.point(float(meetup.find('venue/lon').text), float(meetup.find('venue/lat').text))
                    except TypeError: # Guard against bad data (non-float).
                        pass
                if location is None and meetup.find('group/group_lon') is not None and meetup.find('group/group_lat') is not None:
                    try:
                        location = self.point(float(meetup.find('group/group_lon').text), float(meetup.find('group/group_lat').text))
                    except TypeError: # Guard against bad data (non-float).
                        pass
                if location is None:
                    continue # Skip meetups with no long/lat.

                image = None
                if meetup.find('photo_url') is not None and meetup.find('photo_url').text.strip():
                    image = meetup.find('photo_url').text.strip()

                yield {
                    'title': meeting_name,
                    'item_date': item_date,
                    'url': meetup.find('event_url').text,
                    'location': location,
                    'location_name': location_name,

                    'image': image,
                    'item_datetime': item_datetime,
                    'unique_id': 'meetup:%s' % meetup.find('id').text,
                    'group_name': group_name,
                    'meeting_name': meeting_name,
                }

if __name__ == "__main__":
    MeetupScraper().run()
