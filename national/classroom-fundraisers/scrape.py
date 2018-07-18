"""
DonorsChoose classroom fundraisers
API docs:
http://data.donorschoose.org/docs/overview/
"""
import itertools
import urllib
from scraper import Scraper


API_KEY = '<Use your api key here>'
BASE_URL = 'http://api.donorschoose.org/common/json_feed.html'
MAX = 50       # this is the maximum the API allows


class DonorsChooseScraper(Scraper):
    schema = 'classroom-fundraisers'
    primary_key = ('unique_id',)
    update = True
    item_date_available = False
    # We have no choice but to turn off fresh_days logic, because we don't have
    # item_dates.
    fresh_days = None

    def data(self):
        bbox = (-71.178935, 42.294597, -71.064095, 42.453445)
        for index in itertools.count(0, MAX):
            params = dict(
                APIKey=API_KEY,
                index=index,
                max=MAX,
                # Note that we have to scramble the numbers because normally the
                # bounding box is given as SW corner, NE corner.
                nwLng=bbox[0],
                nwLat=bbox[3],
                seLng=bbox[2],
                seLat=bbox[1],
            )
            url = BASE_URL + '?' + urllib.urlencode(params)
            root = self.json(self.cache_get('data', 'json', url))
            proposals = root['proposals']
            if not proposals:
                break

            for item in proposals:
                if item['city'] == 'Boston':
			yield dict(
                    		title=self.clean_html(item['title']),
		                url=item['proposalURL'],
                    		# item_date is not available. They might add it to the API at some point.
		                location_name=item['schoolName'],
                		location=self.point(float(item['longitude']), float(item['latitude'])),

		                unique_id=item['id'],
                		image=item['imageURL'],
		                teacher_name=item['teacherName'],
                		excerpt=self.clean_html(item['fulfillmentTrailer']),
		                amount=item['totalPrice'],
                		expiration_date=self.date(item['expirationDate'], '%Y-%m-%d'),
                	)

if __name__ == '__main__':
    DonorsChooseScraper().run()
