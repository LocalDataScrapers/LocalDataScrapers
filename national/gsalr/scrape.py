"""
Garage/yard sale events from GSALR.
"""

import datetime
import re
import xmltodict
from scraper import Scraper


FEED_URL_TEMPLATE = 'http://gsalr.com/feed/<Your xml file with gsalr>?loc={city}+{state}'


class GsalrScraper(Scraper):
    """
    Currently don't set mark_unseen_items_as_cancelled to True, because GSALR
    IDs are not stable (the same listing might have different IDs due to how 
    they de-dupe). Note that this will cause some duplicate garage sales to 
    appear on our site.
    """
    source = 'GSALR'
    # mark_unseen_items_as_cancelled = True

    def data(self):
	schema = 'GSALR'
        metro_name = 'chicago' #self.metro['metro_name'].lower()
        state = 'il' #self.metro['state'].lower()

        for listing in self.get_listings(metro_name, state):
            if listing['state'].lower() != state:
                continue

            # Get the coordinate and check that it falls within the metro
            # boundaries. Note that we cannot filter on the city name of the
            # listing because sometimes it's a borough.
	    if listing['longitude'] and listing['latitude']:
		point = self.point(float(listing['longitude']), float(listing['latitude']))
            # Yield one event for every day on which the listing is active.
            dates = self.get_dates(listing['start_date'], listing['end_date'])
            for item_date in dates:
                yield dict(
                    title=listing['title'],
                    item_date=item_date,
                    url=listing['url'],
                    location_name=self.clean_address(listing['address']),
                    location=point,
                    unique_id='gsalr:{0}:{1}'.format(listing['id'], item_date),
                    time=self.get_time_string(listing),
                    description=self.clean_description(listing['description']),
                    is_cancelled=False,
                )

    def get_listings(self, metro_name, state):
        url = FEED_URL_TEMPLATE.format(city=metro_name.replace(' ', '+'),
            state=state)

        xml_str = self.cache_get('feed', 'xml', url)
        result = xmltodict.parse(xml_str)
        for item in result['listings']['listing']:
            item['start_date'] = self.date(item['start_date'], '%Y-%m-%d')
            item['end_date'] = self.date(item['end_date'], '%Y-%m-%d')
            yield item

    def get_dates(self, start_date, end_date):
        """
        Return sequence of dates between start_date and end_date, inclusive.
        """
        yield start_date
        curr_date = start_date
        while True:
            if curr_date == end_date:
                break
            curr_date = curr_date + datetime.timedelta(days=1)
            yield curr_date

    def get_time_string(self, listing):
        result = 'See description'
        try:
            start_time = self.time(listing['start_time'], '%H:%M:%S')
            end_time = self.time(listing['end_time'], '%H:%M:%S')

            result = '%s - %s' % (self.format_time(start_time),
                self.format_time(end_time))

            # If it's a multi-day listing, it's possible that not all times are
            # the same for each day.
            if listing['start_date'] != listing['end_date']:
                result += ' (check description)'
        except:
            pass
        return result

    def clean_description(self, description):
        # Clean up stray '&amp;' occurrences:
        description = description.replace('&amp;', '&')

        # Strip excess whitespace from all lines.
        description = '\n'.join(
            line.strip() for line in description.splitlines())

        # Collapse extraneous newlines.
        return re.sub(r'\n{2,}', '\n\n', description)


if __name__ == '__main__':
    GsalrScraper().run()
