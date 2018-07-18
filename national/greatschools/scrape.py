"""
School reviews from GreatSchools.
API docs: 
http://www.greatschools.net/api/docs/schoolReviews.page
"""

from scraper import Scraper
import re

API_KEY = '<Use your api key for Great Schools>' #API key needs to be passed
API_URL_TEMPL = 'http://api.greatschools.org/reviews/city/{state}/{city}?limit=60&key=' + API_KEY


class GreatschoolsScraper(Scraper):
    schema = 'school-reviews'
    # We need to add item_date to the primary_key because the review_id we
    # extract from the URL is NOT unique. Instead, it refers to the latest
    # review written for a given school by a given reviewer.
    primary_key = ('review_id', 'source', 'item_date',)
    update = False

    def data(self):
	api_url = API_URL_TEMPL.format(
            state='il',
            city='chicago'.replace(' ', '-'),
        )
        page = self.get(api_url)
        for el in self.xpath(page, '//review'):
            if el.find('rating') is not None:
                stars = int(el.find('rating').text)
            else:
                stars = None # rating might be missing
            address = el.find('schoolAddress').text.split(', \n')[0] # '5035 W North Ave, \nChicago, IL  60639' --> '5035 W North Ave'
            school_name = self.clean_html(el.find('schoolName').text)
            url = self.clean_html(el.find('reviewLink').text)
            try:
                submitter = el.find('submitter').text
            except AttributeError:
                submitter = ''
            title = u'%s reviewed' % school_name
            if submitter in ('teacher', 'parent', 'student'):
                title += u' by a %s' % submitter
            yield {
                'title': title,
                'url': url,
                'item_date': self.date(el.find('postedDate').text, '%Y/%m/%d'),
                'location_name': u'%s (%s)' % (school_name, address),
                'location_name_geocoder': address,

                'review_id': re.findall(r'\#ps(\d+)$', url)[0],
                'school_name': school_name,
                'stars': stars,
                'excerpt': self.clean_html(el.find('comments').text),
                'source': 'GreatSchools',
                'submitter': submitter,
            }

if __name__ == '__main__':
    GreatschoolsScraper().run()
