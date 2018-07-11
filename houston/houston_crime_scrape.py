"""
Houston crime reports.
Map page:
http://mycity.houstontx.gov/recentcrime/index.html
"""

import datetime
import json
import urllib
import re
import pytz
from scraper import Scraper


BASE_URL= 'http://mycity.houstontx.gov/ArcGIS/rest/services/wm/RecentCrime_wm/MapServer/%s/query'

CRIME_TYPES = {
    '1': 'Murder',
    '2': 'Rape',
    '3': 'Aggravated Assault',
    '4': 'Robbery',
    '5': 'Burglary',
    '6': 'Theft',
    '7': 'Auto Theft',
}


class CrimeScraper(Scraper):
    schema = 'crime-reports'
    primary_key = ('offense', 'item_date', 'offense_time', 'premise_type', 'location_name')

    def data(self):
        for crime in self.get_crimes():
            # Flatten the dict.
            record = crime['attributes']
            record.update(crime['geometry'])
            datetime_begun = get_datetime(record['Time_Begun'])
            address_range = record['Address_Range'].strip()
            location_name = re.sub(r'(\d+)\-(\d+)', r'\1 block of', address_range)
            location = self.point(record['x'], record['y'])

            yield {
                'title': record['Offense'],
                'item_date': datetime_begun.date(),
                'location_name': location_name,
                'location': location,

                'offense': record['Offense'],
                'offense_time': datetime_begun.time(),
                'beat': record['HPD_Beat'],
                'premise_type': record['Premise_Type'] or "No premises type specified",

                'details': json.dumps({
                    'point': {'x': record['x'], 'y': record['y']},
                    'zip': record['Zip_Code'],
                    'address_range': address_range,
                    'district': record['HPD_District'],
                    'division': record['HPD_Division'],
                    'snb_name': record['SNB_Name'],
                    'snb_num': record['SNB_No'],
                    'council_district': record['Council_District'],
                })
            }

    def get_crimes(self):
        for police_district in range(1, 25):
            for offense_code in CRIME_TYPES.keys():
                url = get_url(police_district, offense_code)
                result = self.cache_get(
                    '{}_{}'.format(police_district, offense_code),
                    'json', url, make_pretty=True)
                for crime in json.loads(result)['features']:
                    yield crime


def get_url(police_district, offense_code):
    params = {
        # Time_Frame 1 = Last day reported, 2 = Last 7 days, 3 = Last 30 days
        'where': "Time_Frame <= 3 AND HPD_District= '%s'" % police_district,
        'f': 'json',
        'outFields': 'Incident_No,Offense,Time_Begun,Premise_Type,Address_Range,Zip_Code,HPD_Division,HPD_District,HPD_Beat,Council_District,SNB_Name,SNB_No',
        'outSR': '4326',
        'returnGeometry': 'true'
    }
    url = BASE_URL % offense_code
    return '%s?%s' % (url, urllib.urlencode(params))

def get_datetime(timestamp):
    """
    Currently, the timestamp they give is a bit weird. You supposedly have to 
    convert it to UTC to get the actual local time.
    This blog post gives a clue as to how they handle the timestamp on the their 
    crime map site:
    http://alexdinnouti.wordpress.com/2013/09/24/some-samples-in-how-to-convert-esrifieldtypedate-or-esri-date-or-epoch-to-a-date-format-using-javascript/
    """
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)
    dt = pytz.timezone('US/Central').localize(dt)
    return dt.astimezone(pytz.utc)


if __name__ == "__main__":
    CrimeScraper().run()
