"""
Scraper for City of Chicago crime reports.
Main page:
http://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2
Metadata:
http://data.cityofchicago.org/views/ijzp-q8t2/columns.json
"""

import datetime
import re
import json
from scraper import Scraper

DATA_URL = 'http://data.cityofchicago.org/api/views/INLINE/rows.json?method=index'
INLINE_QUERY_TEMPLATE = """{
    "originalViewId": "ijzp-q8t2",
    "name": "Inline Filter",
    "query" : {
        "filterCondition" : {
            "type" : "operator",
            "value" : "GREATER_THAN",
            "children": [
                {
                    "columnFieldName" : "date",
                    "type" : "column"
                },
                {
                    "type" : "literal",
                    "value" : "%(start_date)s"
                }
            ]
        }
    }
}"""

class CrimeScraper(Scraper):
    schema = 'crime'
    primary_key = ('case_number',)
    #title_format = u'{secondary_type.name}'

    def data(self):
        # Build an inline query for crimes occurring within the past 180 days:
        start_date = datetime.date.today() - datetime.timedelta(days=10)
        inline_query = INLINE_QUERY_TEMPLATE % {
            'start_date': start_date.strftime('%Y-%m-%dT00:00:00'),
        }
        json_string = self.get(DATA_URL, inline_query, headers={'Content-Type': 'application/json'})
        for record in self.parse_socrata(json_string):
            yield self.convert_record(record)

    def convert_record(self, record):
        "Convert Socrata record into a record that we can insert as a newsitem."
        if record['longitude'] is None or record['latitude'] is None:
            location = None
        else:
            location = self.point(float(record['longitude']), float(record['latitude']))

        if record['x_coordinate'] is None or record['y_coordinate'] is None:
            xy = None
        else:
            xy = '%s;%s' % (record['x_coordinate'], record['y_coordinate'])

        number, street_name = record['block'].split(' ', 1)
        number = int(number.replace('X', '0'))
        location_name = '%s block %s' % (number, self.clean_address(street_name))

        dt = self.datetime(record['date'], '%Y-%m-%dT%H:%M:%S')

        return dict(
            title=record['description'],
            item_date=dt.date(),
            location=location,
            location_name=location_name,

            is_outdated=False,
            primary_type=record['primary_type'],
            secondary_type=record['iucr'],
            secondary_type__name=record['description'],
            case_number=record['case_number'],
            place=record['location_description'] or '',
            crime_time=dt.time(),
            beat=record['beat'],
            domestic=record['domestic'],
            xy=xy,
            real_address=location_name,
        )

if __name__ == "__main__":
    CrimeScraper().run()
