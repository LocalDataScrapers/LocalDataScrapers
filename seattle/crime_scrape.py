"""
Importer for Seattle crime reports.
Browse:
http://data.seattle.gov/Crime/Seattle-Police-Department-Police-Report-Incident/7ais-f98f
Columns:
http://data.seattle.gov/api/views/7ais-f98f/columns.json
"""

import datetime
import json
from string import Template
from scraper import Scraper

BASE_URL = 'http://data.seattle.gov/'
VIEW_ID = '7ais-f98f'
INLINE_PARAMS = {
    "originalViewId": VIEW_ID,
    "name": "Inline Filter",
    "query": {
        "filterCondition": {
            "type": "operator",
            "value": "GREATER_THAN",
            "children": [
                {
                    "columnFieldName": "date_reported",
                    "type": "column"
                },
                {
                    "type": "literal",
                    "value": "${start_date}"
                }
            ]
        }
    }
}

inline_query_template = Template(json.dumps(INLINE_PARAMS))

class CrimeScraper(Scraper):
    schema = 'crime'
    primary_key = ('offense_number',)

    def data(self):
        for report in self.get_incident_reports():
            # Ignore junk rows.
            if report['date_reported'] is None:
                continue
            if report['hundred_block_location'] is None:
                continue

            date_reported = self.datetime(report['date_reported'], '%Y-%m-%dT%H:%M:%S')
            location_name = self.clean_address(report['hundred_block_location'].replace('XX ', '00 '))

            # Skip reports without a full address (e.g. "10800 block of"). We
            # have long/lats for these, but it's bad to publish these on our
            # site without the name of the street.
            if location_name.lower().strip().endswith('block of'):
                continue

            if isinstance(report['longitude'], basestring) and isinstance(report['latitude'], basestring):
                location = self.point(float(report['longitude']), float(report['latitude']))
            else:
                location = None

            yield dict(
                title=report['offense_type'],
                item_date=date_reported.date(),
                location_name=location_name,
                location=location,

                offense_number=report['general_offense_number'],
                time_reported=date_reported.time(),
                date_occurred_begin=self.date(report['occurred_date_or_date_range_start'], '%Y-%m-%dT%H:%M:%S', return_datetime=True),
                date_occurred_end=self.date(report['occurred_date_range_end'], '%Y-%m-%dT%H:%M:%S', return_datetime=True),
                offense_type=report['offense_type'],
                offense_description=report['summarized_offense_description'],
                offense_code='{0}-{1}'.format(report['offense_code'], report['offense_code_extension']),
                offense_code_summary=report['summary_offense_code'],
            )

    def get_incident_reports(self):
        start_date = self.start_date - datetime.timedelta(days=90)
        inline_query = inline_query_template.substitute(
            start_date=start_date.strftime('%Y-%m-%dT00:00:00'))

        for row in self.download_socrata_data(BASE_URL, VIEW_ID, inline_query):
            yield row

if __name__ == "__main__":
    CrimeScraper().run()
