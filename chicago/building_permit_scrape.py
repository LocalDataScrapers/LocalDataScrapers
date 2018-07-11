"""
Screen scraper for City of Chicago building permits.
http://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu
http://data.cityofchicago.org/views/ydr8-5enu/columns.json
"""

from scraper import Scraper
import datetime
import json
import re

DATA_URL = 'http://data.cityofchicago.org/api/views/INLINE/rows.json?method=index'
INLINE_QUERY_TEMPLATE = """{
    "originalViewId": "ydr8-5enu",
    "name": "Inline Filter",
    "query" : {
        "filterCondition" : {
            "type" : "operator",
            "value" : "GREATER_THAN",
            "children": [
                {
                    "columnFieldName" : "_issue_date",
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
permit_type_pattern = re.compile('^PERMIT - (.*)')

class BuildingPermitScraper(Scraper):
    schema = 'building-permits'
    primary_key = ('application_number',)

    def data(self):
        # Build an inline query for building permits issued within the past 7 days:
        start_date = datetime.date.today() - datetime.timedelta(days=7)
        inline_query = INLINE_QUERY_TEMPLATE % {
            'start_date': start_date.strftime('%Y-%m-%dT00:00:00'),
        }
        json_string = self.get(DATA_URL, inline_query, headers={'Content-Type': 'application/json'})

        for record in self.parse_socrata(json_string):
            # Skip the first row, which contains the field labels as the data.
            if record['_permit_type'].strip() == 'PERMIT_TYPE':
                continue

            yield self.convert_record(record)

    def convert_record(self, record):
        "Convert Socrata record into a record that we can insert as a newsitem."
        # Strip the prefix off all permit types.
        match = permit_type_pattern.match(record['_permit_type'])
        work_type = self.smart_title(match.group(1))

        # Sometimes _suffix is None
        record['_suffix'] = record['_suffix'] or ''

        location_name = '%(street_number)s %(street_direction)s %(street_name)s %(_suffix)s' % record
        location_name = self.clean_address(location_name)

        # Despite its formatting, the ISSUE_DATE field never has any time
        # data in it.
        item_date = self.date(record['_issue_date'], '%Y-%m-%dT00:00:00')

        try:
            estimated_value = int(round(float(record['_estimated_cost'])))
        except:
            estimated_value = None

        # If the estimated value is over one trillion, it's probably not
        # reliable, and besides, we might not be able to store it in the
        # database.
        if estimated_value > 1000000000:
            estimated_value = None

        return dict(
            title='Permit issued for ' + work_type.lower(),
            item_date=item_date,
            location_name=location_name,

            application_number=record['permit_'],
            estimated_value=estimated_value,
            work_type=work_type,
            description=record['work_description'],
            issue_time=None,
        )

if __name__ == "__main__":
    BuildingPermitScraper().run()
