"""
Importer for City of Chicago food inspections.
Main page:
http://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5
Metadata:
http://data.cityofchicago.org/api/views/4ijn-s7e5/columns.json
"""

from scraper import Scraper
import datetime
import re

DATA_URL = 'http://data.cityofchicago.org/api/views/INLINE/rows.json?method=index'
INLINE_QUERY_TEMPLATE = """{
    "originalViewId": "4ijn-s7e5",
    "name": "Inline Filter",
    "query" : {
        "filterCondition" : {
            "type" : "operator",
            "value" : "GREATER_THAN",
            "children": [
                {
                    "columnFieldName" : "inspection_date",
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

sentence_end_pattern = re.compile(r'[.] ([a-z])')

class FoodInspectionScraper(Scraper):
    schema = 'food-inspections'
    # We switched over to using inspection_id as the primary key on Janurary 8,
    # 2013. Previously the primary key was (license_number, item_date)
    primary_key = ('inspection_id',)
    #primary_key = ('license_number', 'item_date',)

    def data(self):
        for inspection in self.get_food_inspections():
            if inspection['longitude'] is None or inspection['latitude'] is None or inspection['address'] is None:
                continue
            # Either a religious organization or the license # is unknown
            if inspection['license_'] is None or inspection['license_'] == '0':
                continue
            dba_name = self.smart_title(inspection['dba_name'])

            results = inspection['results'] or 'N/A'
            results_past_tense = {
                'Pass': 'passed inspection',
                'Pass w/ Conditions': 'passed inspection with conditions',
                'Fail': 'failed inspection',
                'Out of Business': 'had a license that changed/expired',
                'Business Not Located': 'could not be found',
            }.get(results, 'was inspected')

            if inspection['longitude'] is None:
                location = None
            else:
                location = self.point(float(inspection['longitude']), float(inspection['latitude']))

            location_name = self.clean_address(inspection['address'])

            yield dict(
                title='{} {}'.format(dba_name, results_past_tense),
                item_date=self.date(inspection['inspection_date'], '%Y-%m-%dT00:00:00'),
                location=location,
                location_name=location_name,

                inspection_id=inspection['inspection_id'],
                dba_name=dba_name,
                license_number=inspection['license_'],
                facility_type=inspection['facility_type'],
                risk=inspection['risk'] or 'N/A',
                inspection_type=inspection['inspection_type'],
                results=results,
                violations=self.format_violation(inspection['violations']),
            )

    def get_food_inspections(self):
        # Build an inline query for food inspections performed within the past 90 days:
        start_date = datetime.date.today() - datetime.timedelta(days=90)
        inline_query = INLINE_QUERY_TEMPLATE % {
            'start_date': start_date.strftime('%Y-%m-%dT00:00:00'),
        }
        json_string = self.cache_get('data', 'json', DATA_URL,
            data=inline_query, headers={'Content-Type': 'application/json'})
        return self.parse_socrata(json_string)

    def format_violation(self, text):
        "Decapitalize most of the violation text to make it more readable."
        if not text:
            return ''
        # Lowercase and split by line, by capitalize the start of every line.
        result = '\n'.join(line.capitalize() for line in text.lower().splitlines())
        # Capitalize the first letter of new sentences.
        result = sentence_end_pattern.sub(lambda m: '. ' + m.group(1).upper(), result)
        return result

if __name__ == "__main__":
    FoodInspectionScraper().run()
