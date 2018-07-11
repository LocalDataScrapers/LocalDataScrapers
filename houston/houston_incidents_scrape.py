"""
Houston incidents (fire, police, and EMS).
Page:
http://cbtcws.cityofhouston.gov/ActiveIncidents/Combined.aspx
===============================================================================
"""

import re
import lxml.html
from scraper import Scraper
import datetime


PAGE_URL = 'http://cohweb.houstontx.gov/ActiveIncidents/Combined.aspx'


class IncidentScraper(Scraper):
    schema = 'active-incidents'
    primary_key = ('agency', 'address', 'call_time', 'incident_type')

    def data(self):
        for item in self.get_incidents():
            try:
                dt = datetime.datetime.strptime(item['call time(opened)'], '%m/%d/%Y %H:%M')
            except ValueError:
                continue

            address = self.clean_html(item['address'])
            cross_street = self.clean_html(item['cross street'])

            if re.match('^\d', address) or cross_street == '':
                location_name = address
            else:
                cross_street = cross_street.lstrip('BLK ')
                location_name = '%s and %s' % (address, cross_street)

            yield dict(
                title=item['incident type'],
                item_date=dt.date(),
                pub_date=self.snap_date_back(3, 0),
                location_name=self.clean_address(location_name),

                address=address,
                agency=item['agency'],
                cross_street=cross_street,
                key_map=item['key map'],
                call_time=dt.time(),
                incident_type=item['incident type'],
                combined_response=item['combined response'],
            )

    def get_incidents(self):
        html = self.cache_get('page', 'html', PAGE_URL)
        tree = lxml.html.fromstring(html)
        table = tree.xpath('//table[@id = "GridView2"]')[0]
        header_row = table.xpath('tr/th/*/text()')

        keys = []
        for key in header_row:
            # Keys should be lowercase.
            keys.append(key.lower())

        table_rows = []
        data_rows = table.xpath('tr')
        for data_row in data_rows:
            row = [td.text_content().strip() for td in data_row.xpath('.//td')]
            table_rows.append(row)

        table_rows.pop(0)
        for r in table_rows:
            yield dict(zip(keys, r))

if __name__ == "__main__":
    IncidentScraper().run()
