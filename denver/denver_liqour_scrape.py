"""
Denver liquor licenses.
Download page:
https://www.denvergov.org/media/gis/DataCatalog/liquor_licenses/csv/liquor_licenses.csv
Search and map page:
http://www.denvergov.org/maps/classic/lic
Metadata:
https://www.denvergov.org/media/gis/DataCatalog/liquor_licenses/csv/liquor_licenses.csv
"""
import csv
import datetime
from cStringIO import StringIO
from scraper import Scraper


DATA_URL = 'https://www.denvergov.org/media/gis/DataCatalog/liquor_licenses/csv/liquor_licenses.csv'


class LiquorLicenseScraper(Scraper):
    schema = 'liquor-licenses'
    primary_key = ('unique_id',)

    def data(self):
        cutoff_date = self.start_date - datetime.timedelta(days=90)

        for license in self.get_liquor_licenses():
            item_date = self.date(license['issue_date'][:10], '%Y-%m-%d')
            if item_date < cutoff_date:
                continue

            yield dict(
                title=license['bus_prof_name'],
                item_date=item_date,
                location_name=self.clean_address(license['full_address']),

                unique_id='{0}'.format(license['bfn']),
                expiration_date=self.date(license['end_date'][:10], '%Y-%m-%d'),
                license_number=0,  # license['lic_id']
                license_category=license['licenses'],  # license['lic_name'],
                license_status=license['lic_status'],
            )

    def get_liquor_licenses(self):
        text = self.cache_get('data', 'csv', DATA_URL)
        sio = StringIO(text)
        reader = csv.reader(sio)
        keys = tuple(t.lower().strip() for t in reader.next())
        for dic in reader:
            for row in reader:
                yield dict(zip(keys, row))


if __name__ == "__main__":
    LiquorLicenseScraper().run()
