"""
Denver Active Business licenses.
Download page:
hhttps://www.denvergov.org/opendata/dataset/city-and-county-of-denver-active-business-licenses
Contact:
denvergis@denvergov.org
"""
import csv
import datetime
from cStringIO import StringIO
from scraper import Scraper

DATA_URL = 'https://www.denvergov.org/media/gis/DataCatalog/active_business_licenses/csv/active_business_licenses.csv'

class ActiveBusinessLicenseScraper(Scraper):
    schema = 'business-licenses'
    primary_key = ('unique_id',)

    def data(self):
        
        for license in self.get_business_licenses():
            item_date = self.date(license['expiration_date'], '%m/%d/%Y %H:%M:%S %p')
            if item_date == None:
                continue
            lic_num = license['bfn']
            sp=lic_num.split('-')
            lic_id=int(sp[2])
            yield dict(
                title=license['entity_name'],
                item_date=item_date,
                location_name=self.clean_address(license['establishment_address']),
                unique_id='{0}'.format(license['bfn']),
                expiration_date=self.date(license['expiration_date'], '%m/%d/%Y %H:%M:%S %p'),
                license_number=lic_id,  # license['lic_id']
                license_category=license['license_type'],  # license['lic_name'],
                license_status=license['license_status'],
            )

    def get_business_licenses(self):
        text = self.cache_get('data', 'csv', DATA_URL)
        sio = StringIO(text)
        reader = csv.reader(sio)
        keys = tuple(t.lower().strip() for t in reader.next())
        for dic in reader:
            for row in reader:
                yield dict(zip(keys, row))


if __name__ == "__main__":
    ActiveBusinessLicenseScraper().run()
