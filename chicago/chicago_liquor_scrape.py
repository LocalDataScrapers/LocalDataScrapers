"""
Chicago liquor licenses.
"""
import csv
import datetime
import re
from cStringIO import StringIO
from scraper import Scraper

DATA_URL = 'https://data.cityofchicago.org/api/views/nrmj-3kcf/rows.csv'

class LiquorLicenseScraper(Scraper):
    schema = 'liquor-licenses'
    primary_key = ('license_number',)

    def data(self):
        cutoff_date = self.start_date - datetime.timedelta(days=30)
        for license in self.get_liquor_licenses():
            item_date = self.date(license['date issued'],'%m/%d/%Y')
            address = license['address']
            add = license['address']
            addr = re.match("[\d\-]+ \w+ \w+ \w+", add)
            if addr:
                address = addr.group(0)
            expiration_date = self.date(license['license term expiration date'][:10], '%m/%d/%Y')
            
            if item_date == None:
                continue
            elif expiration_date < cutoff_date:
                continue

            yield dict(
                title=license['doing business as name'],
                item_date=item_date,
                location_name=self.clean_address(address),
                expiration_date=self.date(license['license term expiration date'][:10], '%m/%d/%Y'),
                license_number=license['license number'],  # license['lic_id']
                license_category=license['license description'],  # license['lic_name'],
                license_status=license['license status'],
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
