"""
Seattle Building Permits.
Download page:
DATA_URL = 'https://data.seattle.gov/api/views/mags-97de'
"""
import csv
import datetime
from cStringIO import StringIO
from scraper import Scraper


DATA_URL = 'https://data.seattle.gov/api/views/mags-97de/rows.csv'


class BuildingPermitScraper(Scraper):
    schema = 'building-permits'
    primary_key = ('unique_id',)

    def data(self):
        
        for permit in self.get_building_permit():
            item_date = self.date(permit['application date'], '%m/%d/%Y')
            if item_date == None:
                continue            
            yield dict(
                title='Permit issued for ' +permit['permit type'],
                item_date=item_date,
                location_name=self.clean_address(permit['address']),
                unique_id=permit['application/permit number'],
                expiration_date=self.date(permit['expiration date'], '%m/%d/%Y'),
                permit_number=permit['application/permit number'],
                estimated_value=permit['value'],
                category=permit['category'],
                work_type=permit['permit type'],
                permit_type=permit['permit type'],
                description=permit['description'],
                status=permit['status'],
            )

    def get_building_permit(self):
        text = self.cache_get('data', 'csv', DATA_URL)
        sio = StringIO(text)
        reader = csv.reader(sio)
        keys = tuple(t.lower().strip() for t in reader.next())
        for dic in reader:
            for row in reader:
                yield dict(zip(keys, row))


if __name__ == "__main__":
    BuildingPermitScraper().run()
