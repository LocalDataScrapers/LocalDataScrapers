
"""
Philly Business licenses.
Download page:
https://www.opendataphilly.org/dataset/licenses-and-inspections-business-licenses
"""
import csv
import datetime
from cStringIO import StringIO
from scraper import Scraper

DATA_URL = 'https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+li_business_licenses&filename=li_business_licenses&format=csv&skipfields=cartodb_id,the_geom,the_geom_webmercator'

class ActiveBusinessLicenseScraper(Scraper):
    schema = 'business-licenses'
    primary_key = ('unique_id',)

    def data(self):
        cutoff_date = datetime.date.today() - datetime.timedelta(days=30)
        for license in self.get_business_licenses():
            lic_title= license['business_name']
            if not lic_title:
                lic_title=license['legalname']
            item_date = self.date(license['mostrecentissuedate'], '%Y-%m-%d %H:%M:%S')
            if item_date == None:
                continue
            elif item_date < cutoff_date:
                continue   
            lic_num =license['licensenum']
            if not lic_num:
                continue

            yield dict(
                title=lic_title,
                item_date=item_date,
                location_name=self.clean_address(license['fulladdress']),
                unique_id='{0}'.format(lic_num),
                license_number=lic_num,
                license_category=license['licensetype'],  # license['lic_name'],
                license_status=license['licensestatus'],
            )
           
    def get_business_licenses(self):
        text = self.cache_get('data', 'csv', DATA_URL)
	import pdb;pdb.set_trace()
        sio = StringIO(text)
        reader = csv.reader(sio)
        keys = tuple(t.lower().strip() for t in reader.next())
        for dic in reader:
            for row in reader:
                yield dict(zip(keys, row))


if __name__ == "__main__":
    ActiveBusinessLicenseScraper().run()
