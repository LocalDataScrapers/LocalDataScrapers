import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
import datetime
import io
import StringIO
from scraper import Scraper

CSV_URL = 'http://data.phl.opendata.arcgis.com/datasets/e10172ebe7964f63830505457c0d7c2a_0.csv'

class StreetClosureScraper(Scraper):
    schema = 'street-closures'
    primary_key = ('unique_id',)

    def data(self):
        cutoff_date = self.start_date - datetime.timedelta(days=0)
        effective_cutoff_date = self.start_date + datetime.timedelta(days=30)
        for street in self.get_street_closure():
            item_date = self.date(street['effectivedate'],'%Y-%m-%dT%H:%M:%S.000Z')
            address = street['address']
            end_date =self.date(street['expirationdate'],'%Y-%m-%dT%H:%M:%S.000Z')
            address = address.replace('block of ', '')
            if end_date is None:
               end_date
            elif end_date < cutoff_date:
                continue
            if item_date > effective_cutoff_date:
                continue
            yield dict(
            	unique_id= (street['permitnumber'] + street['seg_id'] + street['occupancytype']),
                title=street['occupancytype'],
                item_date=item_date,
                location_name=self.clean_address(address),
                end_date=end_date,
                permit_type=street['permittype'],
                permit_number=street['permitnumber'],
                purpose=street['purpose'],
                status=street['status'],
            )

    def get_street_closure(self):
        	filename = (self.cache_get_to_file('data', 'csv', CSV_URL).encode('utf-8'))
		with io.open(filename, "r" , encoding="utf-8-sig") as fp:
			header = tuple(h.lower() for h in fp.readline().split(','))
			for row in csv.DictReader(fp, fieldnames=header):
				yield row


if __name__ == "__main__":
    StreetClosureScraper().run()
