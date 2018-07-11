"""
Boston Food Inspections.
Download page:
https://data.boston.gov/dataset/food-establishment-inspections/resource/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c
"""
import csv
import datetime
from cStringIO import StringIO
from scraper import Scraper


DATA_URL = 'https://data.boston.gov/dataset/03693648-2c62-4a2c-a4ec-48de2ee14e18/resource/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c/download/mayorsfoodcourt.csv'


class FoodInspectionScraper(Scraper):
   schema = 'food-inspections'
   primary_key = ('unique_id',)

   def data(self):
       cutoff_date = self.start_date - datetime.timedelta(days=30)

       for item in self.get_food_inspections():
           if item['issdttm'] and not item['issdttm'].isspace():
              item_date = datetime.datetime.strptime(item['issdttm'],'%Y-%m-%d %H:%M:%S')
              if item_date.date() < cutoff_date:
                  continue
            
              if item['violation'] and not item['violation'].isspace():
                  violation=item['violation']+' '+item['comments']
              else:
                  violation='Unavailable'
            
              yield dict(
                  title=item['businessname'],
                  item_date=item_date.date(),
                  location_name=self.clean_address(item['address']),
                  description=item['descript'].decode("windows-1252").encode("utf8"),
                  unique_id='{0}'.format(item['violation']+item['issdttm']+item['licenseno']),
                  violation=violation,
                  businessname=item['businessname'],
                  license_number='{0}'.format(item['licenseno']),
                  expiration_date=datetime.datetime.strptime(item['expdttm'],'%Y-%m-%d %H:%M:%S'),
                  license_category=item['licensecat'],  # license['lic_name'],
                  status=item['violstatus'] or 'Unavailable',
              )
           else:
               continue

   def get_food_inspections(self):
        text = self.cache_get('data', 'csv', DATA_URL)
        sio = StringIO(text)
        reader = csv.reader(sio)
        keys = tuple(t.lower().strip() for t in reader.next())
        for dic in reader:
            for row in reader:
                yield dict(zip(keys, row))


if __name__ == "__main__":
    FoodInspectionScraper().run()
