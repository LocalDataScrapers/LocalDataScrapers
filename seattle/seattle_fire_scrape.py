import datetime
import csv
import re
from scraper import Scraper
from dateutil import parser

CSV_URL = 'https://data.seattle.gov/api/views/vcrj-7fx5/rows.csv'

class FireDispatchScraper(Scraper):
   schema = 'fire-dispatch'
   primary_key = ('id',)

   def data(self):
       for request in self.get_dispatch():
           request = dict(map(str.strip,x) for x in request.items())
           try:
               if request['latitude'] and request['longitude']:
                   point_x = float(request['latitude'])
                   point_y = float(request['longitude'])
                   location = self.point(point_y,point_x)
               else:
                   continue
               item_date =parser.parse( request['datetime']).date()
               if item_date < (datetime.datetime.today()-datetime.timedelta(days=60)).date():
                   continue

           except (ValueError, KeyError) as ex:
               print (ex)
               location = None

           yield dict(
               title=request['type'],
               item_date=item_date,
               location=location,
               location_name=request['address'],
               description=request['type']+' '+request['incident number'],
               id=request['incident number'],
           )

   def get_dispatch(self):
      filename = self.cache_get_to_file('data', 'csv', CSV_URL)
      with open(filename) as fp:
          header = tuple(h.lower() for h in fp.readline().split(','))
          for row in csv.DictReader(fp, fieldnames=header):
              yield row

if __name__ == "__main__":
   FireDispatchScraper().run()
