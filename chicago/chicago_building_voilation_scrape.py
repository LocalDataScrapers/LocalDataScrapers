"""
Screen scraper for City of Chicago building voilations.
https://data.cityofchicago.org/api/views/22u3-xenr/rows.csv
"""

from scraper import Scraper
import datetime
import csv
import re

CSV_URL = 'https://data.cityofchicago.org/api/views/22u3-xenr/rows.csv'

class BuildingViolationScraper(Scraper):
   schema = 'building-violations'
   primary_key = ('id',)

   def data(self):
       for request in self.get_voilations():
           request = dict(map(str.strip,x) for x in request.items())
           try:
               if request['location'] and not request['location'].isspace():
                   point_x = float(request['latitude'])
                   point_y = float(request['longitude'])
                   location = self.point(point_y,point_x)
               else:
                   continue

               item_date = request['violation last modified date']
               item_date = datetime.datetime.strptime(item_date,'%m/%d/%Y')
               if item_date < datetime.datetime.today()-datetime.timedelta(days=60):
                   continue

           except (ValueError, KeyError) as ex:
               print (ex)
               location = None

           yield dict(
               title=request['violation description'],
               item_date=item_date.date(),
               location=location,
               location_name=request['address'],
               description=request['violation description'],
               id=request['id'],
               violation_code=request['violation code'],
               violation_status=request['violation status'],
               violation_description=request['violation description'],
               violation_inspector_comments=request['violation inspector comments'],
               violation_ordinance=request['violation ordinance'],
           )

   def get_voilations(self):
      """
      Return a dict object for every line in the CSV file.
      Note that we download this file to disk first because it's fairly large.
      Also, we lowercase the field names.
      """
      filename = self.cache_get_to_file('data', 'csv', CSV_URL)
      with open(filename) as fp:
          header = tuple(h.lower() for h in fp.readline().split(','))
          for row in csv.DictReader(fp, fieldnames=header):
              yield row

if __name__ == "__main__":
   BuildingViolationScraper().run()
