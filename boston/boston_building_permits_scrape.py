
from scraper import Scraper
import datetime
import csv
import re

CSV_URL = 'https://data.boston.gov/dataset/cd1ec3ff-6ebf-4a65-af68-8329eceab740/resource/6ddcd912-32a0-43df-9908-63574f8c7e77/download/buildingpermits.csv'

class BuildingPermitScraper(Scraper):
   schema = 'building-permits'
   primary_key = ('permit_number',)

   def data(self):
       cutoff_date = self.start_date - datetime.timedelta(days=30)
       for request in self.get_buildings():
           request = dict(map(str.strip,x) for x in request.items())
           try:
               if request['location'] and not request['location'].isspace():
                   point_x = float(request['location'][request['location'].find('(')+1 : request['location'].find(',')])
                   point_y = float(request['location'][request['location'].find(',')+1 : request['location'].find(')')])
                   location = self.point(point_x,point_y)
               else:
                   continue

               item_date = request['issued_date']
               item_date = datetime.datetime.strptime(item_date,'%Y-%m-%d %H:%M:%S')
	       if item_date.date() < cutoff_date:
                  continue
           except (ValueError, KeyError) as ex:
               print (ex)
               location = None

           location_name = request['address']

           yield dict(
               title='Permit issued for ' + request['description'],
               item_date=item_date.date(),
               location=location,
               location_name=location_name,
               description=request['comments'].decode("windows-1252").encode("utf8"),
               permit_number=request['permitnumber'],
               valuation=request['declared_valuation'],
               permit_type=request['permittypedescr'],
               parcel=request['parcel_id'],
               work_type=request['worktype'],
           )

   def get_buildings(self):
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
   BuildingPermitScraper().run()
