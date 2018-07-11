from scraper import Scraper
import datetime
import csv
import re

CSV_URL = 'https://phl.carto.com/api/v2/sql?q=SELECT+status,+permitdescription,+permitnumber,+permitissuedate,+address,+zip,+typeofwork,+censustract,+descriptionofwork,+ST_Y(the_geom)+AS+lat,+ST_X(the_geom)+AS+lng+FROM+li_permits&filename=li_permits&format=csv&skipfields=cartodb_id'

class BuildingPermitScraper(Scraper):
        schema = 'building-permits'
        primary_key = ('permit_number',)

        def data(self):
                cutoff_date = self.start_date - datetime.timedelta(days=90)
                for request in self.get_buildings():
                        request = dict(map(str.strip,x) for x in request.items())

                        try:
                                address = request['address']
                                location_name = address
                                item_date = request['permitissuedate']
                                item_date = datetime.datetime.strptime(item_date,'%Y-%m-%d %H:%M:%S')

                                if item_date.date() < cutoff_date:
                                        continue

                        except ValueError as ex:
                                continue

                        yield dict(
                                title=request['permitdescription'],
                                item_date=item_date.date(),
                                location_name=location_name,
                       	        description=request['descriptionofwork'],
                                permit_type=request['permitdescription'],
                                permit_number=request['permitnumber'],
                                tract=request['censustract'],
                                work_type=request['typeofwork'],
                                permit_status=request['status'],
                                )

        def get_buildings(self):
                filename = self.cache_get_to_file('data', 'csv', CSV_URL)
                with open(filename) as fp:
                        header = tuple(h.lower() for h in fp.readline().split(','))
                        for row in csv.DictReader(fp, fieldnames=header):
                                yield row


if __name__ == "__main__":
        #from everyblock.retrieval import log_debug
        BuildingPermitScraper().run()
