"""
Denver crime reports.
Main page:
http://data.denvergov.org/dataset/city-and-county-of-denver-crime
Metadata:
http://data.denvergov.org/download/gis/crime/metadata/crime.xml
Crime map:
http://www.denvergov.org/police/policedepartment/crimeinformation/crimemap/tabid/443033/default.aspx
"""
import csv
import datetime
from scraper import Scraper

CSV_URL = 'https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv'

class CrimeScraper(Scraper):
    schema = 'crime-reports'
    primary_key = ('offense_id',)
 #   title_format = u'{offense_code.name}'

    def data(self):
        cutoff_date = self.start_date - datetime.timedelta(days=90)
        for report in self.get_crime_reports():
            try:
                dt = self.datetime(report['"first_occurrence_date"'], '%m/%d/%Y %H:%M:%S %p')
                item_date = dt.date()
                location = self.point(float(report['"geo_lon"']), float(report['"geo_lat"']))
                if item_date < cutoff_date:
                    continue
            except ValueError as ex:
                # Some reports don't have coordinates and we should skip them.
                continue

            location_name = report['"incident_address"'].replace('BLK', 'block of')
            location_name = self.clean_address(location_name)
            # Some reports are missing the address. Don't publish these, because
            # reverse geocoding will not be accurate enough to get the original
            # address.
            if location_name.strip() == '':
                continue

            # Chop off the trailing ".0" on incident IDs.
            incident_id = report['"incident_id"']
            if incident_id.endswith('.0'):
                incident_id = incident_id[:-2]

            yield dict(
                title='',
                item_date=item_date,
                location_name=location_name,
                location=location,

                offense_id=report['"offense_id"'],
                incident_id=incident_id,
                occurrence_time=dt.time(),
                offense_code=report['"offense_code"'],
                offense_category=report['"offense_category_id"'],
            )

    def get_crime_reports(self):
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
    CrimeScraper().run()
