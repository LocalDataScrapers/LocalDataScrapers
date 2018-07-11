"""
Boston crime
"""

import csv
import datetime
from scraper import Scraper

CSV_URL = 'https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/crime.csv'

class CrimeScraper(Scraper):
    schema = 'crime-reports'
    primary_key = ('unique_id',)

    def data(self):
        cutoff_date = self.start_date - datetime.timedelta(days=30)
        for report in self.get_crime_reports():
            try:
                dt = self.datetime(report['occurred_on_date'], '%Y-%m-%d %H:%M:%S')
                item_date = dt.date()
                location = self.point(float(report['long']), float(report['lat']))

                if item_date < cutoff_date:
                    continue
            except ValueError as ex:
                # Some reports don't have coordinates and we should skip them.
                continue

            location_name = report['street']
            location_name = self.clean_address(location_name)
            # Some reports are missing the address. Don't publish these, because
            # reverse geocoding will not be accurate enough to get the original
            # address.
            if location_name.strip() == '':
                continue

            # Chop off the trailing ".0" on incident IDs.
            incident_id = report['incident_number']
            if incident_id.endswith('.0'):
                incident_id = incident_id[:-2]

            yield dict(
                title=report['offense_code_group'],
                item_date=item_date,
                location_name=location_name,
                location=location,
                offense_description=report['offense_description'].decode("windows-1252").encode("utf8"),
                unique_id=incident_id+report['offense_code'],
                offense_id=report['offense_code'],
                incident_number=incident_id,
                occurrence_time=dt.time(),
                offense_code=report['offense_code'],
                offense_category=report['offense_code_group'],
                offense_code_group=report['offense_code_group'],
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
