import datetime
from scraper import Scraper, ScraperBroken
import requests
APP_TOKEN = 'v8ondRvIvB5DulbN4yuIUwOFH'
INCIDENTS_URL = 'https://data.phila.gov/resource/sspu-uyfa.json?$where=dispatch_date_time%20between%20%27{}%27%20and%20%27{}%27&$$app_token={}'

class CrimeScraper(Scraper):
    schema = 'crime'
    primary_key = ('dc_key',)

    def data(self):
        cutoff_date = datetime.date.today() - datetime.timedelta(days=30)

        for incident in self.get_crime_incidents(cutoff_date):
            # Rape incidents do not have a real value for dc_key, so we just
            # exclude them entirely.
            if incident['dc_key'].lower() == 'confidential':
                continue
            item_date = self.date(incident['dispatch_date'], '%Y-%m-%d')
            if item_date < cutoff_date:
                continue
            # If incident doesn't have a description skip it
            if 'text_general_code' not in incident:
                continue
            # If incident doesn't have a ucr code skip it
            if 'ucr_general' not in incident:
                continue

            # If incident doesn't have coordinates values, skip it
            if incident['shape'] is None:
                continue

            try:
                point_x = incident['shape']['coordinates'][0]
                point_y = incident['shape']['coordinates'][1]
                location = self.point(point_x,point_y)
            except (ValueError, KeyError) as ex:
                print (ex)
                location = None

            location_name = incident['location_block']
            # Make intersections geocodable.
            location_name = location_name.replace('/', ' & ')
            location_name = self.clean_address(location_name)

            yield dict(
                title=incident['text_general_code'],
                item_date=item_date,
                location=location,
                location_name=location_name,

                dc_key=incident['dc_key'],
                sector=incident['dc_dist'],
                primary_type=incident['ucr_general'],
                dispatch_time=self.time(incident['dispatch_time'], '%H:%M:%S'),

                # Old fields.
                secondary_type='',
                premise='',
                xy='',
            )

    def get_crime_incidents(self, cutoff_date):
        try:
            today = datetime.date.today()
            crime_incidents = requests.get(INCIDENTS_URL.format(cutoff_date,today,APP_TOKEN))
            crime_incidents = crime_incidents.json()
            for incident in crime_incidents:
                yield incident
        except Exception as ex:
            print(ex)
            raise ScraperBroken
if __name__ == '__main__':
    CrimeScraper().run()
