import arrow
from ast import literal_eval

from scraper import Scraper


API_URL = 'http://data.phila.gov/resource/4t9v-rppq.json'

class ServiceRequestScraper(Scraper):
    schema = 'service-requests'
    primary_key = ('service_request_id',)

    def data(self):

        # Let's examine the past month for changes. We may want to go further
        # in the future.
        end_date = arrow.get().to('US/Eastern') # now
        start_date = end_date.replace(days=-30)

        for request in self.get_service_requests():
            requested_datetime = arrow.get(request['requested_datetime'])

            service_type = request['service_name']
            status = request['status']
            title = service_type + ' was '

            if status == 'Closed':
                title += 'closed'
            elif status == 'Open':
                title += 'opened'

            # We don't want the results without these keys
            if not all (k in request for k in ("agency_responsible", "service_name", "lat", "address")):
                continue

            if request['agency_responsible'] == 'Needs Review':
                continue

            yield dict(
                title=title,
                item_date=requested_datetime.date(),
                location=self.point(float(request['lon']), float(request['lat'])),
                location_name=self.clean_address(request['address']),
                service_request_id=request['service_request_id'],
                service_type=service_type,
                status=status,
                agency_responsible=request['agency_responsible'],
                image_url=request.get('media_url',''),
                description=request.get('description',''),
            )

    def get_service_requests(self):
        filename = self.cache_get_to_file('4t9v-rppq', 'json', API_URL)

        with open(filename, 'r') as json_file:
            json_records = json_file.readlines()
            for record in json_records:
                record = record.replace('\n','')
                record = record[1:] if record[0] == '[' or record[0] == ',' else record
                record = record[:-1] if record[-1] == ']' or record[0] == ',' else record
                yield literal_eval(record)


if __name__ == "__main__":
    ServiceRequestScraper().run()
