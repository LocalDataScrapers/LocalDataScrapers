"""
Chicago service requests.
"""

import json
import datetime
from dateutil.parser import parse as dtparse
from service_requests_pipeline import Pipeline

from scraper import Scraper


class ServiceRequestScraper(Scraper):
    schema = 'service-requests'
    primary_key = ('service_request_id',)

    def data(self):
        min_date = datetime.date.today() - datetime.timedelta(days=3)
        pipeline = Pipeline(min_date=min_date)
        # pipeline = CustomPipeline()

        for request in pipeline.get_values():
            requested_datetime = dtparse(request['requested_datetime'])
            notes = request.get('notes')
            if not notes:
                # Happens very rarely, but is possible.
                continue

            service_type = request['service_name']
            status = request['status']
            title = service_type + ' was '

            if status == 'closed':
                title += 'closed'
            elif status == 'open':
                # If there was something added after the request date, then 
                # count this item as being "updated".
                last_datetime = dtparse(notes[-1]['datetime'])
                if last_datetime.date() > requested_datetime.date():
                    title += 'updated'
                else:
                    title += 'opened'

            try:
                ward = int(request['extended_attributes']['ward'])
            except KeyError:
                ward = 0

            yield dict(
                title=title,
                url=request['service_tracker_url'],
                item_date=requested_datetime.date(),
                location=self.point(request['long'], request['lat']),
                location_name=request['address'],

                service_request_id=request['service_request_id'],
                service_type=service_type,
                status=status,
                agency_responsible=request['agency_responsible'],
                ward=ward,
                image_url=request['media_url'],
                notes=json.dumps(notes),
            )


class CustomPipeline(Pipeline):
    """
    Custom pipeline class that, given a list of service request IDs, can being
    used to update individual newsitems.
    """
    service_request_ids = []

    def get_item_urls(self):        
        url_tmpl = 'http://311api.cityofchicago.org/open311/v2/requests/%s.json?extensions=true'

        for id in self.service_request_ids:
            yield url_tmpl % id

    def get_stages(self):
        return [self.get_item_urls] + self.get_item_stages()

if __name__ == "__main__":
    ServiceRequestScraper().run()
