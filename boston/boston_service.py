"""
Boston service requests.
"""

import json
import datetime
from dateutil.parser import parse as dtparse
from service_requests_pipeline import Pipeline
from django.utils.text import Truncator

from scraper import Scraper


class ServiceRequestScraper(Scraper):
    schema = 'service-requests'
    primary_key = ('service_request_id',)

    def data(self):
        min_date = datetime.date.today() - datetime.timedelta(days=3)
        pipeline = Pipeline(min_date=min_date)

        for request in pipeline.get_values():
            requested_datetime = dtparse(request['requested_datetime'])
            notes = request.get('notes')
            if not notes:
                # Happens very rarely, but is possible.
                continue

            service_type = request['service_name']
            status = request['status']
            description = request.get('description', '')
            if service_type == 'Other' and description:
                title = service_type + ": " + Truncator(description).words(3) + ' was '
            else:
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

            yield dict(
                title=title,
                url=request['service_tracker_url'],
                item_date=requested_datetime.date(),
                location=self.point(request['long'], request['lat']),
                location_name=request['address'],

                service_request_id=request['service_request_id'],
                service_type=service_type,
                status=status,
                image_url=request['media_url'],
                extra=json.dumps({
                    'notes': notes,
                    'description': description,
                    'extended_attributes': request.get('extended_attributes', {})
                }),
            )


if __name__ == "__main__":
    ServiceRequestScraper().run()
