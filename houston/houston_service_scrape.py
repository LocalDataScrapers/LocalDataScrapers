"""
Houston service requests.
"""

import json
import datetime
import arrow
from service_requests_pipeline import Pipeline

from scraper import Scraper


class ServiceRequestScraper(Scraper):
    schema = 'service-requests'
    primary_key = ('service_request_id',)

    def data(self):

        # Let's examine the past month for changes. We may want to go further
        # in the future.
        end_date = arrow.get().to('US/Central') # now
        start_date = end_date.replace(days=-30)
        pipeline = Pipeline(start_date=start_date, end_date=end_date)

        for request in pipeline.get_values():
            requested_datetime = arrow.get(request['created_at'])

            service_type = request['summary']
            status = request['status']
            title = service_type + ' was '

            if status == 'closed':
                title += 'closed'
            elif status == 'open':
                title += 'opened'

            # Covers most cases
            # 10500 Richmond Avenue Houston, Tx 77042, Usa
            # to
            # 10500 Richmond Avenue Houston
            # to
            # 10500 Richmond Avenue
            address = request['address'].split(',')[0].rstrip('Houston')

            media_dict = request.get('media', None)
            if media_dict:
                image_url = media_dict.get('image_full', None)
            else:
                image_url = None

            yield dict(
                title=title,
                item_date=requested_datetime.date(),
                location=self.point(request['lng'], request['lat']),
                location_name=self.clean_address(address),

                service_request_id=request['id'],
                service_type=service_type,
                status=status,
                agency_responsible="",
                image_url=image_url,
                description=request['description'] or '',
            )


if __name__ == "__main__":
    ServiceRequestScraper().run()
