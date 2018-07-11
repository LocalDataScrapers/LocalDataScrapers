"""
Nashville service requests.
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

        pipeline = Pipeline()

        for request in pipeline.get_values():
            if 'issues' not in request:
                break
            if len(request['issues']) < 1:
                break
            for issue in request['issues']:
                updated_date = datetime.datetime.strptime(
                    issue['updated_at'], '%Y-%m-%dT%H:%M:%S-%f:00'
                ).date()
                service_type = issue['summary'].title()
                status = issue['status'].lower()
                title = service_type + ' was '
                try:
                    image = issue['media']['image_full']
                except:
                    image = None
                if status == 'closed':
                    title += 'closed'
                elif status == 'acknowledged':
                    title += 'opened'

                address = issue['address'].split(',')[0].rstrip('Nashville')

                yield dict(
                    title=title,
                    item_date=updated_date,
                    location=self.point(issue['lng'], issue['lat']),
                    location_name=self.clean_address(address),

                    service_request_id=issue['id'],
                    service_type=service_type,
                    status=status,
                    agency_responsible=issue['agency_responsible'] if 'agency_responsible' in issue else 'Unassigned',
                    image_url=image,
                    description=issue['description'] or '',
                )


if __name__ == "__main__":
    ServiceRequestScraper().run()
