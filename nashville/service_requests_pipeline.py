"""
Nashville service requests.
Main page:
https://seeclickfix.com/api/v2/issues?place_url=nashville
SeeClickFix provides an API based alternative. We use the SeeClickFix API directly
because there is no official Open 311 data source at this time.
"""
import arrow
import datetime
import itertools
import urllib
import requests

import sundew_pipeline
import stages as stages


API_BASE_URL = 'https://seeclickfix.com/api/v2/issues?'


class Pipeline(sundew_pipeline.Pipeline):
    name = 'nashville.service_requests'

    def __init__(self, *args, **kwargs):
        super(Pipeline, self).__init__(*args, **kwargs)

    def get_stages(self):
        """
        Standard fare.
        """
        return [
            self.get_list_urls,
            stages.Download, # API call for each URL
            stages.ParseJson, # To lists and dicts
        ]

    def get_list_urls(self):
        """
        Fetch successive pages from the API.
        """
        cur_datetime = datetime.datetime.isoformat(datetime.datetime.now())
        yesterday_datetime = datetime.datetime.isoformat(datetime.datetime.now() - datetime.timedelta(days=10))

        for i in itertools.count(1):
            params = dict(
                page=i,
                place_url='nashville',
                per_page=100,
                details='true',
                before=cur_datetime,
                after=yesterday_datetime,
                status='closed,acknowledged'
            )
            url = API_BASE_URL + urllib.urlencode(params)
            yield url


if __name__ == '__main__':
    pipeline = Pipeline(debug=False)
    pipeline.run()

