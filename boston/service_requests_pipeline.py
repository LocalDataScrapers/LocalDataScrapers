"""
Boston service requests.
Main page:
http://wiki.open311.org/GeoReport_v2/Servers
"""
import re
import itertools
import datetime
import urllib
import time

import sundew_pipeline
import stages as stages


API_BASE_URL = 'https://mayors24.cityofboston.gov/open311/v2/'
WEB_BASE_URL = 'https://mayors24.cityofboston.gov/reports/'


class Pipeline(sundew_pipeline.Pipeline):
    name = 'boston.service_requests'

    def __init__(self, *args, **kwargs):
        # If not specified, min_date defaults to yesterday.
        if 'min_date' in kwargs:
            self.min_date = kwargs.pop('min_date')
        else:
            self.min_date = datetime.date.today()

        super(Pipeline, self).__init__(*args, **kwargs)

    def get_stages(self):
        return self.get_item_url_stages() + self.get_item_stages()

    def get_item_url_stages(self):
        """
        Stages used to get URLs for individual items to process, i.e. returns
        URLs like this:
        https://mayors24.cityofboston.gov/open311/v2/requests/H179938-101001218630.json?extensions=true
        """
        return [
            self.get_list_urls,
            self.download_and_wait,
            stages.ParseJson,
            self.get_item_urls,
        ]

    def get_item_stages(self):
        """
        Stages used to get the final result items.
        """
        return [
            self.download_and_wait,
            stages.ParseJson,
            self.get_final,
        ]

    def get_list_urls(self):
        """
        Fetch successive pages from the API.
        """
        for i in itertools.count(1):
            params = dict(
                page_size=500,
                page=i,
                updated_after=self.min_date,
            )
            yield API_BASE_URL + 'requests.json?' + urllib.urlencode(params)

    def get_item_urls(self, result):
        """
        Extract request urls out of the JSON result page. Only return URLs for
        service requests that have an image.
        """
        url_tmpl = '%srequests/%s.json?extensions=true'
        if len(result) == 0:
            raise sundew_pipeline.StopPipeline

        for item in result:
            service_request_id = item.get('service_request_id')
            if service_request_id and 'media_url' in item:
                yield url_tmpl % (API_BASE_URL, service_request_id)

    def download_and_wait(self, url):
        """
        The server throttles requests, so whenever we encounter a 403, we need
        to wait for a while.
        """
        # Wait for progressively longer periods.
        for seconds in itertools.count(40, 10):
            response = self.get(url)
            if response.status_code == 200:
                break
            print 'Sleeping for %s seconds' % seconds
            time.sleep(seconds)

        return response.text

    def get_final(self, lst):
        """
        Flatten the list since it only ever contains one element. Also add link
        to the service tracker page.
        """
        item = lst[0]
        item['service_tracker_url'] = WEB_BASE_URL + item['service_request_id']
        return item


if __name__ == '__main__':
    pipeline = Pipeline(debug=False)
    pipeline.run()
