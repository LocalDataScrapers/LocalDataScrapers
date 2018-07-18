"""
Photos from Flickr.
Documentation:
http://www.flickr.com/services/api/flickr.photos.search.html
http://www.flickr.com/services/api/flickr.photos.getInfo.html
"""

import itertools
import datetime
import urllib
import time
from dateutil.parser import parse as dtparse

import sundew_pipeline
import stages as stages


API_KEY = '<Use your api key for flickr>'

API_BASE_URL = 'https://api.flickr.com/services/rest/?'


class Pipeline(sundew_pipeline.Pipeline):
    name = 'national.flickr'

    def __init__(self, *args, **kwargs):
        """
        Accepts three keyword params:
        min_date - photos must be no older than this date
        max_date - photos must be no newer than this date
        bbox - bounding box (expects tuple) 
        """
        one_day = datetime.timedelta(days=1)
        yesterday = datetime.date.today() - one_day

        # Default min_date is yesterday, default max_date is one day after 
        # min_date.
        self.min_date = kwargs.pop('min_date', yesterday)
        self.max_date = kwargs.pop('max_date', self.min_date + one_day)

        # Bounding box is required.
        self.bbox = kwargs.pop('bbox')

        super(Pipeline, self).__init__(*args, **kwargs)

    def get_stages(self):
        return [
            self.get_list_urls,
            stages.Download,
            stages.ParseJson,
            self.get_final,
        ]

    def get_list_urls(self):
        """
        Fetch successive pages from the API.
        """
        for i in itertools.count(1):
            params = dict(
                api_key=API_KEY,
                method='flickr.photos.search',
                min_upload_date=str(self.min_date),
                max_upload_date=str(self.max_date),
                bbox=', '.join(str(i) for i in self.bbox),
                accuracy=15,        # street-level
                safe_search=1,      # only safe content
                content_type=1,     # only photos
                media='photos',
                format='json',
                nojsoncallback=1,   # give us raw json only
                per_page=500,       # maximum
                page=i,
                extras='date_taken,date_upload,owner_name,license,geo',
            )
            yield API_BASE_URL + urllib.urlencode(params)

    def get_final(self, result):
        photos = result['photos']['photo']
        for photo in photos:
            # Datetaken field is a string, e.g. "2014-03-17 01:55:38".
            photo['date_taken'] = dtparse(photo['datetaken'])
            # Dateupload field is a unix time stamp, e.g. "1395039338".
            date_tuple = time.localtime(int(photo['dateupload']))[:3]
            photo['date_upload'] = datetime.date(*date_tuple)
            yield photo

        if len(photos) == 0:
            raise sundew_pipeline.StopPipeline


class UserPipeline(Pipeline):
    """
    Restrict photos to those taken by a particular user.
    """

    def __init__(self, *args, **kwargs):
        self.user_id = kwargs.pop('user_id')
        super(UserPipeline, self).__init__(*args, **kwargs)

    def get_list_urls(self):
        """
        Fetch successive pages from the API.
        """
        for i in itertools.count(1):
            params = dict(
                user_id=self.user_id,
                api_key=API_KEY,
                method='flickr.photos.search',
                min_upload_date=str(self.min_date),
                max_upload_date=str(self.max_date),
                bbox=', '.join(str(i) for i in self.bbox),
                accuracy=15,        # street-level
                safe_search=1,      # only safe content
                content_type=1,     # only photos
                media='photos',
                format='json',
                nojsoncallback=1,   # give us raw json only
                per_page=500,       # maximum
                page=i,
                extras='date_taken,date_upload,owner_name,license,geo',
            )
            yield API_BASE_URL + urllib.urlencode(params)


if __name__ == '__main__':
    extent = (-88.235202, 41.644286, -87.523661, 42.071732)
    pipeline = Pipeline(debug=True, bbox=extent)
    pipeline.run()
