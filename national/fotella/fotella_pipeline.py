"""
Photos from Fotella.
Documentation (Not public):
https://docs.google.com/document/d/1NdGlBSytPX-4Upl5cvUtKIKtH6UNq8pDLtTLuZsDtkY/edit
We currently have a five mile maximum radius on this current call and we have
 to use a midday datetime.
We use the tessalate file to partition the metro into 5 mile radius circles.
example: http://fotella.com/api/get/39.9612,-75.1585,2015-11-04T12:00/5,12
"""
from django.contrib.gis.geos import Point, Polygon

import itertools
import datetime
import urllib
import time
from dateutil.parser import parse as dtparse

import sundew_pipeline
import stages as stages

API_BASE_URL = 'http://fotella.com/api/get/'
CALL_RADIUS = 5  # Value is in km


class Pipeline(sundew_pipeline.Pipeline):
    name = 'national.fotella'

    def __init__(self, *args, **kwargs):
        """
        API call returns all photos within a user defined area,
        defined by a user defined point and extending out to a circular user defined radius.
        Accepts three params:
        Point - Centroid  of the cicle subarea within which we will query
        Radius - Radius of photos to query about an area.
        date_time - photos must be no older than this date and time (UTC timezone) (set to 12:00 for best results
        """
        one_day = datetime.timedelta(days=1)
        yesterday = str(datetime.date.today() - one_day) + 'T12:00'

        self.far_datetime = kwargs.pop('far_datetime', yesterday)

        # Extent is required for cover_region call
        self.extent = kwargs.pop('extent')
        self.centroid = kwargs.pop('centroid')

#        centroids = cover_region(self.extent, CALL_RADIUS)
        centroids = [(-88.19630380716728, 41.66106624592328), (-88.11850742150176, 41.66106624592328), (-88.04071103583622, 41.66106624592328), (-87.96291465017069, 41.66106624592328), (-87.88511826450517, 41.66106624592328), (-87.80732187883962, 41.66106624592328), (-87.72952549317408, 41.66106624592328), (-87.65172910750856, 41.66106624592328), (-87.57393272184302, 41.66106624592328), (-87.49613633617749, 41.66106624592328), (-88.23520200000006, 41.71138075361829), (-88.15740561433452, 41.71138075361829), (-88.079609228669, 41.71138075361829), (-88.00181284300345, 41.71138075361829), (-87.92401645733791, 41.71138075361829), (-87.84622007167239, 41.71138075361829), (-87.76842368600686, 41.71138075361829), (-87.69062730034132, 41.71138075361829), (-87.61283091467578, 41.71138075361829), (-87.53503452901025, 41.71138075361829), (-88.19630380716728, 41.76165590997237), (-88.11850742150176, 41.76165590997237), (-88.04071103583622, 41.76165590997237), (-87.96291465017069, 41.76165590997237), (-87.88511826450517, 41.76165590997237), (-87.80732187883962, 41.76165590997237), (-87.72952549317408, 41.76165590997237), (-87.65172910750856, 41.76165590997237), (-87.57393272184302, 41.76165590997237), (-87.49613633617749, 41.76165590997237), (-88.23520200000006, 41.81189170708349), (-88.15740561433452, 41.81189170708349), (-88.079609228669, 41.81189170708349), (-88.00181284300345, 41.81189170708349), (-87.92401645733791, 41.81189170708349), (-87.84622007167239, 41.81189170708349), (-87.76842368600686, 41.81189170708349), (-87.69062730034132, 41.81189170708349), (-87.61283091467578, 41.81189170708349), (-87.53503452901025, 41.81189170708349), (-88.19630380716728, 41.862088137176876), (-88.11850742150176, 41.862088137176876), (-88.04071103583622, 41.862088137176876), (-87.96291465017069, 41.862088137176876), (-87.88511826450517, 41.862088137176876), (-87.80732187883962, 41.862088137176876), (-87.72952549317408, 41.862088137176876), (-87.65172910750856, 41.862088137176876), (-87.57393272184302, 41.862088137176876), (-87.49613633617749, 41.862088137176876), (-88.23520200000006, 41.912245192604786), (-88.15740561433452, 41.912245192604786), (-88.079609228669, 41.912245192604786), (-88.00181284300345, 41.912245192604786), (-87.92401645733791, 41.912245192604786), (-87.84622007167239, 41.912245192604786), (-87.76842368600686, 41.912245192604786), (-87.69062730034132, 41.912245192604786), (-87.61283091467578, 41.912245192604786), (-87.53503452901025, 41.912245192604786), (-88.19630380716728, 41.96236286584621), (-88.11850742150176, 41.96236286584621), (-88.04071103583622, 41.96236286584621), (-87.96291465017069, 41.96236286584621), (-87.88511826450517, 41.96236286584621), (-87.80732187883962, 41.96236286584621), (-87.72952549317408, 41.96236286584621), (-87.65172910750856, 41.96236286584621), (-87.57393272184302, 41.96236286584621), (-87.49613633617749, 41.96236286584621), (-88.23520200000006, 42.0124411495066), (-88.15740561433452, 42.0124411495066), (-88.079609228669, 42.0124411495066), (-88.00181284300345, 42.0124411495066), (-87.92401645733791, 42.0124411495066), (-87.84622007167239, 42.0124411495066), (-87.76842368600686, 42.0124411495066), (-87.69062730034132, 42.0124411495066), (-87.61283091467578, 42.0124411495066), (-87.53503452901025, 42.0124411495066), (-88.19630380716728, 42.062480036317666), (-88.11850742150176, 42.062480036317666), (-88.04071103583622, 42.062480036317666), (-87.96291465017069, 42.062480036317666), (-87.88511826450517, 42.062480036317666), (-87.80732187883962, 42.062480036317666), (-87.72952549317408, 42.062480036317666), (-87.65172910750856, 42.062480036317666), (-87.57393272184302, 42.062480036317666), (-87.49613633617749, 42.062480036317666), (-87.76137667041651, 41.83090146038926)] #cen for cen in centroids]
#        centroids.append(self.centroid)
        self.centroids = centroids

        super(Pipeline, self).__init__(*args, **kwargs)

    def get_stages(self):
        return [
            self.get_list_urls,
            stages.Download,
            self.check_response,
            stages.ParseJson,
            self.get_final,
        ]

    def get_list_urls(self):
        """
        API CALL FORMAT:  http://fotella.com/api/get/41.8450124254935,-87.68836974506735,2015-11-12T12:00/5,12
        BASE URL/POINT_OF_INTEREST,DATETIME_IN_IST/RADIUS,12
        """
        centroids = self.centroids
        for centroid in centroids:
            centroid = Point(centroid)
            point_params = (str(centroid.y),
                           str(centroid.x),
                           str(self.far_datetime),
            )
            radius_params = (str(CALL_RADIUS),
                            str(12),
            )
            point_params = ','.join(point_params)
            radius_params = ','.join(radius_params)
            yield API_BASE_URL + point_params + '/' + radius_params

    def check_response(self,result):
        if '"Items":' in result:
            return result
        else:
            #  If no results, don't bother looking for newsitems and try a new api call
            pass

    def get_final(self, result):
        photos = result[0]['Items']
        for photo in photos:
            # Datetaken field is a string, e.g. "2014-03-17 01:55:38".
            raw_date = dtparse(photo['ImageDate'])
            photo['ImageDate'] = datetime.date(raw_date.year, raw_date.month, raw_date.day)
            raw_date = dtparse(photo['Created'])
            photo['Created'] = datetime.date(raw_date.year, raw_date.month, raw_date.day)
            yield photo

        if len(photos) == 0:
            raise sundew.StopPipeline


if __name__ == '__main__':
    from django.contrib.gis.geos import Point
    coords = (-87.76137667041651, 41.83090146038926)
    centroid_x = -87.76137667041651
    centroid_y = 41.83090146038926
    centroid = Point(centroid_x, centroid_y)
    pipeline = Pipeline(debug=True, centroid=centroid)
    pipeline.run()
