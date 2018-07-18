"""
Photos from Fotella.
"""

from fotella_pipeline import Pipeline
from scraper import Scraper

# Radius to search within for the closest intersection.
INTERSECTION_RADIUS = 250

BLACKLISTED_USER_IDS = []  # Blank until they reveal themselves
## CHICAGO LOCATION PARAMETERS
extents = (-88.23520200000007, 41.644286, -87.52366099999999, 42.071732)
coords = (-87.76137667041651, 41.83090146038926)

##BOSTON LOCATION PARAMETERS
#extents = (-71.178935, 42.294597, -71.064095, 42.453445)
#coords = (-71.12141711650301, 42.37854194285119)

##DENVER LOCATION PARAMETERS
#extents = (-105.303403, 39.550907, -104.488906, 39.914247)
#coords = (-104.87514918085822, 39.73905956262944)

##HIALEAH LOCATION PARAMETERS
#extents = (-80.38560300000006, 25.80287800000002, -80.19692800000006, 25.92807900000002)
#coords =(-80.29509648114536, 25.87375958223654)

##FRESNO LOCATION PARAMETERS
#extents = (-119.93439700000008, 36.66216200000002, -119.61263700000009, 36.94659800000003)
#coords = (-119.77438044284877, 36.791192726699826)

##HOUSTON LOCATION PARAMETERS
#extents = (-95.9101060000001, 29.537381000000025, -95.0145740000001, 30.110706000000025)
#coords = (-95.38146611957355, 29.777868796775426)

##MEDFORD LOCATION PARAMETERS
#extents = (-71.178935, 42.294597, -71.064095, 42.453445)
#coords = (-71.12141711650301, 42.37854194285119)

##NASHVILLE LOCATION PARAMETERS
#extents = (-87.05476600000007, 35.96778500000003, -86.51558900000005, 36.42074400000003)
#coords = (-86.78349279099844, 36.173274726119125)

##PHILLY LOCATION PARAMETERS
#extents = (-75.28030494456422, 39.86700539694134, -74.95574684893913, 40.13792673325193)
#coords = (-75.13402993250466, 40.007579382566526)

##SEATTLE LOCATION PARAMETERS
#extents = (-122.43594932565433, 47.49551362485468, -122.2359476922633, 47.73414067087273)
#coords = (-122.33336751826631, 47.62084261285583)


class FotellaScraper(Scraper):
    schema = 'photos'
    primary_key = ('photo_id', 'source')
    update = True

    def data(self):
        pipeline = Pipeline(extent=extents, centroid=coords)

        for photo in pipeline.get_values():
            # Ignore blacklisted users.
            user_id = photo['Owner']
            if user_id in BLACKLISTED_USER_IDS:
                continue

            # Ignore photos taken outside of the metro.
            pt = self.point(float(photo['Longitude']), float(photo['Latitude']))
            location_name = pt

            yield dict(
                title=photo['Location'].strip() or 'Untitled',
                item_date=photo['ImageDate'],
                url=photo['Link'],
                location=pt,
                location_name=str(location_name),

                photo_id=photo['GeoTimeHash'],
                photo_href=photo['URL'],
                user_id=user_id,
                username=user_id,
                date_taken=photo['Created'],
                license=0,
                source='Fotella',
            )

if __name__ == "__main__":
    FotellaScraper().run()
