"""
Photos from Flickr.
NOTE: Don't run this scraper too much. Once you hit the weekly quota (it's 
supposed to be 20, but it's not very clear), you won't get any more results.
"""

import datetime
from flickr_pipeline import Pipeline, UserPipeline
from scraper import Scraper

PAGE_URL_TEMPLATE = 'https://www.flickr.com/photos/{owner}/{id}'
THUMB_URL_TEMPLATE = 'https://farm{farm}.static.flickr.com/{server}/{id}_{secret}_s.jpg'

BLACKLISTED_USER_IDS = set(['<User Id>'])

# Radius to search within for the closest intersection.
INTERSECTION_RADIUS = 250
BBOXS = (-88.235202, 41.644286, -87.523661, 42.071732)

class FlickrScraper(Scraper):
    schema = 'photos'
    primary_key = ('photo_id', 'source')
    update = True

    def data(self):
        pipeline = Pipeline(bbox=BBOXS)

        for photo in pipeline.get_values():
            # Ignore blacklisted users.
            user_id = photo['owner']
            if user_id in BLACKLISTED_USER_IDS:
                continue

            # Ignore photos taken outside of the metro.
            pt = self.point(float(photo['longitude']), float(photo['latitude']))

            yield dict(
                title=photo['title'].strip() or 'Untitled',
                item_date=photo['date_upload'],
                url=PAGE_URL_TEMPLATE.format(**photo),
                location=pt,
                photo_id=photo['id'],
                photo_href=THUMB_URL_TEMPLATE.format(**photo),
                user_id=user_id,
                username=photo['ownername'],
                date_taken=photo['date_taken'],
                license=int(photo['license']),
                source='Flickr',
            )

if __name__ == "__main__":
    FlickrScraper().run()
