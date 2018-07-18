"""
FACEBOOK FEED UPDATER using the Facebook Graph API instead
"""
import datetime
from dateutil.parser import parse as dtparse
import json
import logging
import pytz
import re
import requests
import urlparse


def get_facebook_token():
        params = {
            'client_id': '######', #Update your Facebook App Id
            'client_secret': '######', #Update your Facebook Oauth secret
            'grant_type': 'client_credentials'
        }
        try:
            response = requests.get('https://graph.facebook.com/oauth/access_token', params=params)
        except requests.exceptions.RequestException as e:
            return None
        if response.status_code != 200:
            return None
        access_token = json.loads(response.content)['access_token']
        return access_token


def get_feed_id(url, access_token):
    split_source = urlparse.urlsplit(url)
    query = urlparse.parse_qs(split_source.query).get('id')
    feed_id = split_source.path.replace('/', '').replace('timeline', '').replace('pages', '')
    id_match = re.search(r'\d+$',feed_id)
    if query:
        feed_id = query[0]
    elif id_match:
        if len(id_match.group())>5:
            # Feed ID is at the end of the url, feed IDs are usually longer than 5 digits
            feed_id = id_match.group()
    if not feed_id.isdigit():
        # Not the feed id, then use the url to do a page lookup and get the feed id.
        params = {
            'access_token': access_token,
        }
        response = requests.get('https://graph.facebook.com/' + feed_id, params=params)
        page = json.loads(response.content)
        if 'id' in page:
            feed_id = page['id']
        else:
            # The url has a problem and should be investigated
            return None
    return feed_id


def get_feed_items(page_id, access_token, next_url=None, days=180):
    # Facebook Graph API param is unix timestamp, we convert to UTC for consistency
    now = datetime.datetime.now()
    current_time = now.strftime('%s')
    past_time = (now - datetime.timedelta(days)).strftime('%s')
    params = {'access_token': access_token,
              'fields': 'story,message,created_time,link,type,place',
              'limit': 100,
              'since': past_time,
              'until': current_time,
              }
    if next_url:
        page = requests.get(next_url)
    else:
        page = requests.get('https://graph.facebook.com/v2.9/{0}/posts/?'.format(page_id), params=params)
    if page.status_code != 200:
        return None
    feed_items = json.loads(page.content)
    return feed_items


class FacebookUpdater(object):
    def __init__(self, logger):
        self.logger = logger
        self.not_found_urls = []  # track number of problem urls
        self.access_token = get_facebook_token()

    def update(self, crawl_timestamp):
        ni_list = []
        last_entry_date = None

	seed = {
		'url': 'https://www.facebook.com/lmdfeed/',
		'source_name': 'Guide Me Home 2 Chicago Luxury Homes',
	}

        try:
            feed_id = get_feed_id(seed['url'], self.access_token)
            feed_items = get_feed_items(feed_id, self.access_token)
            feed_data = feed_items.get('data', [])
	    filename = 'facebook-data.json'
            for post in feed_data:
                # Skip this post if it doesn't have a post id
                if 'id' not in post:
                    continue
                post_title = post.get('story', post.get('message',
                                                        'Untitled {}{}'.format(post['type'], post['created_time'])))
                post_title = post_title if len(post_title) < 45 else post_title[:45] + '...'
                post_description = post.get('message')
                post_datetime = dtparse(post.get('created_time'))

                self.logger.info('Got post id:{}'.format(post['id']))

                if post_datetime and post_datetime > datetime.datetime.now(pytz.utc):
                    # Skip articles in the future, because sometimes articles show
                    # up in the feed before they show up on the site, and we don't
                    # want to retrieve the article until it actually exists.
                    self.logger.info('Skipping article_date {}, which is in the future'.format(post_datetime))
                    continue

                if post_datetime and (last_entry_date is None or last_entry_date < post_datetime):
                    last_entry_date = post_datetime

                # Check if we the post has geo location data
                if 'place' in post:
                    if 'location' in post['place']:
                        if 'street' in post['place']['location']:
                            # We want to geocode to the street address, don't use the point
                            location = None
                        elif 'latitude' in post['place']['location'] and 'longitude' in post['place']['location']:
                            latitude = post['place']['location']['latitude']
                            longitude = post['place']['location']['longitude']
                            location = None #Point(latitude, longitude)
                else:
                    location = None
		ni_list.append(post)

                with open(filename, 'w+') as outfile:
                    json.dump(ni_list, outfile)

	except Exception as ex:
            self.logger.info('{} retrieving facebook seeds from metro.'.format(ex))
            ni_list = []

def update():
    """
    Retrieves and saves every new item for every Facebook Source that is an RSS feed.
    """
    crawl_timestamp = datetime.datetime.now()
    logger = logging.getLogger('facebook')

    updater = FacebookUpdater(logger)
    ni_list = updater.update(crawl_timestamp)
    if ni_list:
        logger.info('Created {} newsitems in this scrape,'.format(len(ni_list)))
    else:
        logger.info('Created 0 NewsItems in this scrape')
    logger.info('Number of error urls: {}, {}'.format(len(updater.not_found_urls), updater.not_found_urls))

if __name__ == "__main__":
    update()
