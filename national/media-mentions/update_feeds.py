"""
RSS-feed retriever
"""

import simplejson as json
import xmltodict
import requests
import datetime
import logging

class FeedUpdater(object):
    def __init__(self, logger):
        self.logger = logger
        self.not_found_count = 0  # track number of 404 errors

    def update(self, crawl_timestamp):
        #This is one of the sample RSS feed url, this can be updated any other url to retrieve data from that rss feed
        #For other urls, refer to sources folder to find out other rss feeds by metro.
	url = 'http://www.frostscience.org/blog/feed/'
        self.logger.info('Downloading %s', url)
        last_entry_date = None

        try:
            feed = requests.get(url)._content
        except Exception as ex:
            self.log_error('{} on {}'.format(ex, url))
            return []

	filename = 'media-data.json'
	obj = xmltodict.parse(feed)
	with open(filename, 'w+') as outfile:
             json.dump(obj, outfile)

def update(seed_id=None, flag='false'):
    """
    Retrieves and saves every new item for every Seed that is an RSS feed.
    """
    ni_dict = {}
    crawl_timestamp = datetime.datetime.now()
    logger = logging.getLogger('eb.retrieval.blob_rss')
    updater = FeedUpdater(logger)
    ni_list = updater.update(crawl_timestamp)

if __name__ == "__main__":
    update()
