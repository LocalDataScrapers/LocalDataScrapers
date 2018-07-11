from retrieval import Retriever
import datetime
import logging

class ScraperBroken(Exception):
    "Something changed in the underlying HTML and broke the scraper."
    pass

class BaseScraper(object):
    """
    Base class for all scrapers in everyblock.retrieval.scrapers.
    """
    logname = 'basescraper'
    sleep = 0

    def __init__(self):
        self.retriever = Retriever(sleep=self.sleep)
        self.logger = logging.getLogger('eb.retrieval.%s' % self.logname)
        self.start_time = datetime.datetime.now()

    def update(self):
        'Run the scraper.'
        raise NotImplementedError()

    def get_html(self, *args, **kwargs):
        return self.retriever.get_html(*args, **kwargs)
