import inspect
import os
import os.path as op
import sqlite3
import zlib
import functools
import pprint
import tempfile
import time

import dateutil.relativedelta
import dateutil.parser
import requests

import stages
#import utils
import keyvalue
import re
from HTMLParser import HTMLParser
from dateutil.parser import parse as duparse

class StopPipeline(Exception):
    pass


class Pipeline(object):
    def __init__(self, name=None, debug=False):
        self.name = getattr(self, 'name', name)
        if not self.name:
            raise Exception('Pipeline object has no name')

        self.finished = False

        self.debug = debug
        if self.debug:
            # Put the key value store file in the current working directory.
            store_path = op.join(os.getcwd(), self.name + '.store')
            self._store = keyvalue.KeyValueStore(store_path, overwrite=False)
            self.cache = keyvalue.RequestCache(self._store)

        self.objects = {}

    def before_run(self):
        """
        Override this method in subclasses. It will execute at the beginning of
        any call to get_values() or run().
        """
        pass

    def after_run(self):
        """
        Override this method in subclasses. It will execute at the end of
        any call to get_values() or run().
        """
        pass

    def get_values(self):
        self.finished = False
        # Use a different session for each run.
        self.session = requests.Session()

        self.before_run()

        self.stages = self.get_stages()

        generators = []

        for i, stage in enumerate(self.stages):
            if i == 0:
                if isinstance(stage, list) or isinstance(stage, tuple):
                    # An input sequence that kicks everything off.
                    generator = iter(stage)
                elif inspect.isgeneratorfunction(stage):
                    # Otherwise we assume it's a generator function.
                    generator = stage()
                elif isinstance(stage, stages.PipelineStage):
                    generator = stage.as_generator_func(self)()
            else:
                if isinstance(stage, stages.PipelineStage):
                    func = stage.as_generator_func(self)
                elif inspect.isclass(stage) and issubclass(stage, stages.PipelineStage):
                    # Is a subclass of stages.PipelineStage.
                    func = stage().as_generator_func(self)
                elif inspect.isgeneratorfunction(stage):
                    func = generator_func_wrapper(stage)
                elif callable(stage):
                    func = simple_func_wrapper(stage)
                else:
                    raise ValueError('Got an invalid stage object: {}'.format(stage))

                generator = func(generators[-1])

            generators.append(generator)

        self.generators = generators

        last_generator = generators[-1]
        try:
            for obj in last_generator:
                yield obj
        except StopPipeline:
            pass

        self.stop()

        self.after_run()

    def run(self):
        t_start = time.clock()

        count = 0
        for count, data in enumerate(self.get_values(), 1):
            if isinstance(data, dict):
                pprint.pprint(data)
            else:
                print(data)

        t_end = time.clock()
        delta = dateutil.relativedelta.relativedelta(seconds=t_end - t_start)

        print
        print '\nFinished in {} minutes and {} seconds.'.format(
            delta.minutes, delta.seconds)
        print 'Returned {} objects.'.format(count)

    def stop(self):
        for gen in self.generators:
            if hasattr(gen, 'close'):
                gen.close()
        self.finished = True

    # Utility methods.

    def get(self, url, **kwargs):
        """
        Retrieve data from a remote server. If in debug mode, first check the
        cache before initiating a request.
        """
        if self.debug:
            response = self.cache.get('GET', url)
            if response is not None:
                print('Retrieving from cache: {}'.format(url))
                return response
            else:
                print('Downloading: {}'.format(url))
                response = self.session.get(url, **kwargs)
                if response.status_code == 200:
                    self.cache.set(response)
                return response
        else:
            return self.session.get(url, **kwargs)

    def get_to_file(self, url, data=None, **kwargs):
        """
        Retrieve data from a remote server and put into a temporary file. If in
        debug mode, first check the cache before initiating a request.
        """
        if self.debug:
            response = self.cache.get('GET', url)
            if response is not None:
                print('Retrieving from cache: {}'.format(url))
                return self.temp_file(response.content)
            else:
                print('Downloading: {}'.format(url))
                response = self.session.get(url, **kwargs)
                if response.status_code == 200:
                    self.cache.set(response)
                return self.temp_file(response.content)
        else:
            # Turn on streaming to conserve memory usage.
            kwargs['stream'] = kwargs.get('stream', True)
            response = self.session.get(url, **kwargs)
            return self.temp_file(response.iter_content(512))

    def post(self, url, data=None, **kwargs):
        if self.debug:
            response = self.cache.get('POST', url, data)
            if response is not None:
                print('Retrieving from cache: {} (POST)'.format(url))
                return response
            else:
                print('Downloading: {} (POST)'.format(url))
                response = self.session.post(url, data, **kwargs)
                self.cache.set(response)
                return response
        else:
            return self.session.post(url, data, **kwargs)

    def post_to_file(self, url, data=None, **kwargs):
        if self.debug:
            response = self.cache.get('POST', url, data)
            if response is not None:
                print('Retrieving from cache: {} (POST)'.format(url))
                return self.temp_file(response.content)
            else:
                print('Downloading: {} (POST)'.format(url))
                response = self.session.post(url, data, **kwargs)
                self.cache.set(response)
                return self.temp_file(response.content)
        else:
            # Turn on streaming to conserve memory usage.
            kwargs['stream'] = kwargs.get('stream', True)
            response = self.session.post(url, data, **kwargs)
            return self.temp_file(response.iter_content(512))

    def temp_file(self, data=None):
        tf = tempfile.TemporaryFile()
        if data:
            if hasattr(data, '__iter__'):
                for chunk in data:
                    tf.write(chunk)
            else:
                tf.write(data)
            tf.seek(0)
        return tf

    def parse_date(self, text):
	if text is None or not text.strip():
            return None
        try:
            return duparse(text).date()
        except:
            return None

    def parse_datetime(self, text):
	if text is None or not text.strip():
            return None
        try:
            return duparse(text)
        except:
            return None

    def html_to_text(self, html):
	if html is None:
            return None
        # Get rid of carriage returns.
        html = re.sub(r'\r|\n', '', html)
        # Get rid of non-breaking spaces.
        html = html.replace(u'\xa0', ' ')

        parser = ChunkingHtmlParser()
        parser.feed(html)
        return parser.get_text()

def generator_func_wrapper(fn):
    def result(source):
        for data in source:
            for value in fn(data):
                yield value

    return result

def simple_func_wrapper(fn):
    def result(source):
        for data in source:
            value = fn(data)
            if value is not None:
                yield value

    return result
