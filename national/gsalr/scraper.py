from collections import namedtuple, defaultdict
from django.contrib.gis.geos import Point, GEOSGeometry, GEOSException
from django.db import transaction
import json
from django.utils.html import strip_tags
import text as text_utils
from lxml import etree
import urlparse
import datetime
import logging
import os
import re
import sys
import traceback
import ftplib
from cStringIO import StringIO
import time
from dateutil.parser import parse as duparse
import httplib2
import socket
from Cookie import SimpleCookie, CookieError

def daterange(d1, d2):
    "Iterator that returns every date between d1 and d2, inclusive."
    current = d1
    while current <= d2:
        yield current
        current += datetime.timedelta(days=1)

def parse_date(value, format, return_datetime=False):
    """
    Equivalent to time.strptime, but it returns a datetime.date or
    datetime.datetime object instead of a struct_time object.
    Returns None if the value evaluates to False.
    """
    # See http://docs.python.org/lib/node85.html
    idx = return_datetime and 7 or 3
    func = return_datetime and datetime.datetime or datetime.date
    if value:
        return func(*time.strptime(value, format)[:idx])
    return None

def parse_time(value, format):
    """
    Equivalent to time.strptime, but it returns a datetime.time object.
    """
    return datetime.time(*time.strptime(value, format)[3:6])


def parse_date_from_text(text):
    if text is None or not text.strip():
        return None
    try:
        return duparse(text).date()
    except:
        return None

UNDEFINED = 'abcde'

# Sequence of event sources that aggregate their events from other, primary
# event sources.
EVENT_AGGREGATORS = ('Eventful',)

Results = namedtuple('Results', [
    'schema',
    'update_start',
    'update_finish',
    'num_added',
    'num_changed',
    'num_skipped',
    'num_hidden',
    'got_error',
    'traceback',
    'num_geocode_succeeded',
    'num_geocode_attempted',
])

class ScraperBroken(Exception):
    pass

class ScraperCodeError(Exception):
    pass

class Scraper(object):
    primary_key = UNDEFINED # Set to None if the scraper should never look for existing records.
    schema = None
    sleep = 0
    timeout = 20
    update = True # Whether to update old records.

    # If a record is found with an item_date more than 14 days old, the
    # pub_date will be set to the item_date. Set to None to bypass this
    # behavior for an individual scraper (i.e., always set pub_date to
    # the time the scraper runs).
    fresh_days = 14

    # Set this to False for data sets where we don't have a natural item_date.
    # In this case, the scraper will set it to datetime.date.now() the first
    # time it sees a record, and it will stay that way for subsequent updates
    # to the same record.
    item_date_available = True

    # Hook for setting NewsItem.title from values in datadict. By default, just
    # use the dictionary key 'title'.
    # Other examples:
    #   "{secondary_type.name}" (for a lookup object)
    #   "News at {street_number} {street_name} {street_suffix}"
    title_format = u'{title}'

    def __init__(self, retriever=None):
        self.retriever = retriever
        self.logger = logging.getLogger('start fetching data')
        #self._geocoder = SmartGeocoder()
        self.clear_cache()
        self.h = httplib2.Http(timeout=20, disable_ssl_certificate_validation=True)
        self.user_agent = 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)'
        self._cookies = SimpleCookie()
        self.scraper_name = None
        self.is_dry_run = False
        self.num_geocode_attempted = 0
        self.num_geocode_succeeded = 0

    def clear_cache(self):
        self._metro_object_cache = None
        self._schema_fields_cache = None
        self._schema_object_cache = None
        self.created_newsitem_ids = []
        if hasattr(self, '_temp_files'):
            for filename in self._temp_files:
                os.unlink(filename)
        self._temp_files = []
        self.cleanup()

    def cleanup(self):
        """
        This is a hook for cleaning up after the scraper is done. It's
        guaranteed to be called at the end of run() and dry_run().
        """
        pass

    def dry_run(self, prepare=False, save_ungeocoded_addresses=False):
        """
        Run the scraper, but do not create or update any NewsItem objects.
        If `prepare` is True, this method will geocode location_names and create
        Lookup objects.
        If `save_ungeocoded_addresses` is a string value, the string will be
        treated as a file path, and all addresses that fail to be geocoded
        will be saved into that file.
        """
        for item in self.dry_run_iter(prepare, save_ungeocoded_addresses):
            pass

    def dry_run_iter(self, prepare=False, save_ungeocoded_addresses=False):
        """
        Just like dry_run(), but returns a generator that iterates over the
        data dictionaries created.
        """
        import pprint
        self.is_dry_run = True
        self.cache_retriever = CacheRetriever(self)
        self.start_time = datetime.datetime.now()
        self.start_date = self.start_time.date()
        if save_ungeocoded_addresses:
            self.ungeocoded_addresses = {}
            self.geocode = self.geocode_and_log

        try:
            for datadict in self.data():
                if prepare:
                    datadict = self.prepare_data(datadict)
                pprint.pprint(datadict)
                yield datadict
        finally:
            self.clear_cache()

        self.logger.info('Geocoding succeeded/attempted: {0}/{1}'.format(self.num_geocode_succeeded, self.num_geocode_attempted))
        if save_ungeocoded_addresses:
            self.create_geocoding_report()

    def run(self, raise_errors=True):
        self.logger.info("run() started")
        self.num_added = self.num_changed = 0
        self.start_time = datetime.datetime.now()
        self.start_date = self.start_time.date()
        # We use a try/finally here so that the DataUpdate object is created
        # regardless of whether the scraper raised an exception.
        results = None

	filename = 'gsalr-data.txt'
	ni = []
        try:
            got_error = True
            for datadict in self.data():
                ni.append(self.save(datadict))
            got_error = False
 	    with open(filename, 'w+') as outfile:
                json.dump(ni, outfile)

        except:
            # Record exceptions in the finally block
            if raise_errors:
                raise
            else:
                pass
        finally:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # Rollback, in case the database is in an aborted transaction. This
            # avoids the "psycopg2.ProgrammingError: current transaction is aborted,
            # commands ignored until end of transaction block" error.
#            from django.db import connection
 #           connection._rollback()
            finish_time = datetime.datetime.now()
            self.clear_cache()

            results = Results(schema=self.schema,
                update_start=self.start_time,
                update_finish=finish_time,
                num_added=self.num_added,
                num_changed=self.num_changed,
                num_skipped=0,
                num_hidden=0,
                got_error=got_error,
                traceback=''.join([x for x in traceback.format_exception(exc_type, exc_value, exc_traceback)]),
                num_geocode_succeeded=self.num_geocode_succeeded,
                num_geocode_attempted=self.num_geocode_attempted,
            )

            self.logger.info('Records added: %s', self.num_added)
            self.logger.info('Records changed: %s', self.num_changed)
            self.logger.info('Geocoding succeeded/attempted: {0}/{1}'.format(self.num_geocode_succeeded, self.num_geocode_attempted))
        return results

    def prepare_data(self, datadict):
        # Get/create Lookup objects for lookup fields and set the value to the
        # Lookup ID.
        # Set the NewsItem fields in case they don't exist.
        datadict['description'] = datadict.get('description', '')
        datadict['url'] = datadict.get('url', '')

        # Set the NewsItem.pub_date according to the fresh_days value.
        datadict['pub_date'] = datadict.get('pub_date', self.start_time)
        if self.fresh_days is not None and (self.start_date - datadict['item_date']).days >= self.fresh_days:
            datadict['pub_date'] = datetime.datetime.combine(datadict['item_date'], datetime.time(0, 0))

        # Calculate location.
        datadict['location'] = str(datadict.get('location', None))
	datadict['time'] = datadict.get('time', None)
	datadict['is_cancelled'] = datadict.get('is_cancelled', None)
	
        # Calculate title.
        TITLE_MAXLENGTH = 255
        datadict['title'] = self.title_format.format(**datadict)
        if len(datadict['title']) >= TITLE_MAXLENGTH - 3:
            datadict['title'] = datadict['title'][:TITLE_MAXLENGTH - 3] + u'...'

        # Convert non-many-to-many Lookup objects back to the IDs.
        return datadict

    def save(self, datadict):
        datadict = self.prepare_data(datadict)
        #if datadict['location'] is None:
         #   return
        old_newsitem = None

        # Special case for item_date_available == False.
        # Unfortunately this logic can't live in prepare_data() because we
        # don't have old_newsitem at that point.
        if not self.item_date_available:
            if old_newsitem is None:
                datadict['item_date'] = datetime.date.today()
            else:
                datadict['item_date'] = old_newsitem.item_date
        ni = self.create_newsitem(datadict)
        self.num_added += 1
 #       self.logger.info(u'Created NewsItem %s (total created in this scrape: %s)', ni.id, self.num_added)
#        self.created_newsitem_ids.append(ni.id)
        return ni

    def create_newsitem(self, datadict):
        ni = {}#NewsItem.objects.create(
            #schema=self.schema_object,
        ni['title']=datadict['title'],
        ni['description']=datadict['description'],
        ni['url']=datadict['url'],
        ni['pub_date']=str(datadict['pub_date']),
        ni['item_date']=str(datadict['item_date']),
        ni['location']=datadict['location'],
        ni['location_name']=datadict['location_name'],
        ni['location_id']=None, # Scrapers shouldn't post to locations. In theory.
	ni['time']=datadict['time']

        return ni

    def data(self):
        """
        Yields final dictionaries of data, each of which MUST contain the
        following keys:
            title -- string
            item_date -- datetime.date
            location_name -- string
        Also, an item_date (datetime.date) must exist, except if you've
        specified item_date_available=False in the scraper.
        These keys might also exist:
            url
            location_name_geocoder
        Other data keys correspond to SchemaField.name.
        If a value of the dictionary is a list or dictionary, it will
        automatically be converted to JSON before being inserted into the
        db_attribute table as a string.
        If a key of the dictionary is a lookup=True SchemaField, then the value
        should be the Lookup.code value, not the Lookup ID or Lookup object.
        """
        raise NotImplementedError()

    def broken(self, message):
        raise ScraperBroken(message)

    def get(self, uri, *args, **kwargs):
        "Returns HTML for the given URL and POST data."
        parse_result = urlparse.urlparse(uri)
        if parse_result.scheme == 'ftp':
            return self.ftp_get(parse_result)
        else:
            return self.get_html(uri, *args, **kwargs)

    def get_to_file(self, *args, **kwargs):
        """
        Retrieves the given URL and POST data to a local file. Returns the filename.
        The Scraper automatically deletes the file when scraping is done.
        """
        filename = self.retriever.get_to_file(*args, **kwargs)
        self._temp_files.append(filename) # Keep track so we can delete after scrape is done.
        return filename

    def cache_get(self, prefix, suffix, url, make_pretty=False, **kwargs):
        """
        Download the file at the given URL and return its contents as a string.
        If a dry run is in process, save it as a file in a cache directory using
        the given prefix and suffix.
        """
        if self.is_dry_run:
            return self.cache_retriever.get(prefix, suffix, url, make_pretty=make_pretty, **kwargs)
        else:
            return self.get(url, **kwargs)

    def cache_get_to_file(self, prefix, suffix, url, **kwargs):
        """
        Download the file at the given URL, save it to disk, and return its
        filename. If a dry run is in process, save it as a file in a cache
        directory using the given prefix and suffix.
        """
        if self.is_dry_run:
            return self.cache_retriever.get_to_file(prefix, suffix, url, **kwargs)
        else:
            return self.get_to_file(url, **kwargs)

    def get_to_file(self, *args, **kwargs):
        """
        Downloads the given URI and saves it to a temporary file. Returns the
        full filename of the temporary file.
        """
        import os
        from tempfile import mkstemp
        fd, name = mkstemp()
        fp = os.fdopen(fd, 'wb')
        fp.write(self.get_html(*args, **kwargs))
        fp.close()
        return name

    def get_html(self, uri, data=None, headers=None, send_cookies=True, follow_redirects=True, raise_on_error=True, basic_auth=None):
        return self.get_html_and_headers(uri, data, headers, send_cookies, follow_redirects, raise_on_error, basic_auth)[0]

    def get_html_and_headers(self, uri, data=None, headers=None, send_cookies=True, follow_redirects=True, raise_on_error=True, basic_auth=None):
        if self.sleep and self.page_downloaded:
            self.logger.debug('Sleeping for %s seconds', self.sleep)
            time.sleep(self.sleep)
        self.page_downloaded = True

        # Prepare the request.
        if not headers:
            headers = {}
        headers['user-agent'] = headers.get('user-agent', self.user_agent)

        method = data and "POST" or "GET"
        body = urlencode(data) if isinstance(data, dict) else data
        if method == "POST" and body and 'Content-Type' not in headers:
            headers.setdefault('Content-Type', 'application/x-www-form-urlencoded')

        # Get the response.
        resp_headers = None
        for attempt_number in range(3):
            self.logger.debug('Attempt %s: %s %s', attempt_number + 1, method, uri)
            if data:
                self.logger.debug('Data: %r', data)
            if headers:
                self.logger.debug('Headers: %r' % headers)
            try:
                resp_headers, content = self.h.request(uri, method, body=body, headers=headers)
                if resp_headers['status'] == '500':
                    self.logger.debug("Request got a 500 error: %s %s", method, uri)
                    continue # Try again.
                break
            except socket.timeout:
                self.logger.debug("Request timed out after %s seconds: %s %s", self.h.timeout, method, uri)
                continue # Try again.
            except socket.error, e:
                self.logger.debug("Got socket error: %s", e)
                continue # Try again.
            except httplib2.ServerNotFoundError:
                raise RetrievalError("Could not %s %r: server not found" % (method, uri))
        if resp_headers is None:
            raise RetrievalError("Request timed out 3 times: %s %s" % (method, uri))

        # Raise RetrievalError if necessary.
        if raise_on_error and resp_headers['status'] in ('400', '408', '500'):
            raise RetrievalError("Could not %s %r: HTTP status %s" % (method, uri, resp_headers['status']))
        if raise_on_error and resp_headers['status'] == '404':
            raise PageNotFoundError("Could not %s %r: HTTP status %s" % (method, uri, resp_headers['status']))

        # Set any received cookies.
        if 'set-cookie' in resp_headers:
            try:
                self._cookies.load(resp_headers['set-cookie'])
            except CookieError:
                # Skip invalid cookies.
                pass

        # Handle redirects that weren't caught by httplib2 for whatever reason.
        if follow_redirects and resp_headers['status'] in ('301', '302', '303'):
            try:
                new_location = resp_headers['location']
            except KeyError:
                raise RetrievalError('Got redirect, but the response was missing a "location" header. Headers were: %r' % resp_headers)
            self.logger.debug('Got %s redirect', resp_headers['status'])

            # Some broken Web apps send relative URLs in their "Location"
            # headers in redirects. Detect that and use urljoin() to get a full
            # URL.
            if not new_location.startswith('http://') and not new_location.startswith('https://'):
                new_location = urljoin(uri, new_location)
            # Clear the POST data, if any, so that we do a GET request.
            if data:
                data = {}
                del headers['Content-Type']
            return Retriever.get_html_and_headers(self, new_location, data, headers, send_cookies)

        return content, resp_headers


    def ftp_get(self, parse_result):
        ftp = ftplib.FTP(parse_result.netloc)
        ftp.login()
        sio = StringIO()
        ftp.retrbinary('RETR {0}'.format(parse_result.path), sio.write)
        ftp.quit()
        return sio.getvalue()

    def save_tempfile(self, data):
        """
        Saves data (a string) to a temporary file and returns the filename.
        The Scraper automatically deletes the file when scraping is done.
        """
        from tempfile import mkstemp
        fd, name = mkstemp()
        fp = os.fdopen(fd, 'wb')
        fp.write(data)
        fp.close()
        return name

    def json(self, json_string):
        return json.loads(json_string)

    def pulldom(self, filename, element_name):
        """
        Return an iterator over dictionaries elements that match `element_name`.
        This uses the pulldom parser, so it's more memory efficient for large
        xml files.
        """
        from xml.dom import pulldom
        from xml.dom import Node
        fh = open(filename, 'r')
        events = pulldom.parse(fh)
        for event in events:
            node_type, node = event
            if node_type == 'START_ELEMENT' and node.nodeName == element_name:
                events.expandNode(node)
                node.normalize()
                record = {}
                # TODO: This only parses a flat list of elements. It should
                # probably handle attributes or nested elements as well.
                for subnode in node.childNodes:
                    if subnode.nodeType != Node.TEXT_NODE:
                        if subnode.hasChildNodes():
                            record[subnode.nodeName] = subnode.firstChild.nodeValue
                        else:
                            record[subnode.nodeName] = ''
                yield record
        fh.close()

    def xml(self, xml_string):
        "Returns an etree XML object for the given string."
        return etree.fromstring(xml_string)

    def xpath(self, xml_string, *xpath_args, **xpath_kwargs):
        "Does the given XPath query on the given string and yields etree elements."
        xml = etree.fromstring(xml_string)
        for el in etree.XPath(*xpath_args, **xpath_kwargs)(xml):
            yield el

    def dict_from_element(self, elem, remove_namespaces=False):
        """
        Convert the given XML element into a dictionary object. The element must
        be an lxml.etree Element object. If remove_namespaces is True, then
        attempt to get rid of the namespace before inserting the key.
        """
        items = ((child.tag, child.text) for child in elem.findall('*'))
        if remove_namespaces:
            old_items = items
            items = []
            for k, v in old_items:
                if '}' in k:
                    k = k.split('}')[1]
                items.append((k, v))
        return dict(items)

    def csv(self, fp, lower_keys=False):
        import csv

        if lower_keys:
            reader = csv.reader(fp)
            # Make the keys lower-case so the code is easier to work with.
            keys = tuple(t.lower() for t in reader.next())
            for row in reader:
                yield dict(zip(keys, row))
        else:
            reader = csv.DictReader(fp)
            for row in reader:
                yield row

    def zipfile(self, fp):
        import zipfile
        return zipfile.ZipFile(fp, 'r')

    def regex(self, regex, data):
        "Returns an iterator over dicts that match the given regex."
        for record in regex.finditer(data):
            yield record.groupdict()

    def parse_socrata(self, json_string):
        """
        Parses a Socrata API "view" and yields dictionaries of the values.
        See http://data.seattle.gov/api/docs/views
        This assumes the OLD Socrata API and probably shouldn't be used for new
        code.
        """
        j = self.json(json_string)
        cols = [c['fieldName'].strip() for c in j['meta']['view']['columns']]
        for row in j['data']:
            yield dict(zip(cols, row))

    def download_socrata_data(self, base_url, view_id, inline_query, columns=None):
        retriever = SocrataRetriever(self, columns)
        for record in retriever.get_records(base_url, view_id, inline_query):
            yield record

    def date(self, *args, **kwargs):
        "Parses a date."
        return parse_date(*args, **kwargs)

    def datetime(self, *args, **kwargs):
        "Parses a datetime."
        kwargs['return_datetime'] = True
        return parse_date(*args, **kwargs)

    def time(self, *args, **kwargs):
        "Parses a time."
        return parse_time(*args, **kwargs)

    def address_to_block(self, *args, **kwargs):
        return text_utils.address_to_block(*args, **kwargs)

    def smart_title(self, *args, **kwargs):
        return text_utils.smart_title(*args, **kwargs)

    def sentence_case(self, *args, **kwargs):
        return text_utils.sentence_case(*args, **kwargs)

    def geocode(self, location_name):
        """
        Tries to geocode the given location string
	"""
        try:
            return self._geocoder.geocode(location_name)
        except (GeocodingException, ParsingError):
            return None

    def geocode_and_log(self, location_name):
        #Same as geocode() method, but keeps track of addresses that fail to
        #geocode.
        try:
            return self._geocoder.geocode(location_name)
        except (GeocodingException, ParsingError) as ex:
            # The ungeocoded_addresses attribute only exists if dry_run() is
            # invoked.
            self.ungeocoded_addresses[location_name] = ex
            return None

    def point(self, longitude, latitude, *args, **kwargs):
        return Point(longitude, latitude, *args, **kwargs)

    def geos_geometry(self, wkt):
        try:
            return GEOSGeometry(wkt)
        except:
            raise GEOSException

    def clean_html(self, html):
	text=html
        NAMED_ENTITY_SPECIAL_CASES = {
            'apos': u"'",
        }
        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
            # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))
                    else:
                        return unichr(int(text[2:-1]))
                except ValueError:
                    pass
            else:
            # named entity
                entity = text[1:-1]
                try:
                    text = unichr(htmlentitydefs.name2codepoint[entity])
                except KeyError:
                    try:
                        return NAMED_ENTITY_SPECIAL_CASES[entity]
                    except KeyError:
                        pass
            return text # leave as is

         # Keep running this regular expression on the text until there are no
         # changes. We do this to catch double- and triple-escaped stuff.
        while 1:
            previous_text = text
            text = re.sub("&#?\w+;", fixup, text)
            if text == previous_text:
                break # Otherwise, do it again.
        return text

    def clean_xml(self, xml_string):
        return re.sub(r'&(?!amp;)', '&amp;', xml_string) # Fix unescaped ampersands.

    def clean_address(self, text):
        return text_utils.clean_address(text)

    def strip_tags(self, html):
        return strip_tags(html)

    def intcomma(self, text):
        return text_utils.intcomma(text)

    def normalize(self, text):
        return normalize(text)

    def smallest_containing_location(self, geom, location_type_slug='neighborhoods'):
        #Returns the smallest Location object of the given location_type that
        #intersects the given geometry. Returns None if no locations intersect.
        
        try:
            return Location.objects.filter(location_type__slug=location_type_slug, location__intersects=geom, is_public=True).order_by('-area')[0]
        except IndexError:
            return None

    def snap_date(self, hour, minute):
        """
        "Snaps" to the next datetime with the given 12-hour hour/minute.
        For example, if hour=6 and minute=0, then this will return the next
        6:00, either a.m. or p.m.
        This is used for frequently run scrapers for which we want to bunch
        data into two daily bunches.
        """
        assert hour >= 0 and hour <= 11, 'hour must be between 0 and 11, inclusive'
        now = datetime.datetime.now()
        today_morning = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        today_afternoon = now.replace(hour=hour+12, minute=minute, second=0, microsecond=0)
        tomorrow_morning = (now + datetime.timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        if self.start_time < today_morning:
            return today_morning
        elif self.start_time < today_afternoon:
            return today_afternoon
        else:
            return tomorrow_morning

    def snap_date_back(self, hour, minute, start_datetime=None):
        """
        "Snaps" backward to the previous datetime divisible by the given hour.
        The minute will be hard-coded to the given value. For example, if it's
        currently noon and hour=4, minute=20, this will return 8:20 a.m. of
        today's date.
        """
        if start_datetime is None:
            result = datetime.datetime.now()
        else:
            result = start_datetime
        while result.hour % hour > 0:
            result -= datetime.timedelta(hours=1)
        return result.replace(minute=minute)

    def bunch(self, iterable, key_func):
        import itertools
        for k, g in itertools.groupby(iterable, key_func):
            yield list(g)

    def get_duplicate_events(self, datadict):
        """
        Returns True if there already exists an event newsitem with the same
        title, item_date, location, and time, but NOT the same unique_id.
        """
        schema = Schema.objects.get(slug='events')
        time_field = schema.schemafield_set.get(name='time')
        unique_id_field = schema.schemafield_set.get(name='unique_id')

        qs = NewsItem.objects.filter(schema=schema, title=datadict['title'],
            item_date=datadict['item_date'], location=datadict['location'])
        qs = qs.by_attribute(time_field, datadict['time'])
        qs = qs.extra(where=[
            "db_attribute.{0} <> '{1}'".format(unique_id_field.real_name, datadict['unique_id']),
        ])
        return qs

    def event_is_from_aggregator(self, event_newsitem):
        """
        Returns True if the given event newsitem comes from an event aggregator
        (such as Eventful), False otherwise.
        """
        return Lookup.objects.get(pk=event_newsitem.attributes['source']).name in EVENT_AGGREGATORS

    def handle_duplicate_events(self, datadict):
        """
        Return True if it's OK to update our database with the given datadict,
        False otherwise. If the duplicate event is from an event aggregator,
        change its unique_id value so that it gets treated as if it originally
        came from the current event scraper.
        """
        dup_events = self.get_duplicate_events(datadict)
        if dup_events.count():
            # If there's more than one duplicate event, don't do anything.
            if dup_events.count() > 1:
                return False

            # If the duplicate event is from an event aggregator, change its
            # unique_id value.
            dup_event = dup_events[0]
            if self.event_is_from_aggregator(dup_event):
                self.logger.info('Changed the unique_id of duplicate event %d from %s to %s' % (dup_event.id, dup_event.attributes['unique_id'], datadict['unique_id']))
                dup_event.attributes['unique_id'] = datadict['unique_id']
                return True
            else:
                return False
        else:
            return True

    def create_geocoding_report(self, report_filename='ungeocoded_addresses.txt'):
        counter = defaultdict(int)  # keep a tally of exception counts

        with open(report_filename, 'w') as fout:
            # Output each address that failed to geocode.
            for address, ex in self.ungeocoded_addresses.iteritems():
                class_name = ex.__class__.__name__
                counter[class_name] += 1
                fout.write('{0} ({1}: {2})\n'.format(address, class_name, ex))
            fout.write('=' * 80 + '\n')

            # Output the geocoding exceptions along with the number of occurrences.
            stats = counter.items()
            stats.sort(key=lambda x: -x[1])
            for name, count in stats:
                fout.write('{0} -> {1}\n'.format(name, count))

        self.logger.info('Saved ungeocoded addresses to ungeocoded_addresses.txt')

    def get_lookup(self, schemafield_name, code):
        """
        Return a Lookup object corresponding to a schema field on the current
        schema.
        """
        try:
            return Lookup.objects.get(schema_field__schema__slug=self.schema,
                schema_field__name=schemafield_name, code=code)
        except Lookup.DoesNotExist:
            return None

    def is_in_metro(self, city, state, extra_names=None):
        """
        Return True if the given city and state fall within the current metro;
        False otherwise.
        Note that this handles metros that have multiple cities.
        """
        if self.metro['state'] != state:
            return False

        names = [self.metro['city_name'].upper()]
        if extra_names:
            names += extra_names
        if self.metro['multiple_cities']:
            names += self.metro['city_list']

        return city.upper() in names

    def format_time(self, dt):
        """
        Given a datetime or time object, return a string in the format %I:%M %p,
        truncating the leading zero if present.
        """
        return '{0:%I:%M %p}'.format(dt).lstrip('0')
