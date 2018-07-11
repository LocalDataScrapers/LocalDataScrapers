import os
import os.path as op
import sys
import urllib2
import json
import csv
import collections
import inspect
import re


class SocrataRetriever(object):
    """
    Retrieve Socrata using an inline query. The query will use
    method=getByIds, which pages results, so they need to be fetched via
    multiple requests. This method of downloading Socrata data will become
    the de facto standard, and the old download-everything-at-once method
    will be deprecated.
    """

    # Number of records to download per Socrata request (max is 1000).
    SOCRATA_RECORD_COUNT = 1000

    def __init__(self, scraper, columns):
        self.scraper = scraper
        self.columns = columns

    def json_get(self, prefix, url, data=None):
        json_str = self.scraper.cache_get(prefix, 'json', url, data=data,
            headers={'Content-Type': 'application/json'})
        return json.loads(json_str)

    def get_records(self, base_url, view_id, inline_query):
        # First download the column metadata.
        if self.columns:
            columns = self.columns
        else:
            columns_url = op.join(base_url, 'api/views/{0}/columns.json'.format(view_id))
            columns = [col['fieldName'].strip() for col in self.json_get('columns', columns_url)]

        data_url_prefix = op.join(base_url, 'views/INLINE/rows.json?method=getByIds&length={0}&start='.format(self.SOCRATA_RECORD_COUNT))

        index = 0
        while True:
            # Download a page of data.
            data_url = data_url_prefix + str(index)
            rows = self.json_get('data', data_url, inline_query)['data']
            # Make sure the number of columns equals the number of elements in
            # the first row. Note that the first 8 elements in a row are Socrata
            # metadata, and not useful to us.
            assert len(rows[0]) == len(columns) + 8
            for row in rows:
                yield dict(zip(columns, row[8:]))   # ignore first 8 elements

            # If the number of items we got back is less than the requested amount,
            # then there aren't any more items left to fetch.
            if len(rows) < self.SOCRATA_RECORD_COUNT:
                break
            else:
                index += self.SOCRATA_RECORD_COUNT


class CacheRetriever(object):
    """
    This class is used for troubleshooting broken scrapers or developing new
    scrapers. It retrieve the contents of a given URL from a disk-based cache
    or, failing that, from the original source.
    """
    def __init__(self, scraper):
        self.scraper = scraper

        # Keep track of the next index number for the given prefix.
        self.prefix_counts = collections.defaultdict(int)

        # Set the output directory based on the module file path.
        module_path = inspect.getmodule(scraper.__class__).__file__
        self.output_dir = '.'.join(module_path.split('/')[-3:-1])
        if self.output_dir == '':
            self.output_dir = './'
        if not op.exists(self.output_dir):
            os.mkdir(self.output_dir)

        # Create a file that logs URLs and their respective cache files.
        url_cache_map_file = op.join(self.output_dir, 'url_cache_map.txt')
        if op.exists(url_cache_map_file):
            os.remove(url_cache_map_file)
        self.cache_map_fp = open(url_cache_map_file, 'w')

    def get(self, prefix, suffix, url, make_pretty=False, **kwargs):
        """
        Same as get_to_file(), but returns the contents of the cached file as a
        string.
        """
        cache_path = self.get_to_file(prefix, suffix, url, make_pretty, **kwargs)
        return open(cache_path).read()

    def get_to_file(self, prefix, suffix, url, make_pretty=False, **kwargs):
        """
        Fetch the contents of the given URL from cache, if available; otherwise,
        fetch from the original source. If caching is not enabled, just fetch
        it from the original source. The given prefix helps determines the file
        name of the cache file.
        """
        self.prefix_counts[prefix] += 1
        cache_path = '{0}/{1}-{2:03}.{3}'.format(self.output_dir, prefix,
            self.prefix_counts[prefix], suffix)
        if not op.exists(cache_path):
            self.append_to_url_map(url, cache_path)
            result = self.scraper.get(url, **kwargs)

            with open(cache_path, 'w') as fout:
                if make_pretty:
                    result = self.prettify(suffix, result)
                fout.write(result)

        print 'Retrieving {0} from {1}'.format(url, cache_path)
        return cache_path

    def append_to_url_map(self, url, cache_path):
        "Log the mapping between a URL and its cache file."
        self.cache_map_fp.write('{0} -> {1}\n'.format(url, cache_path))
        self.cache_map_fp.flush()

    def prettify(self, format, code):
        if format == 'json':
            return json.dumps(json.loads(code), indent=4)
        else:
            return code

class SocrataAnalyzer:
    def __init__(self, base_url):
        self.base_url = base_url

    def load(self, path, key_fields=None, key_fn=None):
        if key_fields:
            key_fn = lambda row: '|'.join(row[k] for k in key_fields)

        result = {}
        for row in self.get_rows(path):
            key = key_fn(row)
            result[key] = row

        return result

    def print_columns(self):
        json_string = urllib2.urlopen(op.join(self.base_url, 'columns.json')).read()
        columns = json.loads(json_string)
        for col in columns:
            print '%s (%s): %s' % (col['name'], col['fieldName'], col['dataTypeName'])

    def get_rows(self, path_or_fp):
        if isinstance(path_or_fp, basestring):
            fp = open(path_or_fp)
        else:
            fp = path_or_fp

        result = json.load(fp)
        cols = [c['fieldName'].strip() for c in result['meta']['view']['columns']]
        return (dict(zip(cols, row)) for row in result['data'])

    def print_lookup_values(self, path, value_field, name_field=None):
        counter = collections.Counter()
        name_map = {}

        for row in self.get_rows(path):
            value = row[value_field]
            counter[value] += 1
            if name_field:
                name_map[value] = row[name_field]

        items = counter.items()
        items.sort(key=lambda t: -t[1])
        for value, count in items:
            head = value
            if name_field:
                head += ' (%s)' % name_map[value]
            print '%s -> %d' % (head, count)

    def check_one_to_one(self, path, key_fields, value_field):
        """
        """
        map = {}
        errors = collections.defaultdict(set)
        for row in self.get_rows(path):
            key = '|'.join(row[k] for k in key_fields)
            value = row[value_field]
            if key in map:
                if map[key] != value:
                    errors[key].add(value)
            else:
                map[key] = value

        if errors:
            for k, v in errors.iteritems():
                print '%s -> %d' % (k, len(v))
        else:
            print '%s and %s are one-to-one!' % (key_fields, value_field)

    def check_primary_key_overlap(self, path, key_fields):
        counter = collections.Counter()

        for row in self.get_rows(path):
            key = '|'.join(row[k] if row[k] is not None else 'NONE' for k in key_fields)
            counter[key] += 1

        items = counter.items()
        items.sort(key=lambda t: -t[1])
        for key, count in items:
            if count > 1:
                print '%s -> %s' % (key, count)

    def make_csv(self, path):
        with open(path) as fin:
            result = json.load(fin)

        cols = [c['fieldName'].strip() for c in result['meta']['view']['columns']]

        writer = csv.writer(sys.stdout)
        writer.writerow(cols)
        for row in result['data']:
            writer.writerow(row)


class RegexComposer(object):
    def __init__(self, expr, **kwargs):
        self.expr = expr
        self.kwargs = kwargs
        self._re = None

    def __setitem__(self, key, val):
        self.kwargs[key] = val

    @property
    def regex(self):
        if self._re:
            return self._re

        kwargs = {}
        for k, v in self.kwargs.items():
            kwargs[k] = '(?P<{}>{})'.format(k, v)
        self._re = re.compile(self.expr.format(**kwargs))
        return self._re

    @property
    def pattern(self):
        return self._re.pattern

    def match(self, text):
        m = self.regex.match(text)
        return m.groupdict() if m else None

    def search(self, text):
        m = self.regex.search(text)
        return m.groupdict() if m else None
