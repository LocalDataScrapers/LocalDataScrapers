import inspect
import urlparse
import re
import zipfile
import csv
import json

from pyquery import PyQuery
import xmltodict
import icalendar
import feedparser


class PipelineStage(object):
    step_returns_context_manager = False

    def __init__(self, input_key=None, output_key=None):
        self.pipeline = None
        self.input_key = input_key
        self.output_key = output_key

    def as_generator_func(self, pipeline):
        self.pipeline = pipeline
        if self.input_key and self.output_key:
            return self.run_with_dicts
        else:
            return self.run

    def run(self, source):
        if self.step_returns_context_manager:
            for data in source:
                obj = self.step(data)
                with obj:
                    yield obj
        else:
            for data in source:
                yield self.step(data)

    def run_with_dicts(self, source):
        if self.step_returns_context_manager:
            for dic in source:
                dic[self.output_key] = self.step(dic[self.input_key])
                with dic[self.output_key]:
                    yield dic
        else:
            for dic in source:
                dic[self.output_key] = self.step(dic[self.input_key])
                yield dic

    def step(self, obj):
        raise NotImplementedError


class Flatten(PipelineStage):
    def run(self, source):
        for gen in source:
            for value in gen:
                yield value


class Download(PipelineStage):
    def step(self, url):
        return self.pipeline.get(url).text


class DownloadFile(PipelineStage):
    step_returns_context_manager = True

    def step(self, url):
        return self.pipeline.get_to_file(url)


class ParseHtml(PipelineStage):
    def __init__(self, *args, **kwargs):
        super(ParseHtml, self).__init__(*args, **kwargs)

    def step(self, data):
        return PyQuery(data)


class ParseXml(PipelineStage):
    def __init__(self, *args, **kwargs):
        # Choices: xmltodict, lxml.etree
        self.mode = kwargs.pop('mode', 'xmltodict')
        super(ParseXml, self).__init__(*args, **kwargs)

    def step(self, data):
        return xmltodict.parse(data)


class ParseCsv(PipelineStage):
    step_returns_context_manager = True

    def __init__(self, *args, **kwargs):
        self.use_dict_reader = kwargs.pop('use_dict_reader', True)
        super(ParseCsv, self).__init__(*args, **kwargs)

    def step(self, csv_file):
        cls = csv.DictReader if self.use_dict_reader else csv.reader
        return cls(csv_file)


class ParseJson(PipelineStage):
    def step(self, data):
        return json.loads(data) if data else {}


class Zip(PipelineStage):
    step_returns_context_manager = True

    def step(self, fileobj):
        return zipfile.ZipFile(fileobj)


class ParseFeed(PipelineStage):
    def step(self, data):
        return feedparser.parse(data)


class ParseICalendar(PipelineStage):
    keys = ('uid', 'url', 'summary', 'location', 'dtstart', 'dtend',
        'description')

    def step(self, data):
        return list(self.get_events(data))

    def get_events(self, ical_string):
        """
        Returns a sequence of dicts. Each dict corresponds to an event component
        found in the given iCalendar string. The dict does not contain the exact
        object found in the event component: strings are converted to unicode
        strings and vDDDTypes are converted to datetime objects.
        """
        ical_string = ParseICalendar.fix_description_field(ical_string)
        cal = icalendar.Calendar.from_ical(ical_string)

        for component in cal.walk():
            if component.name == 'VEVENT':
                dic = {}
                for key in self.keys:
                    obj = component.get(key)

                    # Only keep datetimes and strings as values.
                    if isinstance(obj, icalendar.vDDDTypes):
                        obj = obj.dt
                    elif obj is not None:
                        obj = unicode(obj)
                    dic[key] = obj

                yield dic

    @staticmethod
    def fix_description_field(ical_string):
        """
        Prepend a space in lines that fall inside the DESCRIPTION field of an
        iCalendar string.
        """
        def get_lines():
            inside_desc = True
            for line in ical_string.splitlines():
                m = re.match(r'([A-Z]+)\:', line)
                field = m.group(1) if m else None

                if field is not None:
                    if field == 'DESCRIPTION':
                        inside_desc = True
                        yield line
                        continue
                    else:
                        inside_desc = False

                if inside_desc and not line.startswith(' '):
                    line = ' ' + line

                yield line

        return '\r\n'.join(get_lines())
