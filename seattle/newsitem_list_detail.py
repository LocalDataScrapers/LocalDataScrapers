from django.conf import settings
from django.db import transaction
#from everyblock.analytics.models import ScraperRun
from list_detail import ListDetailScraper
#from everyblock.retrieval.utils import locations_are_close
from scraper import Results
#from everyblock.db.models import Schema, NewsItem, Lookup, field_mapping
#from everyblock.streets.models import Spot
from text import address_to_block
#from everyblock.geocoder import SmartGeocoder, GeocodingException, ParsingError
import datetime
import re
import sys
import traceback


class NewsItemListDetailScraper(ListDetailScraper):
    """
    A ListDetailScraper that saves its data into the NewsItem table.
    Subclasses are required to set the `schema_slugs` attribute.
    self.schemas lazily loads the list of Schema objects the first time it's
    accessed. It is a dictionary in the format {slug: Schema}.
    self.schema is available if schema_slugs has only one element. It's the
    Schema object.
    self.lookups lazily loads a dictionary of all SchemaFields with
    lookup=True. The dictionary is in the format {name: schemafield}. If
    schema_slug has more than one element, self.lookups is a dictionary in the
    format {schema_slug: {name: schemafield}}.
    self.schema_field_mapping lazily loads a dictionary of each SchemaField,
    mapping the name to the real_name. If schema_slug has more than one element,
    self.schema_field_mapping is a dictionary in the format
    {schema_slug: {name: real_name}}.
    """
    schema_slugs = None
    logname = None
    record_check = None

    def __init__(self, *args, **kwargs):
        super(NewsItemListDetailScraper, self).__init__(*args, **kwargs)
        self._schema_cache = None
        self._schemas_cache = None
        self._lookups_cache = None
        self._schema_fields_cache = None
        self._schema_field_mapping_cache = None
        self._geocoder = None
        self.created_newsitem_ids = {} # Maps schema IDs to lists of created newsitems.
        module_name = re.match('^everyblock\.(.*)\.retrieval$', self.__module__)
        if module_name:
            self.scraper_name = module_name.group(1)
        else:
            self.scraper_name = None

    # schemas, schema, lookups and schema_field_mapping are all lazily loaded
    # so that this scraper can be run (in raw_data() or display_data()) without
    # requiring a valid database to be set up.

    def create_newsitem(self, attributes, **kwargs):
        """
        Creates and saves a NewsItem with the given kwargs. Returns the new
        NewsItem.
        kwargs MUST have the following keys:
            title
            item_date
            location_name
        For any other kwargs whose values aren't provided, this will use
        sensible defaults.
        kwargs may optionally contain a 'convert_to_block' boolean. If True,
        this will convert the given kwargs['location_name'] to a block level
        but will use the real (non-block-level) address for geocoding and Block
        association.
        kwargs may optionally contain a 'location_name_geocoder' string. If
        this exists, it will be used to geocode the NewsItem.
        attributes is a dictionary to use to populate this NewsItem's Attribute
        object.
        """
        geocoded_location = place = None
        if 'location_name_geocoder' in kwargs:
            string_to_geocode = kwargs['location_name_geocoder']
        else:
            string_to_geocode = kwargs['location_name']
        result = self.geocode(string_to_geocode)
        if result:
            geocoded_location = result['point']
            if result['point_type'] == 'address':
                _, place = Spot.objects.from_geocoder(result)
            elif result['point_type'] == 'intersection':
                place = result['intersection']
            else:
                place = result['block']
        if kwargs.pop('convert_to_block', False):
            kwargs['location_name'] = address_to_block(kwargs['location_name'])
            # If the exact address couldn't be geocoded, try using the
            # normalized location name.
            if geocoded_location is None:
                geocoded_location = self.geocode(kwargs['location_name'])
                if geocoded_location:
                    geocoded_location = location['point']

        # Normally we'd just use "schema = kwargs.get('schema', self.schema)",
        # but self.schema will be evaluated even if the key is found in
        # kwargs, which raises an error when using multiple schemas.
        schema = kwargs.get('schema', None)
        schema = schema or self.schema
        # Some schemas don't have Source_name or source_id schema fields TODO: Perhaps change to get or create lookup
        if 'source_id' not in attributes:
            if 'source' in attributes:
                source = Lookup.objects.get(id=attributes['source'])
            else:
                source = self.schema
            attributes['source_name'] = source.name
            attributes['source_id'] = source.id

        ni = NewsItem.objects.create(
            schema=schema,
            title=kwargs['title'],
            description=kwargs.get('description', ''),
            url=kwargs.get('url', ''),
            pub_date=kwargs.get('pub_date', self.start_time),
            item_date=kwargs['item_date'],
            location=kwargs.get('location', geocoded_location),
            location_name=kwargs['location_name'],
            spot=None,
            location_id=None, # Scrapers should never save a location_id.
            source_name=attributes['source_name'] if 'source_name' in attributes else attributes['source'],
            source_id=attributes['source_id'] if 'source_id' in attributes else attributes['source'],
        )
        ni.attributes = attributes
        self.num_added += 1
        self.logger.info(u'Created NewsItem %s (total created in this scrape: %s)', ni.id, self.num_added)
        self.created_newsitem_ids.setdefault(schema.id, []).append(ni.id)
        return ni

    def update_existing(self, newsitem, new_values, new_attributes):
        """
        Given an existing NewsItem and dictionaries new_values and
        new_attributes, determines which values and attributes have changed
        and saves the object and/or its attributes if necessary.
        """
        newsitem_updated = False
        # First, check the NewsItem's values.
        for k, v in new_values.items():
            if getattr(newsitem, k) != v:
                self.logger.info('ID %s %s changed from %r to %r' % (newsitem.id, k, getattr(newsitem, k), v))
                setattr(newsitem, k, v)
                newsitem_updated = True
        if newsitem_updated:
            newsitem.save()
        # Next, check the NewsItem's attributes.
        for k, v in new_attributes.items():
            if newsitem.attributes[k] != v:
                self.logger.info('ID %s %s changed from %r to %r' % (newsitem.id, k, newsitem.attributes[k], v))
                newsitem.attributes[k] = v
                newsitem_updated = True
        if newsitem_updated:
            self.num_changed += 1
            self.logger.debug('Total changed in this scrape: %s', self.num_changed)
        else:
            self.logger.debug('No changes to NewsItem %s detected', newsitem.id)
    
    def update(self, raise_errors=True):
        """
        Updates the Schema.last_updated fields after scraping is done.
        """
        self.num_added = 0
        self.num_changed = 0
        update_start = datetime.datetime.now()

        # We use a try/finally here so that the DataUpdate object is created
        # regardless of whether the scraper raised an exception.
        try:
            got_error = True
            super(NewsItemListDetailScraper, self).update()
            got_error = False
        except:
            # Record exceptions in the finally block
            if raise_errors:
                raise
            else:
                pass
        finally:
            # Rollback, in case the database is in an aborted transaction. his
            # avoids the "psycopg2.ProgrammingError: current transaction is aborted,
            # commands ignored until end of transaction block" error.
            exc_type, exc_value, exc_traceback = sys.exc_info()

            update_finish = datetime.datetime.now()
            
            # Clear the Schema cache, in case the schemas have been updated in the
            # database since we started the scrape.
#            self._schemas_cache = self._schema_cache = None

            results = []
        return results

    def geocode(self, location_name):
        """
        Tries to geocode the given location string, returning an Address
        dictionary (as returned by everyblock.geocoder) or None.
        """
        try:
            return self._geocoder.geocode(location_name)
        except (GeocodingException, ParsingError):
            return None

    def safe_location(self, location_name, geom, max_distance=200):
        """
        Returns a location (geometry) to use, given a location_name and
        geometry. This is used for data sources that publish both a geometry
        and a location_name -- we double-check that the geometry is within
        a certain `max_distance` from the geocoded location_name.
        If there's a discrepancy or if the location_name can't be geocoded,
        this returns None.
        Note that if the safety checks are passed, the location
        returned is 'geom', not the geocoded 'location_name'.
        """
        location = self.geocode(location_name)
        if location is None:
            return None
        location_point = location['point']
        if not location_point:
            return None
        location_point.srid = 4326
        is_close, distance = self.locations_are_close(location_point, geom, max_distance)
        if not is_close:
            return None
        return geom

    def locations_are_close(geom_a, geom_b, max_distance=200):
        """
        Verifies that two locations are within a certain distance from
        each other. Returns a tuple of (is_close, distance), where
        is_close is True only if the locations are within max_distance.
        Assumes max_distance is in meters.
        """
        # Both geometries must be GEOSGeometry for the distance method.
        if not (isinstance(geom_a, GEOSGeometry) and isinstance(geom_b, GEOSGeometry)):
            raise ValueError, 'both geometries must be GEOSGeometry instances'
        carto_srid = 3395 # SRS in meters
        geom_a = self.smart_transform(geom_a, carto_srid)
        geom_b = self.smart_transform(geom_b, carto_srid)
        distance = geom_a.distance(geom_b)
        return (distance < max_distance), distance

    def smart_transform(geom, srid, clone=True):
        """
        Returns a new geometry transformed to the srid given. Assumes if
        the initial geom is lacking an SRS that it is EPSG 4326. (Hence the
        "smartness" of this function.) This fixes many silent bugs when
        transforming between SRSes when the geometry is missing this info.
        """
        if not geom.srs:
            geom.srid = 4326
        return geom.transform(srid, clone=clone)
