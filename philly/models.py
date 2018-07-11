import datetime
import re
import time
import unicodedata
import urllib
from django.contrib.gis.db import models
from django.conf import settings
from django.db import connections, transaction
from django.db.models import Q
from connections import get_connection_name
from constants import (STATUS_CHOICES, STATUS_LIVE, USER_SCHEMAS, NEIGHBOR_MESSAGE_SCHEMAS, UGC, RemovalReasons)

def field_mapping(schema_id_list, db):
    """
    Given a list of schema IDs, returns a dictionary of dictionaries, mapping
    schema_ids to dictionaries mapping the fields' name->real_name.
    Example return value:
        {1: {u'crime_type': 'varchar01', u'crime_date', 'date01'},
         2: {u'permit_number': 'varchar01', 'to_date': 'date01'},
        }
    """
    # schema_fields = [{'schema_id': 1, 'name': u'crime_type', 'real_name': u'varchar01'},
    #                  {'schema_id': 1, 'name': u'crime_date', 'real_name': u'date01'}]
    result = {}
    for sf in SchemaField.objects.using(db).filter(schema__id__in=(schema_id_list)).values('schema', 'name', 'real_name'):
        result.setdefault(sf['schema'], {})[sf['name']] = sf['real_name']
    return result

class SchemaManager(models.Manager):
    def for_metro(self, short_name):
        return self.using(get_connection_name(short_name))

class PublicSchemaManager(SchemaManager):
    def get_queryset(self):
        return super(PublicSchemaManager, self).get_queryset().filter(is_public=True)

BUCKET_CHOICES = (
    (0, 'From the Web'),
    (1, 'Public records'),
    (3, 'Neighbor messages'),
)

class FakeSchema(object):
    def __init__(self, slug, name, plural_name):
        self.slug = slug
        self.name = name
        self.plural_name = plural_name
        self.is_active = True

    def is_neighbor_message(self):
        return True

class Schema(models.Model):
    bucket = models.SmallIntegerField(choices=BUCKET_CHOICES)
    name = models.CharField(max_length=100)
    plural_name = models.CharField(max_length=100)
    indefinite_article = models.CharField(max_length=2) # 'a' or 'an'
    slug = models.CharField(max_length=32, unique=True)
    min_date = models.DateField() # the earliest available NewsItem.pub_date for this Schema
    last_updated = models.DateField()
    date_name = models.CharField(max_length=32) # human-readable name for the NewsItem.item_date field
    date_name_plural = models.CharField(max_length=32)
    is_public = models.BooleanField(db_index=True)
    is_active = models.BooleanField() # Whether this is still updated, displayed in navigation lists, etc.
    has_newsitem_detail = models.BooleanField()
    allow_comments = models.BooleanField()
    has_linkable_locations = models.BooleanField()
    pattern = models.CharField(max_length=32, blank=True)
    launch_date = models.DateField()   # the date that this schema was first made public

    #objects = SchemaManager()
    #public_objects = PublicSchemaManager()

    def __unicode__(self):
        return self.name

    def url(self):
        return '/%s/' % self.slug

    def is_new(self):
        return datetime.date.today() - self.launch_date < datetime.timedelta(days=7)

    def is_neighbor_content(self):
        return self.slug in USER_SCHEMAS

    def is_neighbor_message(self):
        return self.slug in NEIGHBOR_MESSAGE_SCHEMAS

    def pattern_slug(self):
        return 'neighbor-message' if self.slug in NEIGHBOR_MESSAGE_SCHEMAS else self.slug

class SchemaInfo(models.Model):
    schema = models.ForeignKey(Schema)
    short_description = models.TextField()
    summary = models.TextField()
    source = models.TextField()
    grab_bag_headline = models.CharField(max_length=128, blank=True)
    grab_bag = models.TextField(blank=True)
    short_source = models.CharField(max_length=128)
    update_frequency = models.CharField(max_length=64)
    intro = models.TextField()

    def __unicode__(self):
        return unicode(self.schema)

class SchemaField(models.Model):
    schema = models.ForeignKey(Schema)
    name = models.CharField(max_length=32)
    real_name = models.CharField(max_length=10) # 'varchar01', 'varchar02', etc.
    pretty_name = models.CharField(max_length=32) # human-readable name, for presentation
    pretty_name_plural = models.CharField(max_length=32) # plural human-readable name
    pattern_slot = models.CharField(max_length=32) # name used in newsitem_list pattern template
    display = models.BooleanField() # whether to display value on the public site
    is_lookup = models.BooleanField() # whether the value is a foreign key to Lookup
    display_order = models.SmallIntegerField()
    display_format = models.CharField(max_length=50, blank=True)
    display_api = models.BooleanField(default=False) #whether to display value on an api

    def __unicode__(self):
        return u'%s - %s' % (self.schema, self.name)

    def _get_slug(self):
        return self.name.replace('_', '-')
    slug = property(_get_slug)

    def _datatype(self):
        return self.real_name[:-2]
    datatype = property(_datatype)

    def is_type(self, *data_types):
        """
        Returns True if this SchemaField is of *any* of the given data types.
        Allowed values are 'varchar', 'date', 'time', 'datetime', 'bool', 'int'.
        """
        for t in data_types:
            if t == self.real_name[:-2]:
                return True
        return False

    def is_many_to_many_lookup(self):
        """
        Returns True if this SchemaField is a many-to-many lookup.
        """
        return self.is_lookup and not self.is_type('int') and not self.is_type('lookup')

    def smart_pretty_name(self):
        """
        Returns the pretty name for this SchemaField, taking into account
        many-to-many fields.
        """
        if self.is_many_to_many_lookup():
            return self.pretty_name_plural
        return self.pretty_name

class SchemaFieldInfo(models.Model):
    schema = models.ForeignKey(Schema)
    schema_field = models.ForeignKey(SchemaField)
    help_text = models.TextField()

    def __unicode__(self):
        return unicode(self.schema_field)

class LocationType(models.Model):
    name = models.CharField(max_length=255) # e.g., "Ward" or "Congressional District"
    plural_name = models.CharField(max_length=64) # e.g., "Wards"
    scope = models.CharField(max_length=64) # e.g., "Chicago" or "U.S.A."
    slug = models.CharField(max_length=32, unique=True)
    is_significant = models.BooleanField() # whether this is used to display aggregates, etc.

    def __unicode__(self):
        return u'%s, %s' % (self.name, self.scope)

    def url(self):
        return '/locations/%s/' % self.slug

class LocationQuerySet(models.query.GeoQuerySet):
    def alphabetize(self, location_type_slug):
        for i, loc in enumerate(self.filter(location_type__slug=location_type_slug).order_by('name')):
            loc.display_order = i
            loc.save()

class LocationManager(models.GeoManager):
    def get_queryset(self, *args, **kwargs):
        return LocationQuerySet(self.model).filter(is_deleted=False)

    def alphabetize(self, *args, **kwargs):
        return self.get_queryset().alphabetize(*args, **kwargs)

    def largest_overlapping_neighborhood(self, zipcode, using='default'):
        sql = """
        SELECT * FROM (
            SELECT loc.*, ST_Area(ST_Intersection(loc.location, zipcode.location)) AS overlapping_area
            FROM db_location loc
            LEFT JOIN (
                SELECT location
                FROM db_location
                INNER JOIN db_locationtype
                ON db_location.location_type_id = db_locationtype.id
                WHERE db_location.name = %(zipcode)s AND db_locationtype.slug = 'zipcodes'
            ) AS zipcode
            ON 1=1
            INNER JOIN db_locationtype lt
            ON lt.id = loc.location_type_id
            WHERE loc.is_public = true AND ST_Intersects(loc.location, zipcode.location) AND lt.slug='neighborhoods'
        ) as locations
        ORDER BY locations.overlapping_area DESC;
        """
        params = {'zipcode': str(zipcode)}
        qs = self.db_manager(using).raw(sql, params)
        try:
            return qs[0]
        except IndexError:
            return None

class Location(models.Model):
    name = models.CharField(max_length=255) # e.g., "35th Ward"
    normalized_name = models.CharField(max_length=255, db_index=True)
    slug = models.CharField(max_length=64, db_index=True)
    location_type = models.ForeignKey(LocationType)
    location = models.GeometryField(null=True)
    centroid = models.PointField(null=True)
    display_order = models.SmallIntegerField()
    city = models.CharField(max_length=255)
    source = models.CharField(max_length=64)
    area = models.FloatField(blank=True, null=True) # in square meters
    population = models.IntegerField(blank=True, null=True) # from the 2000 Census
    user_id = models.IntegerField(blank=True, null=True)
    is_public = models.BooleanField()
    description = models.TextField(blank=True)
    creation_date = models.DateTimeField(blank=True, null=True)
    last_mod_date = models.DateTimeField(blank=True, null=True)
    is_deleted = models.BooleanField(default=False)
    is_indexed = models.BooleanField(default=False)
    newsitem_density = models.IntegerField(blank=True, null=True)
    follower_count = models.IntegerField(blank=True, null=True)
    active_users = models.IntegerField(blank=True, null=True)
    objects = LocationManager()
    all_locations = models.GeoManager()

    class Meta:
        unique_together = (('slug', 'location_type'),)

    def __unicode__(self):
        return self.name

    def url(self):
        return '/locations/%s/%s/' % (self.location_type.slug, self.slug)

    def url_with_domain(self):
        return 'https://%s.everyblock.com%s' % (settings.SHORT_NAME, self.url())

    def edit_url(self):
        # This is only used for custom locations.
        return '/accounts/custom-locations/edit/%s/' % self.slug

    def delete_url(self):
        # This is only used for custom locations.
        return '/accounts/custom-locations/delete/%s/' % self.slug

    def _is_custom(self):
        return self.location_type.slug == 'custom'
    is_custom = property(_is_custom)

    def _get_user(self):
        if self.user_id is None:
            return None
        if not hasattr(self, '_user_cache'):
            from everyblock.accounts.models import User
            try:
                self._user_cache = User.objects.get(id=self.user_id)
            except User.DoesNotExist:
                self._user_cache = None
        return self._user_cache
    user = property(_get_user)

    # Stuff for the place interface (see utils/pids.py).
    place_type = 'location'
    place_preposition = 'in'

    def _pretty_name(self):
        return self.name
    pretty_name = property(_pretty_name)

    def _place_type_name(self):
        if self.location_type.slug == 'custom':
            return 'area'
        else:
            return self.location_type.name
    place_type_name = property(_place_type_name)

    def _pid(self):
        return 'l:%s' % self.id
    pid = property(_pid)

    def _geom(self):
        return self.location
    geom = property(_geom)
    search_geom = property(_geom)

    def _allows_messages(self):
        if self.location_type.slug == 'custom' and not self.is_public:
            return False
        if self.location_type.slug in ('cities', 'quadrants'):
            return False
        return True
    allows_messages = property(_allows_messages)

    def _is_unknown(self):
        return self.slug == 'unknown'
    is_unknown = property(_is_unknown)

    def alert_url(self):
        return '%salerts/' % self.url()

    def rss_url(self):
        return '/rss%s' % self.url()

class AttributesDescriptor(object):
    """
    This class provides the functionality that makes the attributes available
    as `attributes` on a model instance.
    """
    def __get__(self, instance, instance_type=None):
        if instance is None:
            raise AttributeError("%s must be accessed via instance" % self.__class__.__name__)
        if not hasattr(instance, '_attributes_cache'):
            instance._attributes_cache = AttributeDict(instance)
        return instance._attributes_cache

    def __set__(self, instance, value):
        if instance is None:
            raise AttributeError("%s must be accessed via instance" % self.__class__.__name__)
        if not isinstance(value, dict):
            raise ValueError('Only a dictionary is allowed')
        db = instance._state.db
        mapping = field_mapping([instance.schema_id], db)[instance.schema_id]
        for k, v in mapping.items():
            if v.startswith('lookup'):
                mapping[k] += '_id'
        mapping = mapping.items()
        values = [value.get(k, None) for k, v in mapping]
        db = instance._state.db
        conn = connections[db]
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE %s
            SET %s
            WHERE news_item_id = %%s
            """ % (Attribute._meta.db_table, ','.join(['%s=%%s' % v for k, v in mapping])),
                values + [instance.id])
        # If no records were updated, that means the DB doesn't yet have a
        # row in the attributes table for this news item. Do an INSERT.
        if cursor.rowcount < 1:
            cursor.execute("""
                INSERT INTO %s (news_item_id, schema_id, %s)
                VALUES (%%s, %%s, %s)""" % (Attribute._meta.db_table, ','.join([v for k, v in mapping]), ','.join(['%s' for k in mapping])),
                [instance.id, instance.schema_id] + values)
        transaction.commit_unless_managed(using=db)

class AttributeDict(dict):
    """
    A dictionary-like object that serves as a wrapper around attributes for a
    given NewsItem.
    """
    def __init__(self, ni):
        dict.__init__(self)
        self.ni = ni
        self.news_item_id = ni.id
        self.cached = False
        self.fields = {}
        for schemafield in SchemaField.objects.using(self.ni._state.db).filter(schema=ni.schema_id):
            self.fields[schemafield.name] = { "real_name": schemafield.real_name, "is_lookup": schemafield.is_lookup }

    def __do_query(self):
        if not self.cached:
            db = self.ni._state.db
            attributes_to_fetch = getattr(self.ni, 'restricted_attributes', None) or self.fields.keys()
            try:
                att_objs = self.ni.attribute_set.all()
                atts = {}
                for obj in att_objs:
                    for att in attributes_to_fetch:
                        real_name = self.fields[att]['real_name']
                        if str(att) == "source":
                            src_id = getattr(obj, real_name+"_id", None)
                            if not src_id:
                                src_id = getattr(obj, real_name, None)
                            try:
                                lookup_obj = Lookup.objects.using(self.ni._state.db).get(id=src_id)
                                atts["source_name"] = lookup_obj.name
                            except Lookup.DoesNotExist:
                                logger.warn("?? lookup doesn't exist: ")
                                pass

                        if self.fields[att]['is_lookup'] and real_name.startswith('lookup'):
                            atts[att] = getattr(obj, real_name+"_id", None)
                        else:
                            atts[att] = getattr(obj, real_name, None)
            except IndexError:
                return # Attributes for this newsitem haven't been saved yet. Just return.
            self.update(atts)
            self.cached = True

    def get(self, *args, **kwargs):
        self.__do_query()
        return dict.get(self, *args, **kwargs)

    def __len__(self):
        self.__do_query()
        return dict.__len__(self)

    def __repr__(self):
        self.__do_query()
        return dict.__repr__(self)

    def __getitem__(self, name):
        self.__do_query()
        return dict.__getitem__(self, name)

    def __setitem__(self, name, value):
        db = self.ni._state.db
        real_name = self.fields[name]['real_name']
        rows_updated = self.ni.attribute_set.all().update(**{real_name:value})
        # If no records were updated, that means the DB doesn't yet have a
        # row in the attributes table for this news item. Do an INSERT.
        if rows_updated < 1:
            attr = Attribute(news_item_id=self.news_item_id, schema_id=self.ni.schema_id, **{real_name:value})
            attr.save(using=db)
        transaction.commit_unless_managed(using=db)
        dict.__setitem__(self, name, value)

def load_newsitem_attributes(newsitem_list, using='default', columns_to_select=[]):
    "Edits newsitem_list in place, adding `attribute_values` and `attribute_slots` attributes."
    # fmap = {schema_id: {'fields': [(name, real_name, pattern_slot)], 'lookups': [real_name1, real_name2]}}
    fmap = {}
    attribute_columns_to_select = set(['news_item'])
    schema_ids = set([ni.schema_id for ni in newsitem_list])
    schema_field_qs = SchemaField.objects.using(using).filter(schema__id__in=schema_ids)
    if columns_to_select:
        schema_field_qs = schema_field_qs.filter(name__in=columns_to_select)

    for sf in schema_field_qs.values('schema', 'name', 'real_name', 'pattern_slot', 'is_lookup', 'display'):
        fmap.setdefault(sf['schema'], {'fields': [], 'lookups': []})
        fmap[sf['schema']]['fields'].append((sf['name'], sf['real_name'], sf['pattern_slot']))
        if sf['is_lookup'] or sf['real_name'].startswith('lookup'):
            fmap[sf['schema']]['lookups'].append(sf['real_name'])
        attribute_columns_to_select.add(str(sf['real_name']))

    att_dict = dict([(i['news_item'], i) for i in Attribute.objects.using(using).filter(news_item__id__in=[ni.id for ni in newsitem_list]).values(*list(attribute_columns_to_select))])

    # Determine which Lookup objects need to be retrieved.
    lookup_ids = set()
    for ni in newsitem_list:
        for real_name in fmap.get(ni.schema_id, {}).get('lookups', []):
            value = att_dict.get(ni.id, {}).get(real_name)
            if not value: continue
            elif ',' in str(value):
                lookup_ids.update(value.split(','))
            else:
                lookup_ids.add(value)

    # Retrieve only the Lookups that are referenced in newsitem_list.
    lookup_ids = [i for i in lookup_ids if i]
    if lookup_ids:
        lookup_objs = Lookup.objects.using(using).in_bulk(lookup_ids)
    else:
        lookup_objs = {}

    # Set 'attribute_values' for each NewsItem in newsitem_list.
    for ni in newsitem_list:
        att = att_dict.get(ni.id, {})
        att_values = {}
        att_slots = {}
        for field_name, real_name, pattern_slot in fmap.get(ni.schema_id, {}).get('fields', []):
            value = att.get(real_name)
            if real_name in fmap.get(ni.schema_id, {}).get('lookups', {}):
                if real_name.startswith('int') or real_name.startswith('lookup'):
                    # value might be None, in which case it wouldn't be in lookup_objs,
                    # so use get() to fallback to None in that case.
                    value = lookup_objs.get(value)
                else: # Many-to-many lookups are comma-separated strings.
                    value = [lookup_objs[int(i)] for i in value.split(',') if i]
            att_slots[pattern_slot] = att_values[field_name] = value
        ni.attribute_values = att_values
        ni.attribute_slots = att_slots

class NewsItemQuerySet(models.query.GeoQuerySet):

    def restrict_attributes(self, attributes):
        clone = self._clone()
        clone.restricted_attributes = attributes
        return clone

    def iterator(self, *args, **kwargs):
        for obj in super(NewsItemQuerySet, self).iterator(*args, **kwargs):
            obj.restricted_attributes = getattr(self, 'restricted_attributes', None)
            yield obj

    def _clone(self, *args, **kwargs):
        obj = super(NewsItemQuerySet, self)._clone(*args, **kwargs)
        obj.restricted_attributes = getattr(self, 'restricted_attributes', None)
        return obj

    def delete(self, *args, **kwargs):
        raise NotImplementedError('NewsItem QuerySets cannot be deleted. Delete instances individually.')

    def update(self, _force=False, *args, **kwargs):
        if not _force:
            raise NotImplementedError('NewsItem QuerySets cannot be updated. Update instances individually.')
        super(NewsItemQuerySet, self).update(*args, **kwargs)

    def prepare_attribute_qs(self, schema_id=None):
        clone = self._clone()
        if 'db_attribute' not in clone.query.extra_tables:
            clone.query.extra_tables += ('db_attribute',)
        clone = clone.extra(where=['db_newsitem.id = db_attribute.news_item_id'])

        # schema_id is an optimization. We've found that adding the
        # db_attribute.schema_id check to the WHERE clause can vastly improve
        # the speed of the query. It probably gives some crucial clues to the
        # PostgreSQL query planner.
        if schema_id is not None:
            clone = clone.extra(where=['db_attribute.schema_id = %s' % schema_id])

        return clone

    def with_attributes(self, columns_to_select=[]):
        """
        Returns a list of NewsItems, each of which has an `attribute_values`
        attribute. `attribute_values` is a dictionary mapping attribute names
        to values. If an attribute is a Lookup, the value will be the Lookup
        object.
        """
        newsitem_list = list(self)
        load_newsitem_attributes(newsitem_list, self._db, columns_to_select=columns_to_select)
        return newsitem_list

    def top_news(self, *args, **kwargs):
        """
        Returns a QuerySet of NewsItems ordered by their blockscore.
        """
        from everyblock.messages.constants import LIVE_BAD, REMOVED_BY_STAFF, REMOVED_BY_USER
        max_date = datetime.datetime.now()
        # Exclude specific types of items from citywide top news.
        qs = self.filter(schema__has_newsitem_detail=True).exclude(status__in=(LIVE_BAD, REMOVED_BY_STAFF, REMOVED_BY_USER))
        return qs.filter(blockscore__isnull=False, pub_date__lte=max_date).order_by('-blockscore')

    def all_user_news(self, *args, **kwargs):
        """
        Returns a QuerySet of all neighbor content
        """
        from everyblock.messages.constants import LIVE_BAD, REMOVED_BY_STAFF, REMOVED_BY_USER
        max_date = datetime.datetime.now()
        # Exclude specific types of items from citywide top news.
        qs = self.filter(schema__slug__in=UGC).exclude(status__in=(LIVE_BAD, REMOVED_BY_STAFF, REMOVED_BY_USER))
        return qs.filter(pub_date__lte=max_date).order_by('-pub_date')

    def by_attribute(self, schema_field, att_value, is_lookup=False):
        """
        Returns a QuerySet of NewsItems whose attribute value for the given
        SchemaField is att_value. If att_value is a list, this will do the
        equivalent of an "OR" search, returning all NewsItems that have an
        attribute value in the att_value list.
        This handles many-to-many lookups correctly behind the scenes.
        If is_lookup is True, then att_value is treated as the 'code' of a
        Lookup object, and the Lookup's ID will be retrieved for use in the
        query.
        """
        clone = self.prepare_attribute_qs(schema_field.schema_id)
        real_name = str(schema_field.real_name)
        if real_name.startswith('lookup'): real_name += '_id'
        if not isinstance(att_value, (list, tuple)):
            att_value = [att_value]
        if is_lookup:
            att_value = Lookup.objects.filter(schema_field__id=schema_field.id, code__in=att_value)
            if not att_value:
                # If the lookup values don't exist, then there aren't any
                # NewsItems with this attribute value. Note that we aren't
                # using QuerySet.none() here, because we want the result to
                # be a NewsItemQuerySet, and none() returns a normal QuerySet.
                clone = clone.extra(where=['1=0'])
                return clone
            att_value = [val.id for val in att_value]
        if schema_field.is_many_to_many_lookup():
            # We have to use a regular expression search to look for all rows
            # with the given att_value *somewhere* in the column. The [[:<:]]
            # thing is a word boundary.
            for value in att_value:
                if not str(value).isdigit():
                    raise ValueError('Only integer strings allowed for att_value in many-to-many SchemaFields')
            clone = clone.extra(where=["db_attribute.%s ~ '[[:<:]]%s[[:>:]]'" % (real_name, '|'.join([str(val) for val in att_value]))])
        elif None in att_value:
            if att_value != [None]:
                raise ValueError('by_attribute() att_value list cannot have more than one element if it includes None')
            clone = clone.extra(where=["db_attribute.%s IS NULL" % real_name])
        else:
            clone = clone.extra(where=["db_attribute.%s IN (%s)" % (real_name, ','.join(['%s' for val in att_value]))], params=tuple(att_value))
        return clone

    def by_place(self, place, block_radius=BLOCK_RADIUS_DEFAULT):
        """
        Returns a QuerySet of NewsItems filtered to the given place
        (either a Block, Location or Spot).
        """
        if place.place_type == 'location':
            if place.location is not None:
                return self.by_location(place)
            else:
                return self.filter(location__isnull=True)
        elif place.place_type == 'block':
            search_buf = make_search_buffer(place.location.centroid, block_radius)
            return self.filter(location__intersects=search_buf)
        elif place.place_type == 'spot':
            return self.filter(spot__id=place.id)
        else:
            raise ValueError('Got unknown place type %r' % place.place_type)

    def by_place_and_date(self, place, start_datetime, end_datetime, block_radius=BLOCK_RADIUS_DEFAULT):
        """
        Returns a QuerySet filtered by the given place and pub_date range.
        (Uses Redis for Locations.)
        """
        return self.by_place(place, block_radius).filter(pub_date__range=(start_datetime, end_datetime))

    def by_block(self, block, radius=3):
        block_buffer = make_search_buffer(block.location.centroid, radius)
        return self.filter(location__intersects=block_buffer)

    def by_location(self, location):
        sql = """
        (location_id=%s OR (location_id IS NULL AND ST_Intersects(location, (SELECT location FROM db_location WHERE id=%s))))
        """
        return self.extra(where=[sql], params=(location.id, location.id))

    def by_neighborhood(self, neighborhood):
        locations = Q(location_id=neighborhood.id)
        blocks = Q(location_id__isnull=True, location__intersects=neighborhood.location)
        return self.filter(locations | blocks)

    def by_user(self, user, email=False):
        sql = []
        field_name = 'send_email' if email else 'show_on_dashboard'
        # Directly followed locations
        sql.append("(location_id IN (SELECT location_id FROM savedplaces_savedplace WHERE user_id=%%s AND %s='t'))" % field_name)

        # Blocks, spots, and points within followed places
        sql.append("(location_id IS NULL AND ST_Intersects(location, (SELECT ST_Union(geometry) FROM savedplaces_savedplace WHERE user_id=%%s AND %s='t')))" % field_name)

        # Locations that contain followed blocks
        # TODO: Filter out custom locations?
        sql.append("""(location_id IN (
            SELECT id FROM db_location WHERE EXISTS (
                SELECT 1 FROM savedplaces_savedplace
                WHERE %s='t' AND block_id IS NOT NULL AND user_id=%%s AND ST_Contains(location, ST_Centroid(geometry))
            )
        ))""" % field_name)

        # Directly followed spots
        sql.append("(spot_id IN (SELECT spot_id FROM savedplaces_savedplace WHERE user_id=%%s AND spot_id IS NOT NULL AND %s='t'))" % field_name)

        muted_sql = "db_newsitem.id NOT IN (SELECT newsitem_id FROM preferences_mutednewsitem WHERE user_id=%s)"

        sql = '(%s)' % " OR ".join(sql)
        return self.extra(where=[sql, muted_sql], params=(user.id, user.id, user.id, user.id, user.id))

    # Schemas #################################################################

    def user_messages(self):
        return self.filter(schema__slug__in=USER_SCHEMAS)

    def neighbor_messages(self):
        return self.filter(schema__slug__in=NEIGHBOR_MESSAGE_SCHEMAS)

    def neighbor_events(self):
        return self.filter(schema__slug='neighbor-events')

    def neighbor_ads(self):
        return self.filter(schema__slug='neighbor-ads')

    def web_posts(self):
        return self.filter(schema__bucket=0)

    def web_events(self):
        return self.filter(schema__slug='events')

    def public_records(self):
        return self.filter(schema__bucket=1)

    # Visibility ##############################################################

    def live(self):
        status_live = Q(status__isnull=True) | Q(status__in=STATUS_LIVE)
        return self.filter(status_live, is_public=True)

    def pending(self):
        from everyblock.messages.constants import STATUS_PENDING
        return self.filter(status__in=STATUS_PENDING)

    def removed(self):
        from everyblock.messages.constants import STATUS_REMOVED
        return self.filter(Q(is_public=False) | Q(status__in=STATUS_REMOVED))

class NewsItemManager(models.GeoManager):
    def get_queryset(self):
        return NewsItemQuerySet(self.model)

    def for_metro(self, short_name):
        return self.using(get_connection_name(short_name))

    def filter_attributes(self, **kwargs):
        return self.get_queryset().filter_attributes(**kwargs)

    def exclude_attributes(self, **kwargs):
        return self.get_queryset().exclude_attributes(**kwargs)

    def by_attribute(self, *args, **kwargs):
        return self.get_queryset().by_attribute(*args, **kwargs)

    def by_place(self, *args, **kwargs):
        return self.get_queryset().by_place(*args, **kwargs)

    def by_place_and_date(self, *args, **kwargs):
        return self.get_queryset().by_place_and_date(*args, **kwargs)

    def by_user(self, *args, **kwargs):
        return self.get_queryset().by_user(*args, **kwargs)

    def with_attributes(self, *args, **kwargs):
        return self.get_queryset().with_attributes(*args, **kwargs)

    def top_news(self, *args, **kwargs):
        return self.get_queryset().top_news(*args, **kwargs)

    def top_nearby_newsitem_ids(self, user, limit=3, min_days_old=3, short_name=None):
        """
        Returns the top N neighbor messages posted to neighborhoods that
        intersect the given user's followed places.
        """
        if short_name is None:
            short_name = settings.SHORT_NAME
        today = datetime.date.today()
        if settings.DEBUG:
            min_days_old = 30
        min_date = today - datetime.timedelta(days=min_days_old)
        min_blockscore = int(min_date.strftime('%Y%m%d0000'))
        conn = connections[short_name]
        cursor = conn.cursor()
        params = {
            'user_id': user.id,
            'limit': limit,
            'blockscore': min_blockscore,
            'pub_date': min_date,
            'status': STATUS_LIVE,
            # The following numbers are completely made up and should be refactored into something more clear and deliberate someday.
            'radius': 0.001 if settings.SHORT_NAME == 'chicago' else .02,
            'schemas': tuple(USER_SCHEMAS),
        }
        cursor.execute("""
            SELECT ni.id
            FROM db_newsitem ni
            WHERE ni.schema_id IN (SELECT id FROM db_schema WHERE slug IN %(schemas)s)
            AND ni.is_public = true
            AND ni.id NOT IN (SELECT newsitem_id FROM preferences_mutednewsitem WHERE user_id=%(user_id)s)
            AND blockscore > %(blockscore)s
            AND pub_date >= %(pub_date)s
            AND ni.location_id IS NOT NULL
            AND (ni.status IN %(status)s OR ni.status IS NULL)
            AND ni.location_id IN (
                SELECT l.id
                FROM db_location l
                WHERE ST_Intersects(l.location, (
                    SELECT ST_Buffer(ST_Union(sp.geometry), %(radius)s)
                    FROM savedplaces_savedplace sp
                    WHERE user_id=%(user_id)s))
                AND l.id NOT IN (
                    SELECT location_id
                    FROM savedplaces_savedplace
                    WHERE location_id IS NOT NULL
                    AND user_id=%(user_id)s)
                AND l.id NOT IN (
                    SELECT id FROM db_location WHERE EXISTS (
                        SELECT 1 FROM savedplaces_savedplace
                        WHERE block_id IS NOT NULL AND user_id=%(user_id)s AND ST_Contains(location, ST_Centroid(geometry))
                    )
                ))
            ORDER BY blockscore DESC
            LIMIT %(limit)s;
            """, params)
        result = (r[0] for r in cursor.fetchall())
        cursor.close()
        conn.close()
        return result

    @transaction.commit_on_success
    def create_with_attributes(self, *args, **kwargs):
        """
        Create and return a NewsItem with Attributes in a transaction.
        """
        using = kwargs.pop('using', None)
        attributes = kwargs.pop('attributes')
        if using is None:
            ni = super(NewsItemManager, self).create(*args, **kwargs)
        else:
            ni = self.get_queryset().using(using).create(*args, **kwargs)
        ni.attributes = attributes
        return ni


class NewsItem(models.Model):
    schema = models.ForeignKey(Schema)
    title = models.CharField(max_length=255)
    description = models.TextField()
    url = models.TextField(blank=True)
    pub_date = models.DateTimeField(db_index=True)
    item_date = models.DateField(db_index=True)
    last_update = models.DateTimeField(db_index=True)
    location = models.GeometryField(blank=True, null=True)
    location_name = models.CharField(max_length=255)
    is_public = models.NullBooleanField(default=True)
    status = models.IntegerField(choices=STATUS_CHOICES, blank=True, null=True) # TODO: Merge is_public into this field.
    was_reported = models.NullBooleanField()
    allow_comments = models.BooleanField(default=True) # Lets us turn off comments on a per-newsitem basis.
    user_id = models.IntegerField(blank=True, null=True, db_index=True)
    blockscore = models.BigIntegerField(blank=True, null=True, db_index=True) # null=True because of legacy records.
    reason = models.IntegerField(choices=RemovalReasons.REASONS, null=True) # Removal reason
    place_type = models.CharField(max_length=25, choices=places.PLACE_TYPE_CHOICES, null=True)
    place_id = models.IntegerField(null=True)
    source_name = models.CharField(null=True, max_length=100) # Not fkey because some newsitems don't have source info
    source_id = models.IntegerField(null=True) # Not fkey because some newsitems don't have source info
    ip_address = models.CharField(max_length=20, blank=True, null=True)

    # TODO: place_type and place_id should eventually replace these fields.
    spot = models.ForeignKey(Spot, blank=True, null=True)
    location_id = models.IntegerField(blank=True, null=True) # Using a real FK sometimes causes unwanted joins.

    # Denormalized fields
    is_first_post = models.NullBooleanField(default=False)
    comment_count = models.IntegerField(default=0, db_index=True)
    thank_count = models.IntegerField(blank=True, null=True)
    poster_name = models.CharField(max_length=255)
    poster_description = models.CharField(max_length=50)
    poster_image_name = models.CharField(max_length=100)
    poster_badge = models.CharField(max_length=100)
    poster_status = models.IntegerField()
    is_watched_thread = models.BooleanField(default=False)

    objects = NewsItemManager()
    attributes = AttributesDescriptor()

    def __unicode__(self):
        return self.title

    def save(self, *args, **kwargs):
        if self.location is not None and not self.location.valid:
            raise Exception('Invalid geometry: %s' % self.location.wkt)
        if self.last_update is None:
            self.last_update = self.pub_date
        self.blockscore = self.calculate_blockscore()
        super(NewsItem, self).save(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self.is_public = False
        self.save()

    def restore(self, *args, **kwargs):
        self.is_public = True
        self.save()

    @property
    def location_tuple(self):
        location_coordinates = []
        coords = []
        if not self.location:
            coords.append({'longitude': "", 'latitude': ""})
        if self.location.geom_type.upper() == 'POLYGON' or self.location.geom_type.upper() == 'MULTIPOLYGON':
            coords.append(self.location.centroid.coords)
        elif self.location.geom_type.upper() == 'MULTIPOINT':
            coords = list(self.location.coords)
        else:
            coords.append(self.location.coords)

        for c in coords:
            location_coordinates.append({'longitude': c[0], 'latitude': c[1]})
        return location_coordinates
       
    @property
    def last_comment_user(self):
        from everyblock.comments.models import Comment
        comments = list(Comment.objects.using(self._state.db).filter(newsitem_id=self.id).order_by('-pub_date'))
        if len(comments) > 0:
            return comments[0].user.public_name
        else:
            return ""

    @property
    def last_comment_date(self):
        from everyblock.comments.models import Comment
        comments = list(Comment.objects.using(self._state.db).filter(newsitem_id=self.id).order_by('-pub_date'))
        if len(comments ) > 0:
            return comments[0].pub_date
        else:
            return ""

    @property
    def cmnt_cnt(self):
        from everyblock.comments.models import Comment
        comments = list(Comment.objects.using(self._state.db).filter(newsitem_id=self.id))
        return len(comments)

    @property
    def timestamp(self):
        return time.mktime(self.pub_date.timetuple())

    def _get_metro(self):
        if not hasattr(self, '_metro'):
            self._metro = self._state.db
            if self._metro in ('default', 'standby'):
                self._metro = settings.SHORT_NAME
        return self._metro

    def _set_metro(self, short_name):
        self._metro = short_name

    metro = property(_get_metro, _set_metro)

    @property
    def place(self):
        if not (self.place_type and self.place_id):
            return None
        return places.get_place(self.place_type, self.place_id, short_name=self.metro)

    @property
    def poster(self):
        if not hasattr(self, '_poster'):
            self._poster = Poster.from_newsitem(self)
        return self._poster

    def calculate_blockscore(self, comment_max=None, day_multiplier=None):
        d = self.pub_date
        date_score = (d.year * 100000000) + (d.month * 1000000) + (d.day * 10000) + (d.hour * 100) + d.minute

        # Number of comments that signifies "a fantastic comment thread,
        # and any more comments shouldn't give this thread a higher score."
        if comment_max is None:
            comment_max = get_metro(self.metro, only_public=False)['blockscore_comment_max']

        # Calculate an activity score based on three factors:
        #     * number of comments
        #     * whether it's a neighbor message (and a good/bad/normal one)
        #     * whether it's a media mention
        # These three factors are weighted differently, with number of
        # comments having the biggest weight.
        comment_score = float(min(self.comment_count, comment_max)) / comment_max # between 0 and 1.
        if self.is_public and self.schema.is_neighbor_content():
            message_score = .7
            from everyblock.messages import constants
            status = self.status
            if status == constants.LIVE_GOOD:
                message_score = 1
            elif status == constants.LIVE_BAD:
                message_score = 0
        else:
            message_score = 0
        media_mention_score = self.schema.slug == 'news-articles' and .7 or 0

        # These should add up to 1000.
        COMMENT_WEIGHT = 600
        MESSAGE_WEIGHT = 300
        MEDIA_MENTION_WEIGHT = 100

        activity_score = (comment_score * COMMENT_WEIGHT) + (message_score * MESSAGE_WEIGHT) + (media_mention_score * MEDIA_MENTION_WEIGHT)

        # day_multiplier lets us weigh recency vs. message_score. Use a
        # higher day_multiplier to allow older items to stay at the top
        # of the list for longer. Use a lower value to favor recency.
        if day_multiplier is None:
            day_multiplier = get_metro(self.metro, only_public=False)['blockscore_day_multiplier']

        return date_score + int(activity_score * day_multiplier)

    # new_url and new_url_with_domain are for v2, item_url and item_url_with_domain for v1.
    def new_url(self):
        value = unicodedata.normalize('NFKD', self.title).encode('ascii', 'ignore')
        value = re.sub(r'\'s', 's', value)
        words = re.findall(r'(\w+)', value)
        stopwords = [
            'I', 'a', 'about', 'an', 'are', 'as', 'at', 'be', 'by', 'com',
            'for', 'from', 'how', 'in', 'is', 'it', 'of', 'on', 'or', 'that',
            'the', 'this', 'to', 'was', 'what', 'when', 'where', 'who',
            'will', 'with', 'the', 'www'
        ]
        slug_words = [w.lower() for w in words if w not in stopwords][:8]
        date = self.pub_date if self.schema.slug in (NEIGHBOR_MESSAGE_SCHEMAS + ['neighbor-ads']) else self.item_date
        date_segment = date.strftime('%b%d').lower()
        slug = '-'.join(slug_words)
        return '/%s/%s-%s-%s/' % (self.schema.slug, date_segment, slug, self.id)

    def new_url_with_domain(self):
        return 'https://%s.everyblock.com%s' % (self.metro, self.new_url())

    def item_url(self):
        return '/%s/by-date/%s/%s/%s/%s/' % (self.schema.slug, self.item_date.year, self.item_date.month, self.item_date.day, self.id)

    def item_url_with_domain(self):
        return 'https://%s.everyblock.com%s' % (self.metro, self.item_url())

    def item_date_url(self):
        return '/%s/by-date/%s/%s/%s/' % (self.schema.slug, self.item_date.year, self.item_date.month, self.item_date.day)

    def place_url(self):
        # TODO: Would be nice to be smarter here, perhaps determining the place
        # type and determining the direct URL, instead of relying on search.
        # Also take into account whether the NewsItem is associated with a
        # private custom location and maybe return None in that case?
        if self.schema.has_linkable_locations and self.location_name.lower() != 'multiple locations':
            try:
                return '/search/?q=%s&type=place' % urllib.quote_plus(self.location_name)
            except KeyError:
                pass # In case location_name has non-ASCII text in it. We've seen u'\x92' for example.
        return ''

    def attributes_for_template(self):
        """
        Return a list of AttributeForTemplate objects for this NewsItem. The
        objects are ordered by SchemaField.display_order.
        """
        fields = SchemaField.objects.filter(schema__id=self.schema_id).select_related().order_by('display_order')
        field_infos = dict([(obj.schema_field_id, obj.help_text) for obj in SchemaFieldInfo.objects.filter(schema__id=self.schema_id)])
        try:
            attribute_row = Attribute.objects.using(self._state.db).filter(news_item__id=self.id).values(*[f.real_name for f in fields])[0]
        except KeyError:
            return []
        return [AttributeForTemplate(f, attribute_row, field_infos.get(f.id, None)) for f in fields]

    def load_attributes(self):
        load_newsitem_attributes([self], self._state.db)

    def _set_photos(self, photo_list):
        self._photos = photo_list

    def _get_photos(self):
        from everyblock.photos.models import Photo
        if not hasattr(self, '_photos'):
            self._photos = list(Photo.objects.using(self._state.db).filter(object_type=Photo.NEWSITEM, object_id=self.id, status=Photo.LIVE))
        return self._photos

    photos = property(_get_photos, _set_photos)

    def _set_embeds(self, embed_list):
        self._embeds = embed_list

    def get_embeds(self):
        return Embed.objects.using(self._state.db).filter(newsitem=self.id)
    embeds = property(get_embeds, _set_embeds)

    def reason_description(self):
        reason = RemovalReasons.DETAILS.get(self.reason, None)
        description = 'This neighbor message has been removed by EveryBlock staff.'
        if reason:
            description = reason.get('short_message', description).format('neighbor message')
        return description

    def reason_slug(self):
        reason = RemovalReasons.DETAILS.get(self.reason, None)
        if reason:
            description = reason.get('slug', '')
        else:
            description = ''
        return description


class Embed(models.Model):
    # Newsitems attached to this embed
    newsitem = models.ManyToManyField(NewsItem)
    url = models.URLField(max_length=2084, unique=True)
    url_type = models.CharField(max_length=50)
    # Full JSON response from embedly
    response = models.TextField()
    # Only iframe html (if available) for the embed
    embed_html = models.TextField()
    provider_url = models.URLField()
    description = models.TextField()
    title = models.CharField(max_length=255)
    author_name = models.CharField(max_length=500)
    provider_name = models.CharField(max_length=500)
    thumbnail_url = models.URLField()

    def __unicode__(self):
        return "Embed for url: {}".format(self.url)


class PromotedNewsItem(models.Model):
    """
    A staff picked and edited newsitem for use on the local slice and other
    places we distribute our news.
    """
    newsitem = models.OneToOneField(NewsItem)
    headline = models.CharField(max_length=255)
    excerpt = models.TextField(blank=True)
    image_url = models.URLField(max_length=255, blank=True)

    def __unicode__(self):
        return self.headline

class AttributeForTemplate(object):
    def __init__(self, schema_field, attribute_row, help_text):
        self.sf = schema_field
        self.raw_value = attribute_row[schema_field.real_name]
        self.schema_slug = schema_field.schema.slug
        self.is_lookup = schema_field.is_lookup
        self.help_text = help_text
        if self.sf.display_format:
            self.formatted_value = formatting.apply_format(self.raw_value, self.sf.display_format)
        else:
            self.formatted_value = None
        if self.is_lookup:
            if self.raw_value == '' or not self.raw_value:
                self.values = []
            elif self.sf.is_many_to_many_lookup():
                try:
                    id_values = map(int, self.raw_value.split(','))
                except ValueError:
                    self.values = []
                else:
                    lookups = Lookup.objects.in_bulk(id_values)
                    self.values = [lookups[i] for i in id_values]
            else:
                self.values = [Lookup.objects.get(id=self.raw_value)]
        else:
            self.values = [self.raw_value]

    def value_list(self):
        """
        Returns a list of {value, description} dictionaries representing each value for
        this attribute.
        """
        from django.utils.dateformat import format, time_format
        descriptions = [None]
        if self.formatted_value is not None:
            values = [self.formatted_value]
        elif self.is_lookup:
            values = [val and val.name or 'None' for val in self.values]
            descriptions = [val and val.description or None for val in self.values]
        elif isinstance(self.raw_value, datetime.datetime):
            values = [format(self.raw_value, 'F j, Y, P')]
        elif isinstance(self.raw_value, datetime.date):
            values = [format(self.raw_value, 'F j, Y')]
        elif isinstance(self.raw_value, datetime.time):
            values = [time_format(self.raw_value, 'P')]
        elif self.raw_value is True:
            values = ['Yes']
        elif self.raw_value is False:
            values = ['No']
        elif self.raw_value is None:
            values = ['N/A']
        else:
            values = [self.raw_value]
        return [{'value': value, 'description': description} for value, description in zip(values, descriptions)]

class Attribute(models.Model):
    news_item = models.ForeignKey(NewsItem, primary_key=True, unique=True)
    schema = models.ForeignKey(Schema)
    # All data-type field names must end in two digits, because the code assumes this.
    varchar01 = models.CharField(max_length=255, blank=True, null=True)
    varchar02 = models.CharField(max_length=255, blank=True, null=True)
    varchar03 = models.CharField(max_length=255, blank=True, null=True)
    varchar04 = models.CharField(max_length=255, blank=True, null=True)
    varchar05 = models.CharField(max_length=255, blank=True, null=True)
    date01 = models.DateField(blank=True, null=True)
    date02 = models.DateField(blank=True, null=True)
    date03 = models.DateField(blank=True, null=True)
    date04 = models.DateField(blank=True, null=True)
    date05 = models.DateField(blank=True, null=True)
    time01 = models.TimeField(blank=True, null=True)
    time02 = models.TimeField(blank=True, null=True)
    datetime01 = models.DateTimeField(blank=True, null=True)
    datetime02 = models.DateTimeField(blank=True, null=True)
    datetime03 = models.DateTimeField(blank=True, null=True)
    datetime04 = models.DateTimeField(blank=True, null=True)
    bool01 = models.NullBooleanField(blank=True)
    bool02 = models.NullBooleanField(blank=True)
    bool03 = models.NullBooleanField(blank=True)
    bool04 = models.NullBooleanField(blank=True)
    bool05 = models.NullBooleanField(blank=True)
    int01 = models.IntegerField(blank=True, null=True)
    int02 = models.IntegerField(blank=True, null=True)
    int03 = models.IntegerField(blank=True, null=True)
    int04 = models.IntegerField(blank=True, null=True)
    int05 = models.IntegerField(blank=True, null=True)
    int06 = models.IntegerField(blank=True, null=True)
    int07 = models.IntegerField(blank=True, null=True)
    lookup01 = models.ForeignKey("Lookup", blank=True, null=True, related_name='+')
    lookup02 = models.ForeignKey("Lookup", blank=True, null=True, related_name='+')
    lookup03 = models.ForeignKey("Lookup", blank=True, null=True, related_name='+')
    lookup04 = models.ForeignKey("Lookup", blank=True, null=True, related_name='+')
    lookup05 = models.ForeignKey("Lookup", blank=True, null=True, related_name='+')
    lookup06 = models.ForeignKey("Lookup", blank=True, null=True, related_name='+')
    lookup07 = models.ForeignKey("Lookup", blank=True, null=True, related_name='+')
    text01 = models.TextField(blank=True, null=True)
    text02 = models.TextField(blank=True, null=True)

    def __unicode__(self):
        return u'Attributes for news item %s' % self.news_item_id

class LookupManager(models.Manager):
    def get_or_create_lookup(self, schema_field, name, code=None, description='', make_text_slug=True, logger=None, using=None):
        """
        Returns the Lookup instance matching the given SchemaField, name and
        Lookup.code, creating it (with the given name/code/description) if it
        doesn't already exist.
        If make_text_slug is True, then a slug will be created from the given
        name. If it's False, then the slug will be the Lookup's ID.
        """
        code = code or name # code defaults to name if it wasn't provided

        # Convert code to a string if it's not. Otherwise, the Lookup.objects.get(...)
        # will fail.
        if not isinstance(code, basestring):
            code = unicode(code)
        if not using:
            using = settings.SHORT_NAME
        try:
            obj = Lookup.objects.using(using).get(schema_field__id=schema_field.id, code=code)
        except Lookup.DoesNotExist:
            if make_text_slug:
                slug = slugify(name)
                if len(slug) > 32:
                    # Only bother to warn if we're actually going to use the slug.
                    if make_text_slug and logger:
                        logger.warn("Trimming slug %r to %r in order to fit 32-char limit." % (slug, slug[:32]))
                    slug = slug[:32]
            else:
                # To avoid integrity errors in the slug when creating the Lookup,
                # use a temporary dummy slug that's guaranteed not to be in use.
                # We'll change it back immediately afterward.
                slug = '__TEMPORARY__'
            if len(name) > 255:
                old_name = name
                name = name[:250] + '...'
                # Save the full name in the description.
                if not description:
                    description = old_name
                if logger:
                    logger.warn("Trimming name %r to %r in order to fit 255-char limit." % (old_name, name))
            obj = Lookup.objects.using(using).create(schema_field_id=schema_field.id, name=name, code=code, slug=slug,
                                                     description=description)
            if not make_text_slug:
                # Set the slug to the ID.
                obj.slug = obj.id
                Lookup.objects.using(using).filter(id=obj.id).update(slug=obj.id)
            if logger:
                logger.info('Created %s %r' % (schema_field.name, name))
        return obj

class Lookup(models.Model):
    schema_field = models.ForeignKey(SchemaField)
    name = models.CharField(max_length=255)
    # `code` is the optional internal code to use during retrieval.
    # For example, in scraping Chicago crimes, we use the crime type code
    # to find the appropriate crime type in this table. We can't use `name`
    # in that case, because we've massaged `name` to use a "prettier"
    # formatting than exists in the data source.
    code = models.CharField(max_length=255, blank=True)
    slug = models.CharField(max_length=32, db_index=True)
    description = models.TextField(blank=True)

    objects = LookupManager()

    class Meta:
        unique_together = (('slug', 'schema_field'),)

    def __unicode__(self):
        return u'%s - %s' % (self.schema_field, self.name)

class SearchSpecialCase(models.Model):
    query = models.CharField(max_length=64, unique=True)
    redirect_to = models.CharField(max_length=255, blank=True)
    title = models.CharField(max_length=128, blank=True)
    body = models.TextField(blank=True)

    def __unicode__(self):
        return self.query

class DataUpdate(models.Model):
    # Keeps track of each time we update our data.
    schema = models.ForeignKey(Schema)
    update_start = models.DateTimeField()  # When the scraper/importer started running.
    update_finish = models.DateTimeField() # When the scraper/importer finished.
    num_added = models.IntegerField()
    num_changed = models.IntegerField()
    num_deleted = models.IntegerField()
    num_skipped = models.IntegerField()
    got_error = models.BooleanField()
    list_records_seen = models.IntegerField(null=True)
    detail_records_seen = models.IntegerField(null=True)
    exc_type = models.CharField(max_length=100)
    exc_value = models.TextField()
    traceback = models.TextField()
    scraper = models.CharField(max_length=100)

    def __unicode__(self):
        return u'%s started on %s' % (self.schema.name, self.update_start)

    def total_time(self):
        delta = self.update_finish - self.update_start
        return str(delta).split('.')[0]
