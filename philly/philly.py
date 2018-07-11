from base import *

SHORT_NAME = "philly"
CACHE_MIDDLEWARE_KEY_PREFIX = SHORT_NAME
TIME_ZONE = 'America/New_York'

DB_ENGINE = 'django.contrib.gis.db.backends.postgis'
DB_USER = 'postgres'
DB_HOST = '127.0.0.1'
DB_PORT = '5432'

MASTER_NAME = SHORT_NAME
STANDBY_NAME = '%s_standby' % SHORT_NAME

DATABASES = {
    'default': {'NAME': MASTER_NAME, 'ENGINE': DB_ENGINE, 'USER': DB_USER, 'HOST': DB_HOST},
    SHORT_NAME: {'NAME': MASTER_NAME, 'ENGINE': DB_ENGINE, 'USER': DB_USER, 'HOST': DB_HOST},
    'standby': {'NAME': STANDBY_NAME, 'ENGINE': DB_ENGINE, 'USER': DB_USER, 'HOST': DB_HOST},
    'internal': {'NAME': 'internal', 'ENGINE': DB_ENGINE, 'USER': DB_USER, 'HOST': DB_HOST},
}
