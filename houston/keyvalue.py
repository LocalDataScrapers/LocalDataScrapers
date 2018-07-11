import os
import os.path as op
import sqlite3
import cPickle as pickle

import requests


class KeyValueStore(object):
    def __init__(self, file_path, overwrite=True):
        already_exists = op.exists(file_path)
        if already_exists and overwrite:
            os.remove(file_path)

        self.conn = sqlite3.connect(file_path)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        try:
            self.cursor.execute('CREATE TABLE main (key text, content blob);')
        except sqlite3.OperationalError as ex:
            if ex.message != 'table main already exists':
                raise ex

    def get(self, key):
        args = (key,)
        self.cursor.execute('SELECT content FROM main WHERE key = ?;', args)
        result = self.cursor.fetchone()
        if result is None:
            return None

        content = str(result[0])    # convert from buffer to str
        return content

    def set(self, key, value):
        buf = buffer(value)
        sql = 'INSERT INTO main (key, content) VALUES (?, ?);'
        args = (key, buf)
        self.cursor.execute(sql, args)
        self.conn.commit()

    def __del__(self, ):
        self.conn.close()


class RequestCache(object):
    """
    Stores and retrieves the results of HTTP requests.
    """

    def __init__(self, kvstore):
        self.store = kvstore

    def get(self, request_method, url, data=None):
        "Returns a requests.Response object."
        key = repr((request_method, url, data))
        content = self.store.get(key)
        if content:
            resp = requests.Response()
            resp.status_code = 200
            resp._content = content
            return resp
        else:
            return None

    def set(self, response):
        "Accepts a requests.Response object."
        # Note that response.url returns a unicode string, so we have to convert
        # to a simple string before it can be used as part of the key.
        key = repr((response.request.method, str(response.url),
            response.request.body))
        self.store.set(key, response.content)


class ObjectStore(object):
    def __init__(self, kvstore):
        self.store = kvstore

    def get(self, key):
        return pickle.loads(self.store.get(key))

    def set(self, key, obj):
        self.store.set(key, pickle.dumps(obj))
