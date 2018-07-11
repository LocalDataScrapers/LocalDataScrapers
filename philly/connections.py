from django.conf import settings

def get_connection_name(short_name):
    """
    Return the connection name to use for the given metro.
    """
    # When running tests, always use the default connection. This avoids
    # problems with having both a default connection and a metro-specific one.
    if getattr(settings, 'TESTING', False):
        return 'default'
    return short_name
