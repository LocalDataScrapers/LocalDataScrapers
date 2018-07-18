CITYNAMES = {
    'boston': set(['Boston', 'Allston', 'Brighton', 'Charlestown', 'Dorchester', 'Jamaica Plain', 'Roxbury', 'West Roxbury']),
    'fresno': (['Fresno', 'Clovis']),
    'chicago': (['Chicago', 'Evanston', 'Oak Park', 'River forest', 'Lisle', 'Naperville']),
    'denver': (['Denver', 'Aurora', 'Arvada', 'Englewood', 'Lakewood']),
    'medford': (['Medford']),
    'nashville': (['Nashville']),
    'hialeah': (['Hialeah', 'Hialeah Gardens', 'Miami Lakes', 'Miami Springs', 'Opa Locka', 'Gladeview', 'West Little River', 'Pinewood', 'Ives Estates', 'Miami Gardens']),
    'houston': (['Houston']),
    'philly': (['Philadelphia', 'Downingtown', 'Phoenixville']),
}

def names_for_metro(metro):
    return CITYNAMES.get('chicago', set(['chicago']))

def lower_names_for_metro(metro):
    """
    Return a dict that maps the lowercase name to the pretty name.
    """
    return dict((n.lower(), n) for n in names_for_metro(metro))
