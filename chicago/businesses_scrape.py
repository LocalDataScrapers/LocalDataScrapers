"""
Importer for City of Chicago business licenses.
Main page:
https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses/r5kz-chrr
Notes on this dataset:
https://github.com/everyblock/everyblock.com/wiki/Chicago-Business-License-Dataset-Analysis
"""

import datetime
import re
import json
from scraper import Scraper

suffixes = {
 'ALY': ['ALLEE', 'ALLY', 'ALLEY', 'ALY'],
 'ANX': ['ANEX', 'ANNX', 'ANX', 'ANNEX'],
 'ARC': ['ARC', 'ARCADE'],
 'AVE': ['AVEN', 'AVNUE', 'AVENU', 'AVN', 'AV', 'AVE', 'AVENUE'],
 'BCH': ['BCH', 'BEACH'],
 'BG': ['BURG'],
 'BGS': ['BURGS'],
 'BLF': ['BLUF', 'BLF', 'BLUFF'],
 'BLFS': ['BLUFFS'],
 'BLVD': ['BLVD', 'BOULV', 'BOUL', 'BOULEVARD', 'BL'],
 'BND': ['BEND', 'BND'],
 'BR': ['BRNCH', 'BR', 'BRANCH'],
 'BRG': ['BRG', 'BRIDGE', 'BRDGE'],
 'BRK': ['BRK', 'BROOK'],
 'BRKS': ['BROOKS'],
 'BTM': ['BTM', 'BOTTM', 'BOT', 'BOTTOM'],
 'BYP': ['BYPS', 'BYPA', 'BYPAS', 'BYP', 'BYPASS'],
 'BYU': ['BAYOU', 'BAYOO'],
 'CIR': ['CIRC', 'CRCLE', 'CIR', 'CIRCL', 'CIRCLE', 'CRCL'],
 'CIRS': ['CIRCLES'],
 'CLB': ['CLUB', 'CLB'],
 'CLF': ['CLF', 'CLIFF'],
 'CLFS': ['CLIFFS', 'CLFS'],
 'CMN': ['COMMON'],
 'COR': ['CORNER', 'COR'],
 'CORS': ['CORNERS', 'CORS'],
 'CP': ['CP', 'CAMP', 'CMP'],
 'CPE': ['CAPE', 'CPE'],
 'CRES': ['CRESENT',
          'CRECENT',
          'CRSENT',
          'CRSNT',
          'CRES',
          'CRSCNT',
          'CRESCENT'],
 'CRK': ['CK', 'CR', 'CREEK', 'CRK'],
 'CRSE': ['COURSE', 'CRSE'],
 'CRST': ['CREST'],
 'CSWY': ['CSWY', 'CAUSWAY', 'CAUSEWAY'],
 'CT': ['COURT', 'CRT', 'CT'],
 'CTR': ['CNTER', 'CTR', 'CENTRE', 'CEN', 'CENT', 'CNTR', 'CENTR', 'CENTER'],
 'CTRS': ['CENTERS'],
 'CTS': ['COURTS', 'CTS'],
 'CURV': ['CURVE'],
 'CV': ['COVE', 'CV'],
 'CVS': ['COVES'],
 'CYN': ['CANYON', 'CANYN', 'CNYN', 'CYN'],
 'DL': ['DALE', 'DL'],
 'DM': ['DAM', 'DM'],
 'DR': ['DRIV', 'DR', 'DRIVE', 'DRV'],
 'DRS': ['DRIVES'],
 'DV': ['DV', 'DVD', 'DIV', 'DIVIDE'],
 'EST': ['EST', 'ESTATE'],
 'ESTS': ['ESTATES', 'ESTS'],
 'EXPY': ['EXPY', 'EXPR', 'EXPRESS', 'EXPW', 'EXP', 'EXPWY', 'EXPRESSWAY'],
 'EXT': ['EXTN', 'EXT', 'EXTNSN', 'EXTENSION'],
 'EXTS': ['EXTS', 'EXTENSIONS'],
 'FALL': ['FALL'],
 'FLD': ['FIELD', 'FLD'],
 'FLDS': ['FIELDS', 'FLDS'],
 'FLS': ['FLS', 'FALLS'],
 'FLT': ['FLAT', 'FLT'],
 'FLTS': ['FLATS', 'FLTS'],
 'FRD': ['FRD', 'FORD'],
 'FRDS': ['FORDS'],
 'FRG': ['FORGE', 'FRG', 'FORG'],
 'FRGS': ['FORGES'],
 'FRK': ['FORK', 'FRK'],
 'FRKS': ['FORKS', 'FRKS'],
 'FRST': ['FRST', 'FOREST', 'FORESTS'],
 'FRY': ['FERRY', 'FRY', 'FRRY'],
 'FT': ['FRT', 'FT', 'FORT'],
 'FWY': ['FREEWAY', 'FRWAY', 'FRWY', 'FREEWY', 'FWY'],
 'GDN': ['GARDN', 'GRDN', 'GARDEN', 'GDN', 'GRDEN'],
 'GDNS': ['GDNS', 'GRDNS', 'GARDENS'],
 'GLN': ['GLEN', 'GLN'],
 'GLNS': ['GLENS'],
 'GRN': ['GRN', 'GREEN'],
 'GRNS': ['GREENS'],
 'GRV': ['GROVE', 'GRV', 'GROV'],
 'GRVS': ['GROVES'],
 'GTWY': ['GTWAY', 'GATWAY', 'GTWY', 'GATEWAY', 'GATEWY'],
 'HBR': ['HARBOR', 'HARBR', 'HARB', 'HRBOR', 'HBR'],
 'HBRS': ['HARBORS'],
 'HL': ['HILL', 'HL'],
 'HLS': ['HLS', 'HILLS'],
 'HOLW': ['HOLW', 'HOLWS', 'HLLW', 'HOLLOWS', 'HOLLOW'],
 'HTS': ['HTS', 'HEIGHT', 'HGTS', 'HT', 'HEIGHTS'],
 'HVN': ['HVN', 'HAVEN', 'HAVN'],
 'HWY': ['HIWY', 'HIGHWAY', 'HWY', 'HWAY', 'HIWAY', 'HIGHWY'],
 'INLT': ['INLT', 'INLET'],
 'IS': ['ISLAND', 'IS', 'ISLND'],
 'ISLE': ['ISLE', 'ISLES'],
 'ISS': ['ISS', 'ISLANDS', 'ISLNDS'],
 'JCT': ['JCT', 'JCTN', 'JUNCTION', 'JCTION', 'JUNCTN', 'JUNCTON'],
 'JCTS': ['JCTNS', 'JCTS', 'JUNCTIONS'],
 'KNL': ['KNL', 'KNOL', 'KNOLL'],
 'KNLS': ['KNOLLS', 'KNLS'],
 'KY': ['KY', 'KEY'],
 'KYS': ['KEYS', 'KYS'],
 'LAND': ['LAND'],
 'LCK': ['LOCK', 'LCK'],
 'LCKS': ['LOCKS', 'LCKS'],
 'LDG': ['LODGE', 'LDGE', 'LODG', 'LDG'],
 'LF': ['LF', 'LOAF'],
 'LGT': ['LIGHT', 'LGT'],
 'LGTS': ['LIGHTS'],
 'LK': ['LAKE', 'LK'],
 'LKS': ['LAKES', 'LKS'],
 'LN': ['LN', 'LANE', 'LANES', 'LA'],
 'LNDG': ['LNDNG', 'LNDG', 'LANDING'],
 'LOOP': ['LOOPS', 'LOOP'],
 'MALL': ['MALL'],
 'MDW': ['MEADOW', 'MDW'],
 'MDWS': ['MEDOWS', 'MEADOWS', 'MDWS'],
 'MEWS': ['MEWS'],
 'ML': ['ML', 'MILL'],
 'MLS': ['MLS', 'MILLS'],
 'MNR': ['MNR', 'MANOR'],
 'MNRS': ['MNRS', 'MANORS'],
 'MSN': ['MSN', 'MISSN', 'MISSION', 'MSSN'],
 'MT': ['MT', 'MOUNT', 'MNT'],
 'MTN': ['MOUNTAIN', 'MOUNTIN', 'MNTN', 'MNTAIN', 'MTN', 'MTIN'],
 'MTNS': ['MOUNTAINS', 'MNTNS'],
 'MTWY': ['MOTORWAY'],
 'NCK': ['NCK', 'NECK'],
 'OPAS': ['OVERPASS'],
 'ORCH': ['ORCHRD', 'ORCH', 'ORCHARD'],
 'OVAL': ['OVAL', 'OVL'],
 'PARK': ['PK', 'PARK', 'PARKS', 'PRK'],
 'PASS': ['PASS'],
 'PATH': ['PATH', 'PATHS'],
 'PIKE': ['PIKE', 'PIKES'],
 'PKWY': ['PKWAY', 'PKY', 'PARKWAYS', 'PKWY', 'PARKWY', 'PKWYS', 'PARKWAY'],
 'PL': ['PLACE', 'PL'],
 'PLN': ['PLAIN', 'PLN'],
 'PLNS': ['PLNS', 'PLAINS', 'PLAINES'],
 'PLZ': ['PLAZA', 'PLZ', 'PLZA'],
 'PNE': ['PINE'],
 'PNES': ['PINES', 'PNES'],
 'PR': ['PR', 'PRR', 'PRAIRIE', 'PRARIE'],
 'PRT': ['PRT', 'PORT'],
 'PRTS': ['PRTS', 'PORTS'],
 'PSGE': ['PASSAGE'],
 'PT': ['PT', 'POINT'],
 'PTS': ['POINTS', 'PTS'],
 'RADL': ['RADL', 'RAD', 'RADIEL', 'RADIAL'],
 'RAMP': ['RAMP'],
 'RD': ['RD', 'ROAD'],
 'RDG': ['RDG', 'RIDGE', 'RDGE'],
 'RDGS': ['RDGS', 'RIDGES'],
 'RDS': ['ROADS', 'RDS'],
 'RIV': ['RIV', 'RVR', 'RIVER', 'RIVR'],
 'RNCH': ['RANCHES', 'RANCH', 'RNCH', 'RNCHS'],
 'ROW': ['ROW'],
 'RPD': ['RAPID', 'RPD'],
 'RPDS': ['RPDS', 'RAPIDS'],
 'RST': ['RST', 'REST'],
 'RTE': ['ROUTE'],
 'RUE': ['RUE'],
 'RUN': ['RUN'],
 'SHL': ['SHL', 'SHOAL'],
 'SHLS': ['SHLS', 'SHOALS'],
 'SHR': ['SHOAR', 'SHORE', 'SHR'],
 'SHRS': ['SHORES', 'SHOARS', 'SHRS'],
 'SKWY': ['SKYWAY'],
 'SMT': ['SMT', 'SUMMIT', 'SUMITT', 'SUMIT'],
 'SPG': ['SPRING', 'SPNG', 'SPRNG', 'SPG'],
 'SPGS': ['SPRINGS', 'SPGS', 'SPRNGS', 'SPNGS'],
 'SPUR': ['SPUR', 'SPURS'],
 'SQ': ['SQR', 'SQ', 'SQUARE', 'SQU', 'SQRE'],
 'SQS': ['SQRS', 'SQUARES'],
 'ST': ['STRT', 'STREET', 'STR', 'ST'],
 'STA': ['STATN', 'STN', 'STATION', 'STA'],
 'STRA': ['STRAVE',
          'STRAV',
          'STRAVEN',
          'STRAVN',
          'STRVN',
          'STRAVENUE',
          'STRVNUE',
          'STRA'],
 'STRM': ['STREME', 'STRM', 'STREAM'],
 'STS': ['STREETS'],
 'TER': ['TER', 'TERRACE', 'TERR'],
 'TPKE': ['TURNPK', 'TRPK', 'TPK', 'TPKE', 'TURNPIKE', 'TRNPK'],
 'TRAK': ['TRACK', 'TRACKS', 'TRKS', 'TRK', 'TRAK'],
 'TRCE': ['TRCE', 'TRACES', 'TRACE'],
 'TRFY': ['TRFY', 'TRAFFICWAY'],
 'TRL': ['TRLS', 'TRAIL', 'TR', 'TRL', 'TRAILS'],
 'TRWY': ['THROUGHWAY'],
 'TUNL': ['TUNEL', 'TUNNEL', 'TUNLS', 'TUNL', 'TUNNL', 'TUNNELS'],
 'UN': ['UNION', 'UN'],
 'UNS': ['UNIONS'],
 'UPAS': ['UNDERPASS'],
 'VIA': ['VIADUCT', 'VDCT', 'VIA', 'VIADCT'],
 'VIS': ['VSTA', 'VIS', 'VISTA', 'VST', 'VIST'],
 'VL': ['VILLE', 'VL'],
 'VLG': ['VILLAG', 'VILLG', 'VILLIAGE', 'VLG', 'VILL', 'VILLAGE'],
 'VLGS': ['VLGS', 'VILLAGES'],
 'VLY': ['VLY', 'VALLEY', 'VALLY', 'VLLY'],
 'VLYS': ['VALLEYS', 'VLYS'],
 'VW': ['VW', 'VIEW'],
 'VWS': ['VWS', 'VIEWS'],
 'WALK': ['WALKS', 'WALK', 'WK'],
 'WALL': ['WALL'],
 'WAY': ['WY', 'WAY'],
 'WAYS': ['WAYS'],
 'WL': ['WELL'],
 'WLS': ['WLS', 'WELLS'],
 'XING': ['CROSSING', 'XING', 'CRSSNG', 'CRSSING'],
 'XRD': ['CROSSROAD']}

DATA_URL = 'http://data.cityofchicago.org/api/views/INLINE/rows.json?method=index'
INLINE_QUERY_TEMPLATE = """{
    "originalViewId": "r5kz-chrr",
    "name": "Inline Filter",
    "query" : {
        "filterCondition" : {
            "type" : "operator",
            "value" : "GREATER_THAN",
            "children": [
                {
                    "columnFieldName" : "license_start_date",
                    "type" : "column"
                },
                {
                    "type" : "literal",
                    "value" : "%(start_date)s"
                }
            ]
        }
    }
}"""

# Skip these types of licenses because their addresses are usually private
# residences. In the case of Raffles, we skip these because they only apply to
# nonprofit organizations (which may have sensitive addresses, e.g. a women's
# domestic abuse center).
SKIP_LICENSE_CODES = (
    1012,       # Home Occupation
    1431,       # Junk Peddler
    1525,       # Massage Therapist
    1603,       # Peddler, food (fruits & vegtables only)
    1604,       # Peddler, non-food
    1605,       # Street Performer
    1606,       # Peddler,food - (fruits and vegetables only) - special
    1607,       # Peddler, non-food, special
    1625,       # Raffles
    4406,       # Peddler License
)

SKIP_LICENSE_TITLES = set(['TAXES ON WHEELS, INC.'])

class BusinessLicenseScraper(Scraper):
    schema = 'business-licenses'
    primary_key = ('license_number', 'item_date')

    def data(self):
        # Build an inline query for business licenses that took effect within the past 90 days:
        start_date = datetime.date.today() - datetime.timedelta(days=90)
        inline_query = INLINE_QUERY_TEMPLATE % {
            'start_date': start_date.strftime('%Y-%m-%dT00:00:00'),
        }
        json_string = self.get(DATA_URL, inline_query, headers={'Content-Type': 'application/json'})

        records = list(self.parse_socrata(json_string))
        for record in self.filter_records(records):
            yield self.convert_record(record)

    def filter_records(self, records):
        """
        Filter out records that have the following characteristics:
        - Shares primary keys with another record
        - Not in Chicago
        - Does not have a license_start_date
        - Has a license_code that we don't want to include in our database
        """
        keys = {}       # keeps track of whether keys have duplicates
        for record in records:
            key = '{0}|{1}'.format(record['license_number'], record['license_start_date'])
            if key in keys:
                keys[key] = True        # this key is not unique
            else:
                keys[key] = False       # this is the first occurrence of this key
            record['__eb_key'] = key      # store it for later use

        for record in records:
            if keys.get(record['__eb_key']) == True:
                continue
            if record['city'] is None or record['city'].lower() != 'chicago' or \
            record['state'].lower() != 'il':
                continue
            if record['license_start_date'] is None:
                continue
            if int(record['license_code']) in SKIP_LICENSE_CODES:
                continue
            if record['doing_business_as_name'].strip() in SKIP_LICENSE_TITLES:
                continue
            # TODO: Put business license renewals in their own schema
            if record['application_type'] == 'RENEW':
                continue

            yield record

    def convert_record(self, record):
        "Convert Socrata record into a record that we can insert as a newsitem."
        # Does this need further cleanup?
        location_name = self.clean_address(record['address'])
        dba_name = record['doing_business_as_name'].strip()

        try:
            location = self.point(float(record['longitude']), float(record['latitude']))
        except TypeError:
            location = None
        
        return dict(
            title=dba_name,
            item_date=self.date(record['license_start_date'], '%Y-%m-%dT00:00:00'),

            location=location,
            location_name=location_name,
            location_name_geocoder=fix_address(location_name),

            doing_business_as_name=dba_name,
            license_code=int(record['license_code']),
            license_code__name=record['license_description'],
            application_type=record['application_type'],
            license_number=int(record['license_number']),
            account_number=int(record['account_number']),
            site_number=int(record['site_number']),
            expiration_date=self.date(record['expiration_date'], '%Y-%m-%dT00:00:00')
        )

# Common street address suffixes
suffixes_re = re.compile('|'.join(s+'\\.' for s in suffixes.keys()), re.IGNORECASE)
# Common trailing number endings
number_endings_re = re.compile(r'\d+(st|nd|rd|th)?\s*$', re.IGNORECASE)
# Common ending for floor
floor_ending_re = re.compile(r'(Fl\.|Fl|Floor|)\s*$', re.IGNORECASE)

def fix_address(address):
    m = suffixes_re.search(address)
    if m:
        # Chop off anything past the suffix
        address = address[:m.span()[1]]

    m = floor_ending_re.search(address)
    if m:
        # Chop off the Floor keyword
        address = address[:m.span()[0]]

    while True:
        m = number_endings_re.search(address)
        if m:
            # Chop off any trailing numbers
            address = address[:m.start()].strip()
        else:
            break

    return address

if __name__ == "__main__":
    BusinessLicenseScraper().run()
