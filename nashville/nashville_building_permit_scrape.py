"""
Scraper for City of Nashville building permits.
https://data.nashville.gov/resource/eh88-icrc.json
"""

from scraper import Scraper
import json
import requests

DATA_URL = 'https://data.nashville.gov/resource/eh88-icrc.json'

class BuildingPermitScraper(Scraper):
    schema = 'building-permits'
    primary_key = ('permit_number',)

    def data(self):

        data = requests.get(DATA_URL)
        json_string = json.loads(data.text)

        for item in json_string:
            yield self.convert_item(item)

    def convert_item(self, item):
        "Convert Socrata item into a item that we can insert as a newsitem."

        perm_type_desc = item["permit_type_description"]
        perm_type_for_title = self.smart_title(perm_type_desc.upper().replace("PERMIT", "").strip())

        item_date = self.date(item['date_issued'], '%Y-%m-%dT00:00:00')

        try:
            estimated_value = int(round(float(item['const_cost'])))
        except:
            estimated_value = None

        # If the estimated value is over one trillion, it's probably not
        # reliable, and besides, we might not be able to store it in the
        # database.
        if estimated_value > 1000000000:
            estimated_value = None

        address = self.clean_address(item['address'])
        mapped_location = None
        try:
            mapped_location = self.point(float(item['mapped_location']['longitude']), float(item['mapped_location']['latitude']))
        except KeyError:
            pass

        return dict(
            title='Permit issued for ' + perm_type_for_title.lower(),
            item_date=item_date,
            location=mapped_location,
            location_name=address,
            description=item['purpose'] if 'purpose' in item else 'Unassigned',
            permit_number=item['permit'],
            valuation=estimated_value,
            permit_type=item['per_ty'], # perm_type_desc
            contact=item['contact'],
            parcel=item['parcel'],
            subdivision_lot=item['subdivision_lot'],
        )

if __name__ == "__main__":
    BuildingPermitScraper().run()
