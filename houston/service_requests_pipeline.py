"""
Houston service requests.
Main page:
http://hfdapp.houstontx.gov/311/index.php
http://seeclickfix.com/houston/open311/
Houston provides 311 data via file download. As of Nov 2014, the quality of the
data is less than ideal. There are a lot of unusable records because of missing
data, and it doesn't have any media references.
SeeClickFix provides an API based alternative. The data is sourced from the
official Houston 311 app, which SeeClickFix happens to provide. The SeeClickFix
follows the Open311 spec.
"""
import arrow
import urllib

import sundew_pipeline
import stages as stages


API_BASE_URL = 'https://seeclickfix.com/api/v2/issues/'


class Pipeline(sundew_pipeline.Pipeline):
    name = 'houston.service_requests'

    def __init__(self, *args, **kwargs):
        if 'start_date' in kwargs and 'end_date' in kwargs:
            self.end_date = arrow.get(kwargs.pop('end_date'))
            self.start_date = arrow.get(kwargs.pop('start_date'))
        else:
            # Default to a day's worth of time
            self.end_date = arrow.get().to('US/Central')
            self.start_date = self.end_date.replace(days=-30)

        super(Pipeline, self).__init__(*args, **kwargs)

    def get_stages(self):
        """
        Standard fare.
        """
        return [
            self.get_list_urls,
            stages.Download, # API call for each URL
            stages.ParseJson, # To lists and dicts
            self.pluck_issues,
            stages.Flatten
        ]

    def get_list_urls(self):
        """
        Fetch successive pages from the API.
        """
        # Iterate through each day of the month
        for start, end in arrow.Arrow.span_range('day', self.start_date, self.end_date):
            params = dict(
                place_url='houston',
                per_page=1000,
                after=start.isoformat(),
                before=end.isoformat(),
            )
            url = API_BASE_URL + '?' + urllib.urlencode(params)
            yield url

    def pluck_issues(self, source):
        """
        Add domain to absolute URL path.
        """
        yield source['issues']



if __name__ == '__main__':
    pipeline = Pipeline(debug=False)
    pipeline.run()
