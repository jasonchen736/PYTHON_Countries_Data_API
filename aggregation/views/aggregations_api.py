import json
import requests

from flask import current_app, jsonify, request
from flask.views import MethodView
from werkzeug.exceptions import abort

from aggregation.utils.country_data_aggregator import CountryDataAggregator
from aggregation.utils.validation import validate_aggregation_request


class AggregationsAPI(MethodView):

    def get(self):
        """Retrieve aggregated stats by aggregation type, metric, and region
        """

        # validate request parameters
        validation_results = validate_aggregation_request(request.args)
        if validation_results.errors:
            # we can humanize the messages here
            return jsonify(validation_results.errors), 400

        # initialize aggregation object
        country_data = CountryDataAggregator()
        # fetch data
        if country_data.is_expired():
            try:
                # for showcasing code, we're not going to use the api url
                import sys
                if 'pytest' in sys.modules:
                    response = requests.get(current_app.config.get('DATA_API_URL'), timeout=15)
                else:
                    import os
                    from unittest.mock import patch
                    with patch('requests.get') as mock_get:
                        sample_data_file = os.path.join(os.getcwd(), 'sample_data', 'data.json')
                        with open(sample_data_file, 'r', encoding='utf-8') as f:
                            mock_get.return_value.status_code = 200
                            mock_get.return_value.text = f.read()
                            mock_get.return_value.headers = {}
                        response = requests.get(current_app.config.get('DATA_API_URL'), timeout=15)
                if response.status_code == 200:
                    json_data = json.loads(response.text)
                    # let's cache the data, use cache control max-age as indicator
                    # default to caching data for a day
                    seconds = 86400
                    cache_control = response.headers.get('Cache-Control')
                    if cache_control:
                        parts = cache_control.split(',')
                        for part in parts:
                            if part.strip().startswith('max-age='):
                                _, seconds = part.split('=')
                    country_data.store_data(json_data, int(seconds))
                else:
                    # we can return a message if necessary
                    abort(503)
            except Exception:
                # timeouts and other errors
                # as it relates to our api, this could be a 503
                # we can break this up if necessary
                abort(503)

        # process aggregation
        aggregation_results = country_data.get_aggregation(request.args)

        return jsonify(aggregation_results), 200
