import datetime
from unittest.mock import patch

from freezegun import freeze_time
import pytest

from aggregation import app
from aggregation.utils.country_data_aggregator import CountryDataAggregator


@pytest.fixture(scope='module')
def client():
    app.config['TESTING'] = True
    testing_client = app.test_client()
    yield testing_client

def test_aggrigations_api_get_valid_request(client):
    CountryDataAggregator.country_data = None
    CountryDataAggregator.country_data_expiry = None

    with patch('aggregation.views.aggregations_api.requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = '{}'
        mock_get.return_value.headers = {}

        response = client.get('/aggregated-data?aggregation=sum&field=area&by=region')

    assert response.status_code == 200
    assert response.get_json() == {}

@freeze_time('2019-09-20 00:00:00')
def test_aggrigations_api_get_valid_request_no_max_age(client):
    CountryDataAggregator.country_data = None
    CountryDataAggregator.country_data_expiry = None

    with patch('aggregation.views.aggregations_api.requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = '{}'
        mock_get.return_value.headers = {'Cache-Control': 'a=b'}

        response = client.get('/aggregated-data?aggregation=sum&field=area&by=region')

    assert response.status_code == 200
    assert response.get_json() == {}
    assert CountryDataAggregator.country_data_expiry == datetime.datetime.now() + datetime.timedelta(seconds=86400)

@freeze_time('2019-09-20 00:00:00')
def test_aggrigations_api_get_valid_request_with_max_age(client):
    CountryDataAggregator.country_data = None
    CountryDataAggregator.country_data_expiry = None

    with patch('aggregation.views.aggregations_api.requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = '{}'
        mock_get.return_value.headers = {'Cache-Control': 'max-age=60'}

        response = client.get('/aggregated-data?aggregation=sum&field=area&by=region')

    assert response.status_code == 200
    assert response.get_json() == {}
    assert CountryDataAggregator.country_data_expiry == datetime.datetime.now() + datetime.timedelta(seconds=60)

def test_aggrigations_api_get_valid_request_cached_data(client):
    CountryDataAggregator.country_data = {}
    CountryDataAggregator.country_data_expiry = datetime.datetime.now() + datetime.timedelta(hours=1)

    response = client.get('/aggregated-data?aggregation=sum&field=area&by=region')

    assert response.status_code == 200
    assert response.get_json() == {}

def test_aggrigations_api_get_bad_request(client):
    response = client.get('/aggregated-data')
    assert response.status_code == 400
    assert response.get_json() == {'aggregation': ['required field'], 'by': ['required field'], 'field': ['required field']}

def test_aggrigations_api_get_api_error(client):
    CountryDataAggregator.country_data = None
    CountryDataAggregator.country_data_expiry = None

    with patch('aggregation.views.aggregations_api.requests.get') as mock_get:
        mock_get.return_value.status_code = 500
        response = client.get('/aggregated-data?aggregation=sum&field=area&by=region')

    assert response.status_code == 503

def test_aggrigations_api_get_api_exception(client):
    CountryDataAggregator.country_data = None
    CountryDataAggregator.country_data_expiry = None

    with patch('aggregation.views.aggregations_api.requests.get') as mock_get:
        mock_get.side_effect = Exception('test')
        response = client.get('/aggregated-data?aggregation=sum&field=area&by=region')

    assert response.status_code == 503

