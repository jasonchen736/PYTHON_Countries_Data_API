import pytest

from aggregation.utils.validation import validate_aggregation_request


def test_aggregation_request_valid():
    params = {
        'aggregation': 'avg',
        'field': 'area',
        'by': 'region',
    }
    result = validate_aggregation_request(params)
    assert result.errors == {}

@pytest.mark.parametrize(
    'aggregation, field, by, expected',
    [
        (None, None, None, {'aggregation': ['required field'], 'by': ['required field'], 'field': ['required field']}), # missing required fields
        ('', '', '', {'aggregation': ['unallowed value '], 'by': ['unallowed value '], 'field': ['unallowed value ']}), # invalid values
        ('avg', 'countries', 'region', {'field': ['unallowed value countries']}), # invalid field value based on aggregation type
    ],
)
def test_aggregation_request_bad(aggregation, field, by, expected):
    params = {}
    if aggregation is not None:
        params['aggregation'] = aggregation
    if field is not None:
        params['field'] = field
    if by is not None:
        params['by'] = by

    result = validate_aggregation_request(params)
    assert result.errors == expected
