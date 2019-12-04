from cerberus import Validator


AGGREGATION_OPTIONS = [
    'avg',
    'count',
    'max',
    'min',
    'sum',
]
FIELD_OPTIONS = {
    'avg': [
        'area',
        'borders',
        'currencies',
        'gini',
        'languages',
        'latlng',
        'population',
    ],
    'count': [
        'borders',
        'countries',
        'currencies',
        'languages',
    ],
    'max': [
        'area',
        'gini',
        'population',
    ],
    'min': [
        'area',
        'borders',
        'currencies',
        'gini',
        'languages',
        'population',
    ],
    'sum': [
        'area',
        'gini',
        'population',
    ],
}
REGION_OPTIONS = ['region', 'subregion']

def validate_aggregation_request(data):
    """Validate aggregation request input

    Keyword arguments:
    data -- dictionary of parameters to validate against
    """
    schema = {
        'aggregation': {'type': 'string', 'allowed': AGGREGATION_OPTIONS},
        'field': {'type': 'string', 'allowed': FIELD_OPTIONS.get(data.get('aggregation'), [])},
        'by': {'type': 'string', 'allowed': REGION_OPTIONS},
    }
    validator = Validator(require_all=True)
    validator.validate(data, schema)
    return validator
