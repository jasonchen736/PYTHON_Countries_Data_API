import datetime
from functools import partial
import statistics


# Map aggregation types to functions
AGGREGATION_FUNCTIONS = {
    'avg': statistics.mean,
    'count': len,
    'max': max,
    'min': min,
    'sum': sum,
}

def aggregate_list(aggregation_function, data_set):
    """Return aggregate results of data set based on provided aggregation function

    Keyword arguments:
    aggregation_function -- function used to calculate aggregate
    data_set -- list of values to aggregate
    """
    aggregation_results = []
    for sub_set in data_set:
        aggregation_results.append(round(aggregation_function(sub_set), 2))
    return aggregation_results

def append_list(length, target, value):
    """Accumulator function to accumulate a list of values

    Keyword arguments:
    length -- expected size of values list
    target -- list to accumulate values in
    value -- a list of values
    """
    if not value:
        value = []
    if not target:
        for i in range(length):
            target.append([])
    # append 0 for non existing values
    value_length = len(value)
    for i in range(length):
        if i < value_length:
            append_value = value[i]
        else:
            append_value = 0
        target[i].append(append_value)

def append_one(target, value):
    """Accumulator function to just always accumulate 1

    Keyword arguments:
    target -- list to accumulate values in
    value -- value is a param of accumulator fuctions, ignored here
    """
    target.append(1)

# Map custom accumulation and aggregation functions
CUSTOM_PROCESSORS = {
    'countries': {
        'accumulator': append_one,
    },
    'latlng': {
        'accumulator': partial(append_list, 2),
        'aggrigator': aggregate_list,
    },
}


class CountryDataAggregator:
    """This is a class for performing aggregation calculation on a country data set
    """

    # data store
    country_data = None
    country_data_expiry = None
    # let's cache aggregation request results
    aggregation_request_results = None

    def get_request_key(self, params):
        """Generate request cache key

        Keyword arguments:
        params -- dictionary of aggregation parameters
        """
        return '%s:%s:%s' % (params.get('aggregation'), params.get('field'), params.get('by'))

    def accumulate_data_sets(self, field, by, accumulator):
        """Collect and group target values in data set

        Keyword arguments:
        field -- value index
        by -- group by index
        accumulator -- custom accumulation function
        """
        accumulation_results = {}
        for data_set in CountryDataAggregator.country_data:
            group = data_set.get(by)
            if not group:
                group = 'null'
            value = data_set.get(field)

            if group not in accumulation_results:
                accumulation_results[group] = []

            if accumulator:
                accumulator(accumulation_results[group], value)
            elif isinstance(value, list):
                accumulation_results[group].append(len(value) if value else 0)
            else:
                accumulation_results[group].append(value if value else 0)

        return accumulation_results

    def aggregate_data_sets(self, accumulation_results, aggregation_method, aggregator):
        """Aggregate collected values based on aggregation type

        Keyword arguments:
        accumulation_results -- dictionary of grouped and collected values
        aggregation_method -- type of aggregation
        aggregator -- custom aggregation function
        """
        aggregation_results = {}

        aggregation_function = AGGREGATION_FUNCTIONS.get(aggregation_method)
        for group, data_set in accumulation_results.items():
            if aggregator:
                aggregation_results[group] = aggregator(aggregation_function, data_set)
            else:
                aggregation_results[group] = round(aggregation_function(data_set), 2)

        return aggregation_results

    def is_expired(self):
        """Check if we need to fetch data from API, no data, or expired data
        """
        if CountryDataAggregator.country_data is None or CountryDataAggregator.country_data_expiry is None:
            return True

        if CountryDataAggregator.country_data_expiry <= datetime.datetime.now():
            return True

        return False

    def store_data(self, data, cache_time):
        """Store data, reset request cache, set expiry

        Keyword arguments:
        data -- country data
        cache_time -- seconds to cache expire
        """
        CountryDataAggregator.country_data = data
        CountryDataAggregator.country_data_expiry = datetime.datetime.now() + datetime.timedelta(seconds=cache_time)
        CountryDataAggregator.aggregation_request_results = {}

    def get_aggregation(self, params):
        """Process aggregation request

        Keyword arguments:
        params -- dictionary of aggregation parameters
        """

        # check cache
        key = self.get_request_key(params)
        if key in CountryDataAggregator.aggregation_request_results:
            return CountryDataAggregator.aggregation_request_results.get(key)

        aggregation_method = params.get('aggregation')
        field = params.get('field')
        by = params.get('by')

        custom_processor = CUSTOM_PROCESSORS.get(field, {})
        accumulation_results = self.accumulate_data_sets(field, by, custom_processor.get('accumulator'))
        aggregation_results = self.aggregate_data_sets(accumulation_results, aggregation_method, custom_processor.get('aggrigator'))

        CountryDataAggregator.aggregation_request_results[key] = aggregation_results

        return aggregation_results
