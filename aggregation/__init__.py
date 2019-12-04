from flask import Flask


from aggregation.views.aggregations_api import AggregationsAPI


def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')

    aggregations_api = AggregationsAPI.as_view('aggregations')
    app.add_url_rule('/aggregated-data', view_func=aggregations_api, methods=["GET"])

    return app

app = create_app()
