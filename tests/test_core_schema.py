import json

import pytest
from pydantic import ValidationError

from basin3d.core.schema.enum import FeatureTypeEnum, ResultQualityEnum, StatisticEnum, TimeFrequencyEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature


@pytest.mark.parametrize("params, error", [({}, False),
                                           ({"datasource": ["FOO"],
                                             "monitoring_features": []}, False),
                                           ({"datasource": ["FOO"],
                                             "monitoring_features": ['bar', 'base'],
                                             "parent_features": ['moo']}, False),
                                           ({"feature_type": 'POINT'}, False),
                                           ({"feature_type": 'FOO'}, True),
                                           ({"datasource": 'FOO'}, False)],
                         ids=["empty", "valid1", "valid2", "feature-type-valid", "feature-type-invalid",
                              "datasource-invalid"])
def test_query_monitoring_feature(params, error):
    """Test the monitoring feature query dataclass"""
    if not error:
        query = QueryMonitoringFeature(**params)

        for p in ["datasource", "monitoring_features", "parent_features",
                  "feature_type"]:

            if p not in params:
                assert getattr(query, p) is None
            else:
                assert getattr(query, p) == params[p] or getattr(query, p) == [params[p]]
    else:
        pytest.raises(ValidationError, QueryMonitoringFeature, **params)


@pytest.mark.parametrize("params, error", [({}, True),
                                           ({"datasource": ["FOO"],
                                             "monitoring_features": [],
                                             "aggregation_duration": 'MONTH',
                                             "observed_property_variables":['FOO', 'BAR'],
                                             "start_date": "2021-01-01"}, True),
                                           ({"datasource": ["FOO"],
                                             "monitoring_features": [],
                                             "aggregation_duration": 'MONTH',
                                             "start_date": "2021-01-01"}, True),
                                           ({"datasource": ["FOO"],
                                             "monitoring_features": ['bar', 'base'],
                                             "aggregation_duration": 'MONTH',
                                             "observed_property_variables":['FOO', 'BAR'],
                                             "start_date": "2021-01-01"}, False),
                                           ({"aggregation_duration": 'FOO',
                                             "monitoring_features": ['bar', 'base'],
                                             "observed_property_variables":['FOO', 'BAR'],
                                             "start_date": "2021-01-01"}, True),
                                           ({"statistic": ['MEAN'],
                                             "monitoring_features": ['bar', 'base'],
                                             "aggregation_duration": 'MONTH',
                                             "observed_property_variables":['FOO', 'BAR'],
                                             "start_date": "2021-01-01"}, False),
                                           ({"result_quality": 'CHECKED',
                                             "monitoring_features": ['bar', 'base'],
                                             "aggregation_duration": 'MONTH',
                                             "observed_property_variables":['FOO', 'BAR'],
                                             "start_date": "2021-01-01"}, False),
                                           ({"statistic": ['FOO'],
                                             "monitoring_features": ['bar', 'base'],
                                             "aggregation_duration": 'MONTH',
                                             "observed_property_variables":['FOO', 'BAR'],
                                             "start_date": "2021-01-01"}, True),
                                           ({"result_quality": 'BAR',
                                             "monitoring_features": ['bar', 'base'],
                                             "aggregation_duration": 'MONTH',
                                             "observed_property_variables":['FOO', 'BAR'],
                                             "start_date": "2021-01-01"}, True)],
                         ids=["empty-invalid", "monitoring-feature-invalid", "observed-property-variables-missing", "valid1",
                              "aggregation-duration-invalid",
                              "statistic-valid", "result-quality-valid",
                              "statistic-invalid", "result-quality-invalid"])
def test_query_measurement_timeseries_tvp(params, error):
    """Test the measurement timeseries tvp query dataclass"""
    if not error:
        query = QueryMeasurementTimeseriesTVP(**params)

        for p in ["datasource", "aggregation_duration", "monitoring_features", "observed_property_variables",
                  "start_date", "end_date", "statistic", "result_quality"]:

            query_json = json.loads(query.json())
            if p not in params:
                assert query_json[p] is None
            else:
                assert query_json[p] == params[p]

            if 'aggregation_duration' not in query_json.keys():
                assert query_json['aggregation_duration'] == 'DAY'

    else:
        pytest.raises(ValidationError, QueryMeasurementTimeseriesTVP, **params)


@pytest.mark.parametrize("enum", [FeatureTypeEnum, ResultQualityEnum, StatisticEnum, TimeFrequencyEnum])
def test_enumerations(enum):
    """Test the BASIN-3D enumerations"""

    assert enum.names()
    assert enum.values()
