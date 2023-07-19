import json

import pytest
from pydantic import ValidationError

from basin3d.core.schema.enum import FeatureTypeEnum, ResultQualityEnum, StatisticEnum, TimeFrequencyEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature


@pytest.mark.parametrize("params, error", [({}, False),
                                           ({"datasource": ["FOO"],
                                             "monitoring_feature": []}, False),
                                           ({"datasource": ["FOO"],
                                             "monitoring_feature": ['bar', 'base'],
                                             "parent_feature": ['moo']}, False),
                                           ({"feature_type": 'POINT'}, False),
                                           ({"feature_type": 'FOO'}, True),
                                           ({"datasource": 'FOO'}, False),
                                           ({"id": 'foo'}, False),
                                           ({"id": ['foo']}, True)],
                         ids=["empty", "valid1", "valid2", "feature-type-valid", "feature-type-invalid",
                              "datasource-invalid", "id-valid", "id-invalid"])
def test_query_monitoring_feature(params, error):
    """Test the monitoring feature query dataclass"""
    if not error:
        query = QueryMonitoringFeature(**params)

        for p in ["datasource", "monitoring_feature", "parent_feature",
                  "feature_type", "id"]:

            if p not in params:
                assert getattr(query, p) is None
            else:
                assert getattr(query, p) == params[p] or getattr(query, p) == [params[p]]
    else:
        pytest.raises(ValidationError, QueryMonitoringFeature, **params)


@pytest.mark.parametrize(
    "params, error", [
                      # empty-invalid
                      ({}, True),
                      # monitoring-feature-invalid
                      ({"datasource": ["FOO"],
                        "monitoring_feature": [],
                        "aggregation_duration": 'MONTH',
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, True),
                      # observed-property-missing
                      ({"datasource": ["FOO"],
                        "monitoring_feature": [],
                        "aggregation_duration": 'MONTH',
                        "start_date": "2021-01-01"}, True),
                      # valid1
                      ({"datasource": ["FOO"],
                        "monitoring_feature": ['bar', 'base'],
                        "aggregation_duration": 'MONTH',
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, False),
                      # aggregation-duration-invalid
                      ({"aggregation_duration": 'FOO',
                        "monitoring_feature": ['bar', 'base'],
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, True),
                      # aggregation-duration-valid-none
                      ({"aggregation_duration": None,
                        "monitoring_feature": ['bar', 'base'],
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, False),
                      # statistic-valid
                      ({"statistic": ['MEAN'],
                        "monitoring_feature": ['bar', 'base'],
                        "aggregation_duration": 'MONTH',
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, False),
                      # result-quality-valid
                      ({"result_quality": ['VALIDATED'],
                        "monitoring_feature": ['bar', 'base'],
                        "aggregation_duration": 'MONTH',
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, False),
                      # statistic-invalid
                      ({"statistic": ['FOO'],
                        "monitoring_feature": ['bar', 'base'],
                        "aggregation_duration": 'MONTH',
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, True),
                      # result-quality-invalid
                      ({"result_quality": ['BAR'],
                        "monitoring_feature": ['bar', 'base'],
                        "aggregation_duration": 'MONTH',
                        "observed_property":['FOO', 'BAR'],
                        "start_date": "2021-01-01"}, True)],
    ids=["empty-invalid", "monitoring-feature-invalid", "observed-property-missing", "valid1",
         "aggregation-duration-invalid", "aggregation-duration-valid-none",
         "statistic-valid", "result-quality-valid", "statistic-invalid", "result-quality-invalid"])
def test_query_measurement_timeseries_tvp(params, error):
    """Test the measurement timeseries tvp query dataclass"""
    if not error:
        query = QueryMeasurementTimeseriesTVP(**params)

        for p in ["datasource", "aggregation_duration", "monitoring_feature", "observed_property",
                  "start_date", "end_date", "statistic", "result_quality", "id"]:

            query_json = json.loads(query.json())
            if p not in params:
                assert query_json[p] is None
            else:
                if p == "aggregation_duration" and params[p] is None:
                    params[p] = 'DAY'
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


# ToDo: test_set_mapped_attribute_enum_type
