import json
from unittest.mock import MagicMock

import basin3d
from basin3d.plugins.usgs import USGSMonitoringFeatureAccess
from tests.utilities import get_text, get_json

import pytest

from typing import Iterator

from pydantic import ValidationError

from basin3d.core.models import Base
from basin3d.core.schema.enum import ResultQualityEnum, TimeFrequencyEnum, StatisticEnum
from basin3d.synthesis import register


def get_url(data, status=200):
    """
    Creates a get_url call for mocking with the specified return data
    :param data:
    :param status:
    :return:
    """
    return type('Dummy', (object,), {
        "json": lambda: data,
        "status_code": status,
        "url": "/testurl"})


def get_url_text(text, status=200):
    """
    Creates a get_url_text call for mocking with the specified return data
    :param text:
    :param status:
    :return:
    """

    return type('Dummy', (object,), {
        "text": text,
        "status_code": status,
        "url": "/testurl"})


@pytest.mark.parametrize('additional_query_params',
                         [({"monitoring_feature": ["USGS-09110990", "USGS-09111250"], "observed_property": []}),
                          ({"observed_property": ["RDC"]})],
                         ids=['missing-variables', 'missing-monitoring_features'])
def test_measurement_timeseries_tvp_observations_usgs_errors(additional_query_params, monkeypatch):
    """ Test USGS Timeseries data query"""

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("usgs_mtvp_sites.rdb")),
                                               get_url(get_json("usgs_nwis_dv_p00060_l09110990_l09111250.json"))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query = {
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.DAY,
        "quality": [ResultQualityEnum.VALIDATED],
        **additional_query_params
    }
    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query)


@pytest.mark.parametrize('additional_filters, usgs_response, expected_results',
                         [
                          # all-good
                          ({"monitoring_feature": ["USGS-09110990", "USGS-09111250"], "observed_property": ["RDC"], "result_quality": [ResultQualityEnum.VALIDATED]},
                           "usgs_nwis_dv_p00060_l09110990_l09111250.json", {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED], "count": 2}),
                          # some-quality-filtered-data
                          ({"monitoring_feature": ["USGS-09110990"], "observed_property": ["WT"], "result_quality": [ResultQualityEnum.UNVALIDATED]},
                           "usgs_get_data_09110000_VALIDATED_UNVALIDATED_WT_only.json",
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.UNVALIDATED], "count": 1, "synthesis_msgs": ['09110000 - 00010: 2 timestamps did not match data quality query.']}),
                          # all-data-filtered
                          ({"monitoring_feature": ["USGS-09110990"], "observed_property": ["WT"], "result_quality": [ResultQualityEnum.REJECTED]},
                           "usgs_get_data_09110000_VALIDATED_UNVALIDATED_WT_only.json",
                           {"count": 0, "synthesis_msgs": []})
                         ],
                         ids=['all-good', 'some-quality-filtered-data', 'missing-mapping'])
def test_measurement_timeseries_tvp_observations_usgs(additional_filters, usgs_response, expected_results, monkeypatch):
    """ Test USGS Timeseries data query"""

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("usgs_mtvp_sites.rdb")),
                                               get_url(get_json(usgs_response))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query = {
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.DAY,
        **additional_filters
    }

    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    # loop through generator and serialized the object, get actual object and compare
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            data = json.loads(timeseries.to_json())
            count += 1
            assert data["statistic"]["attr_mapping"]["basin3d_vocab"] == expected_results.get("statistic")
            for idx, result_quality in enumerate(data["result_quality"]):
                assert result_quality["attr_mapping"]["basin3d_vocab"] == expected_results.get("result_quality")[idx]
        assert count == expected_results.get("count")
        if expected_results.get('synthesis_msgs'):
            expected_msgs = expected_results.get('synthesis_msgs')
            msgs = measurement_timeseries_tvp_observations.synthesis_response.messages
            for idx, msg in enumerate(msgs):
                assert msg.msg == expected_msgs[idx]
    else:
        pytest.fail("Returned object must be iterator")


@pytest.mark.parametrize("query, feature_type", [({"id": "USGS-13"}, "region"),
                                                 ({"id": "USGS-0102"}, "subregion"),
                                                 ({"id": "USGS-011000"}, "basin"),
                                                 ({"id": "USGS-01020004"}, "subbasin")],
                         ids=["region", "subregion", "basin", "subbasin"])
def test_usgs_monitoring_feature(query, feature_type, monkeypatch):
    """Test USGS search by region  """

    def mock_get_huc_codes(*args, **kwargs):
        return get_text("new_huc_rdb.txt")

    monkeypatch.setattr(USGSMonitoringFeatureAccess, 'get_hydrological_unit_codes', mock_get_huc_codes)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()


@pytest.mark.parametrize("query, feature_type", [({"id": "USGS-09129600"}, "point")],
                         ids=["point"])
def test_usgs_monitoring_feature2(query, feature_type, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([
        get_url_text(get_text("new_huc_rdb.txt")),
        get_url_text(get_text("usgs_monitoring_feature_query_point_rdb_09129600.rdb"))
    ]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()


@pytest.mark.parametrize("query, expected_count", [({"datasource": "USGS"}, 2889),
                                                   ({"monitoring_feature": ['USGS-02']}, 1),
                                                   ({"feature_type": "region"}, 21),
                                                   ({"feature_type": "subregion"}, 222),
                                                   ({"feature_type": "basin"}, 379),
                                                   ({"feature_type": "subbasin"}, 2267),
                                                   ({"feature_type": "watershed"}, 0),
                                                   ({"feature_type": "subwatershed"}, 0),
                                                   ({"feature_type": "site"}, 0),
                                                   ({"feature_type": "plot"}, 0),
                                                   ({"feature_type": "vertical path"}, 0),
                                                   ({"feature_type": "horizontal path"}, 0),
                                                   ({"parent_feature": ['USGS-02']}, 118),
                                                   ({"parent_feature": ['USGS-0202'], "feature_type": "subbasin"}, 8)],
                         ids=["all", "region_by_id", "region", "subregion", "basin", "subbasin", "watershed", "subwatershed",
                              "site", "plot", "vertical_path", "horizontal_path", "all_by_region", "subbasin_by_subregion"])
def test_usgs_monitoring_features(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("new_huc_rdb.txt"))]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count


@pytest.mark.parametrize("query, expected_count", [({"parent_feature": ['USGS-02020004'], "feature_type": "point"}, 52)],
                         ids=["points_by_subbasin"])
def test_usgs_monitoring_features2(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([
        get_url_text(get_text("usgs_monitoring_features_query_point_02020004.rdb"))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    assert count == expected_count


@pytest.mark.parametrize("query, expected_count", [({"monitoring_feature": ["USGS-09129600"], "feature_type": "point"}, 1)],
                         ids=["point_by_id"])
def test_usgs_monitoring_features3(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([
        get_url_text(get_text("usgs_monitoring_feature_query_point_rdb_09129600.rdb"))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    assert count == expected_count


@pytest.mark.parametrize("query, expected_count", [({"feature_type": "point"}, 0),
                                                   ({"parent_feature": ['USGS-020200'], "feature_type": "point"}, 0)],
                         ids=["point", "invalid_points"])
def test_usgs_monitoring_features_invalid_query(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    response = get_url_text(get_text("invalid_url.txt"), 400)
    mock_get_url = MagicMock(side_effect=list([response]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    # TODO should there be some kind of exception handling for invalid queries that don't return anything?

    response_dict = response.__dict__
    is_400_response = 'HTTP Status 400' in response_dict['text'] and response_dict['status_code'] == 400

    if is_400_response is True:
        count = 0
        assert count == expected_count
