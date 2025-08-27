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
                          ({"monitoring_feature": [(4, 2, 2)], "observed_property": ["RDC"]}),
                          ({"monitoring_feature": [(4, 2, 2, 3)], "observed_property": ["RDC"]}),
                          ({"observed_property": ["RDC"]})],
                         ids=['missing-variables', 'malformed_mf', 'malformed_bbox-mf', 'missing-monitoring_features'])
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


@pytest.mark.parametrize('additional_filters, usgs_response, usgs_resources2, expected_results',
                         [
                          # all-good-mf-single-bbox
                          ({"monitoring_feature": [(-106.9, 38.65, -106.8, 38.67)], "observed_property": ["RDC"], "result_quality": [ResultQualityEnum.VALIDATED], "start_date": "2023-04-01", "end_date": "2023-04-10"},
                           ["usgs_mtvp_bbox1.rdb", "usgs_get_data_bbox1.json"], [],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED, ResultQualityEnum.VALIDATED], "mvp_count": 2, "result_count": [10, 10], "missing_values_count": [0, 0]}),
                          # all-good-mf-multi-bbox
                          ({"monitoring_feature": [(-106.9, 38.65, -106.8, 38.67), (-106.7, 38.9, -106.5, 39.0)], "observed_property": ["RDC"], "start_date": "2024-04-01", "end_date": "2024-04-10"},
                           ["usgs_mtvp_bbox1.rdb", "usgs_get_data_bbox1_202404.json"], ["usgs_mf_query_point_single_bbox.rdb", "usgs_get_data_bbox2_202404.json"],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED, ResultQualityEnum.VALIDATED, ResultQualityEnum.UNVALIDATED], "mvp_count": 3, "result_count": [10, 10, 10], "missing_values_count": [0, 0, 0]}),
                          # all-good-mf-full-overlap
                          ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0), "USGS-09106800"], "observed_property": ["RDC"], "start_date": "2024-04-01", "end_date": "2024-04-10"},
                           ["usgs_monitoring_feature_query_mix_overlap_09106800.rdb", "usgs_get_data_bbox2_202404.json"], ["usgs_mf_query_point_single_bbox.rdb", "usgs_get_data_bbox2_202404.json"],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.UNVALIDATED], "mvp_count": 1, "result_count": [10], "missing_values_count": [0]}),
                          # all-good-mf-strings
                          ({"monitoring_feature": ["USGS-09110990", "USGS-09111250"], "observed_property": ["RDC"], "result_quality": [ResultQualityEnum.VALIDATED], "start_date": "2020-04-01", "end_date": "2020-04-30"},
                            ["usgs_mtvp_sites.rdb", "usgs_nwis_dv_p00060_l09110990_l09111250.json"], [],
                            {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED, ResultQualityEnum.VALIDATED], "mvp_count": 2, "result_count": [30, 30], "missing_values_count": [0, 0]}),
                          # some-quality-filtered-data
                          ({"monitoring_feature": ["USGS-09110990"], "observed_property": ["WT"], "result_quality": [ResultQualityEnum.UNVALIDATED], "start_date": "2020-04-01", "end_date": "2020-04-30"},
                           ["usgs_mtvp_sites.rdb", "usgs_get_data_09110000_VALIDATED_UNVALIDATED_WT_only.json"], [],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.UNVALIDATED], "mvp_count": 1, "result_count": [2], "missing_values_count": [0],
                            "synthesis_msgs": ['09110000 - 00010: 2 timestamps did not match data quality query.']}),
                          # all-data-filtered
                          ({"monitoring_feature": ["USGS-09110990"], "observed_property": ["WT"], "result_quality": [ResultQualityEnum.REJECTED], "start_date": "2020-04-01", "end_date": "2020-04-30"},
                           ["usgs_mtvp_sites.rdb", "usgs_get_data_09110000_VALIDATED_UNVALIDATED_WT_only.json"], [],
                           {"mvp_count": 0, "result_count": [0], "missing_values_count": [0], "synthesis_msgs": []}),
                          # all-data-filtered
                          ({"monitoring_feature": ["USGS-09110990"], "observed_property": ["RDC"], "start_date": "2023-04-01", "end_date": "2023-04-10"},
                           ["usgs_mtvp_sites.rdb", "usgs_get_data_09110000_missing_vals.json"], [],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.UNVALIDATED], "mvp_count": 1, "result_count": [10], "missing_values_count": [7],
                            "synthesis_msgs": []}),
                         ],
                         ids=['all-good-mf-single-bbox', 'all-good-mf-multi-bbox', 'all-good-mf-full-overlap',
                              'all-good-mf-strings', 'some-quality-filtered-data', 'missing-mapping', 'missing-values'])
def test_measurement_timeseries_tvp_observations_usgs(additional_filters, usgs_response, usgs_resources2, expected_results, monkeypatch):
    """ Test USGS Timeseries data query"""

    resources = [get_url_text(get_text(usgs_response[0])), get_url(get_json(usgs_response[1]))]
    if usgs_resources2:
        resources = [get_url_text(get_text(usgs_response[0])), get_url_text(get_text(usgs_resources2[0])),
                     get_url(get_json(usgs_response[1])), get_url(get_json(usgs_resources2[1]))]

    mock_get_url = MagicMock(side_effect=list(resources))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query = {
        "aggregation_duration": TimeFrequencyEnum.DAY,
        **additional_filters
    }

    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    # loop through generator and serialized the object, get actual object and compare
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        mvp_count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            data = json.loads(timeseries.to_json())
            assert data["statistic"]["attr_mapping"]["basin3d_vocab"] == expected_results.get("statistic")
            for result_quality in data["result_quality"]:
                assert result_quality["attr_mapping"]["basin3d_vocab"] == expected_results.get("result_quality")[mvp_count]
            result_count = 0
            missing_value_count = 0
            for result_value in data["result"]["value"]:
                result_count += 1
                if result_value[1] == -999999:
                    missing_value_count += 1
            assert result_count == expected_results.get("result_count")[mvp_count]
            assert missing_value_count == expected_results.get("missing_values_count")[mvp_count]
            mvp_count += 1
        assert mvp_count == expected_results.get("mvp_count")
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
                                                   ({"feature_type": "vertical_path"}, 0),
                                                   ({"feature_type": "horizontal_path"}, 0),
                                                   ({"parent_feature": ['USGS-02']}, 118),
                                                   ({"parent_feature": ['USGS-0202'], "feature_type": "subbasin"}, 8)],
                         ids=["all", "region_by_id", "region", "subregion", "basin", "subbasin", "watershed", "subwatershed",
                              "site", "plot", "vertical_path", "horizontal_path", "all_by_region", "subbasin_by_subregion"])
def test_usgs_monitoring_features(query, expected_count):
    """Test USGS non-point features"""

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


@pytest.mark.parametrize("query, expected_count", [({"parent_feature": ['USGS-02020004'], "feature_type": "point"}, 54)],
                         ids=["points_by_subbasin"])
def test_usgs_monitoring_features2(query, expected_count, monkeypatch):
    """Test USGS points by subbasin"""

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


@pytest.mark.parametrize("query, resource, resource2, expected_count, expected_site_set",
                         [({"monitoring_feature": ["USGS-09129600"], "feature_type": "point"}, "usgs_monitoring_feature_query_point_rdb_09129600.rdb", [], 1, ("USGS-09129600", )),
                          ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0)], "feature_type": "point"}, "usgs_mf_query_point_single_bbox.rdb", [], 1, ("USGS-09106800", )),  # bbox with single return
                          ({"monitoring_feature": [(-106.7, 38.5, -106.5, 39.9)], "feature_type": "point"}, "usgs_mf_query_point_single_bbox_many_sites.rdb", [], 57, None),  # bbox with multiple returns
                          ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0), (-106.7, 38.5, -106.5, 39.0)], "feature_type": "point"},
                            "usgs_mf_query_point_single_bbox.rdb", ["usgs_mf_query_point_2_bbox_overlap.rdb"], 8, None),  # bbox with complete overlap
                          ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0), "USGS-09129600"], "feature_type": "point"},
                            "usgs_monitoring_feature_query_point_rdb_09129600.rdb", ["usgs_mf_query_point_single_bbox.rdb"], 2, ("USGS-09106800", "USGS-09129600")),  # bbox and string mix
                          ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0), "USGS-09106800"], "feature_type": "point"},
                           "usgs_monitoring_feature_query_mix_overlap_09106800.rdb", ["usgs_mf_query_point_single_bbox.rdb"], 1, ("USGS-09106800", )),  # bbox and string overlap
                          ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0), "USGS-09106800", "USGS-09110000", (-90.6, 34.4, -90.5, 34.6), "USGS-09107000"], "feature_type": "point"},
                           "usgs_mf_query_point_3_strings.rdb", ["usgs_mf_query_point_single_bbox.rdb", "usgs_mf_query_point_single_bbox2.rdb"],
                           5, ("USGS-09106800", "USGS-09107000", "USGS-09110000", "USGS-07047970", "USGS-07287700")),  # bboxs and strs with overlap
                          ({"monitoring_feature": [(-106.71, 38.58, -106.7, 39.59)], "feature_type": "point"}, "usgs_mf_query_bbox_empty.rdb", [], 0, None),  # bbox with no returns
                          ({"datasource": "FOO", "monitoring_feature": [(-106.7, 38.5, -106.5, 39.9)], "feature_type": "point"}, "usgs_mf_query_bbox_empty.rdb", [], 0, None),  # bbox and wrong datasource
                          ],
                         ids=["point_by_id", "single_bbox_one_site", "single_bbox_many_sites", "2_bbox_overlap", "mix", "mix_overlap", "mix_many_overlap", "empty", "wrong-datasource"])
def test_usgs_monitoring_features3(query, resource, resource2, expected_count, expected_site_set, monkeypatch):
    """Test USGS point by id"""

    resources = [get_url_text(get_text(resource))]
    if resource2:
        for rr in resource2:
            resources.append(get_url_text(get_text(rr)))

    mock_get_url = MagicMock(side_effect=list(resources))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0
    site_set = set()

    for mf in monitoring_features:
        count += 1
        site_set.add(mf.id)
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    assert count == expected_count
    if expected_site_set:
        assert site_set == set(expected_site_set)


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
