import csv
import json
import os
import pytest

from os.path import dirname
from pydantic import ValidationError
from typing import Iterator, Optional
from unittest.mock import MagicMock

import basin3d
import basin3d.plugins.epa
from basin3d.core.models import Base, MonitoringFeature
from basin3d.core.schema.enum import TimeFrequencyEnum, FeatureTypeEnum
from basin3d.synthesis import register
from tests.utilities import get_text


def get_url_json(data, status=200):
    """
    Creates a get_url call for mocking with the specified return data
    :param data:
    :param status:
    :return:
    """
    return type('Dummy', (object,), {
        "content": data,
        "status_code": status,
        "url": "/testurl"})


def mock_timeout_error(dummyarg, **kwargs):
    raise TimeoutError("mock time_out")


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


def mock_post_wqp(dummyarg1, dummyarg2, dummyarg3):
    return type('Dummy', (object,), {
        # "iter_lines": response_iter_lines,
        "status_code": 200,
        "url": "/testurl"})


def mock_get_url_400(dummyarg, **kwargs):
    return type('Dummy', (object,), {
        # "iter_lines": response_iter_lines,
        "status_code": 400,
        "url": "/testurl"})


@pytest.mark.parametrize("query, expected_msg",
                         [({"feature_type": "region"}, "Feature type REGION not supported by EPA Water Quality eXchange."),
                          ({"feature_type": "point"}, "EPA Water Quality eXchange requires either a parent feature or monitoring feature be specified in the query."),
                          ({"parent_feature": ['EPA-huc'], "monitoring_feature": ["EPA-siteid"]}, "EPA Water Quality eXchange does not support querying monitoring features by both parent_feature (huc) and monitoring_feature (list of ids)."),
                          ({"parent_feature": "EPA-00001"}, "EPA Water Quality eXchange: 00001 does not appear to be a valid USGS huc: 2, 4, 6, 8, 10, or 12-digit code."),
                          ({"parent_feature": "EPA-A001"}, "EPA Water Quality eXchange: A001 does not appear to be a valid USGS huc: 2, 4, 6, 8, 10, or 12-digit code.")],
                         ids=["wrong_feature_type", "no-parent-or-monitoring-feature", "both-parent-and-monitoring-features", "malformed-huc-1", "malformed-huc-2"])
def test_epa_monitoring_features_invalid_query(query, expected_msg, monkeypatch):
    # Test EPA monitoring feature invalid query

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1

    msg = monitoring_features.synthesis_response.messages[0]
    assert msg.msg == expected_msg


@pytest.mark.parametrize("query, resource_file, loc_csv_resource, expected_count",
                         [({"parent_feature": "EPA-1402"}, "epa_1402.json", None, 2418),
                          ({"parent_feature": "EPA-14020001"}, "epa_14020001.json", None, 355),
                          ({"parent_feature": "EPA-1402000101"}, "epa_1402000101.json", None, 36),
                          ({"parent_feature": "EPA-140200010101"}, "epa_140200010101.json", None, 3),
                          ({"monitoring_feature": ['EPA-0801417-CB-AS-1']}, "epa_loc_single.json", None, 1),
                          ({"monitoring_feature": ['EPA-WIDNR_WQX-001', 'EPA-11NPSWRD_WQX-BLCA_NURE_0002', 'EPA-CCWC-MM-29 WASH #3']}, "epa_loc_multiple.json", None, 3),
                          ({"monitoring_feature": ['EPA-WIDNR_WQX-001', 'EPA-invalid']}, "epa_loc_one_good.json", None, 1),
                          ({"monitoring_feature": ['EPA-invalid']}, "epa_loc_empty.json", None, 0),
                          ({"parent_feature": ['EPA-1402001']}, "epa_mock.json", None, 0),
                          ({"feature_type": "region"}, "epa_mock.json", None, 0),
                          ({"feature_type": "subregion"}, "epa_mock.json", None, 0),
                          ({"feature_type": "basin"}, "epa_mock.json", None, 0),
                          ({"feature_type": "subbasin"}, "epa_mock.json", None, 0),
                          ({"feature_type": "watershed"}, "epa_mock.json", None, 0),
                          ({"feature_type": "subwatershed"}, "epa_mock.json", None, 0),
                          ({"feature_type": "site"}, "epa_mock.json", None, 0),
                          ({"feature_type": "plot"}, "epa_mock.json", None, 0),
                          ({"feature_type": "vertical_path"}, "epa_mock.json", None, 0),
                          ({"feature_type": "horizontal_path"}, "epa_mock.json", None, 0),
                          ({"monitoring_feature": [(-106.7, -106.5, 38.5, 39.9)], "feature_type": "point"}, "epa_mock.json", None, 0),
                          ],
                         ids=["huc-wildcard", "huc-8", "huc-10", "huc-12", "single-mf", "multiple-mf", "one-invalid-mf",
                              "mf-invalid", "huc-invalid", "region", "subregion", "basin", "subbasin", "watershed",
                              "subwatershed", "site", "plot", "vertical_path", "horizontal_path", "unsupported-geocoord"])
def test_epa_monitoring_features(query, resource_file, loc_csv_resource, expected_count, monkeypatch):
    """ Test EPA monitoring feature list """

    mock_get_url = MagicMock(side_effect=list([get_url_json(get_text(resource_file))]))
    monkeypatch.setattr(basin3d.plugins.epa, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count


@pytest.mark.parametrize("fail_over_type, query, loc_csv_resource, expected_count, expected_synthesis_messages",
                         [("timeout", {"parent_feature": "EPA-14020001"}, "epa_station14020001.csv", 365,
                           ['WFS Geoserver timed out, fail over to WQP Station request\nError: mock time_out']),
                          ("bad_return", {"parent_feature": "EPA-14020001"}, "epa_station14020001.csv", 365,
                           ['WFS Geoserver did not respond appropriately, fail over to WQP Station request\nError: '
                            'EPA WQX https://www.waterqualitydata.us/ogcservices/wfs/?request=GetFeature&service=wfs&'
                            'version=2.0.0&typeNames=wqp_sites&SEARCHPARAMS=huc%3A14020001%3Bproviders%3ASTORET&output'
                            'Format=application%2Fjson returned error code 400. Trying fail over.']),
                          ],
                         ids=["timeout", "bad_return"])
def test_get_monitoring_features_fail_over_v2_2(fail_over_type, query, loc_csv_resource, expected_count, expected_synthesis_messages, monkeypatch):

    monkeypatch.setattr(basin3d.plugins.epa, 'EPA_WQP_API_VERSION', '2.2')
    if fail_over_type == "timeout":
        monkeypatch.setattr(basin3d.plugins.epa, 'get_url', mock_timeout_error)
    else:
        monkeypatch.setattr(basin3d.plugins.epa, 'get_url', mock_get_url_400)

    def get_csv_dict(dummyvar):
        with open(os.path.join(dirname(__file__), "resources", "epa_v2-2", loc_csv_resource)) as data_file:
            for row in csv.DictReader(data_file):
                yield row

    monkeypatch.setattr(basin3d.plugins.epa, '_get_csv_dict_reader', get_csv_dict)
    monkeypatch.setattr(basin3d.plugins.epa, '_post_wqp_search', mock_post_wqp)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if mf.id == 'EPA-21COL001_WQX-10124E':
            print('******** CHECK EPA-21COL001_WQX-10124E')
            assert mf.coordinates.absolute.horizontal_position[0].x == -106.9859090000
            assert mf.coordinates.absolute.horizontal_position[0].y == 38.8709750000

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count

    synthesis_msgs = [syn_msg.msg for syn_msg in monitoring_features.synthesis_response.messages]

    for expected_synthesis_msg in expected_synthesis_messages:
        assert expected_synthesis_msg in synthesis_msgs


@pytest.mark.parametrize("query, loc_csv_resource, expected_count, expected_synthesis_messages",
                         [({"parent_feature": "EPA-14020001"}, "epa_station14020001.csv", 305,
                           ['WFS Geoserver timed out, fail over to WQP Station request\nError: mock time_out']),
                          ],
                         ids=["huc-14020001"])
def test_get_monitoring_features_fail_over_v3_0(query, loc_csv_resource, expected_count, expected_synthesis_messages, monkeypatch):

    monkeypatch.setattr(basin3d.plugins.epa, 'EPA_WQP_API_VERSION', '3.0')
    monkeypatch.setattr(basin3d.plugins.epa, 'get_url', mock_timeout_error)

    def get_csv_dict(dummyvar):
        with open(os.path.join(dirname(__file__), "resources", "epa_v3-0", loc_csv_resource)) as data_file:
            for row in csv.DictReader(data_file):
                yield row

    monkeypatch.setattr(basin3d.plugins.epa, '_get_csv_dict_reader', get_csv_dict)
    monkeypatch.setattr(basin3d.plugins.epa, '_post_wqp_search', mock_post_wqp)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if mf.id == 'EPA-21COL001_WQX-10124E':
            print('******** CHECK EPA-21COL001_WQX-10124E')
            assert mf.coordinates.absolute.horizontal_position[0].x == -106.9859090000
            assert mf.coordinates.absolute.horizontal_position[0].y == 38.8709750000

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count

    synthesis_msgs = [syn_msg.msg for syn_msg in monitoring_features.synthesis_response.messages]

    for expected_synthesis_msg in expected_synthesis_messages:
        assert expected_synthesis_msg in synthesis_msgs



@pytest.mark.parametrize("query, resource_file, mf_id",
                         [({"id": "EPA-MCHD-88"}, "epa_mchd_88.json", "EPA-MCHD-88"),
                          ({"id": "EPA-invalid"}, "epa_loc_empty.json", None),
                          ({"id": "EPA-MCHD-88"}, "epa_mock.json", None)],
                         ids=["valid-mf", "invalid-mf", "invalid-feature_type"])
def test_epa_monitoring_feature_id(query, resource_file, mf_id, monkeypatch):
    # Test EPA Monitoring Feature search by id

    mock_get_url = MagicMock(side_effect=list([get_url_json(get_text(resource_file))]))
    monkeypatch.setattr(basin3d.plugins.epa, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature: Optional[MonitoringFeature] = response.data

    if mf_id:
        assert monitoring_feature is not None
        assert isinstance(monitoring_feature, Base)
        assert monitoring_feature.id == query["id"]
        assert monitoring_feature.feature_type == FeatureTypeEnum.POINT.value
    else:
        assert monitoring_feature is None


arsenic_site_list = ['EPA-CCWC-COAL-26', 'EPA-CORIVWCH_WQX-650', 'EPA-CCWC-COAL-00', 'EPA-CORIVWCH_WQX-649', 'EPA-CORIVWCH_WQX-159', 'EPA-CORIVWCH_WQX-508',
                     'EPA-CORIVWCH_WQX-258', 'EPA-11NPSWRD_WQX-CURE_381633107054700', 'EPA-CORIVWCH_WQX-7897', 'EPA-CORIVWCH_WQX-646', 'EPA-CORIVWCH_WQX-297',
                     'EPA-CORIVWCH_WQX-392', 'EPA-CCWC-COAL-20', 'EPA-CORIVWCH_WQX-272', 'EPA-CORIVWCH_WQX-875', 'EPA-CORIVWCH_WQX-831', 'EPA-CORIVWCH_WQX-3943',
                     'EPA-CORIVWCH_WQX-644', 'EPA-CORIVWCH_WQX-176', 'EPA-CORIVWCH_WQX-3586', 'EPA-CORIVWCH_WQX-262', 'EPA-CORIVWCH_WQX-972', 'EPA-CCWC-KEY DITCH',
                     'EPA-CORIVWCH_WQX-876', 'EPA-CCWC-KEY-00', 'EPA-CORIVWCH_WQX-260', 'EPA-CORIVWCH_WQX-3584', 'EPA-CORIVWCH_WQX-4134', 'EPA-CORIVWCH_WQX-537',
                     'EPA-CORIVWCH_WQX-624', 'EPA-CORIVWCH_WQX-3580', 'EPA-CORIVWCH_WQX-3188', 'EPA-CORIVWCH_WQX-125', 'EPA-CCWC-COAL-06', 'EPA-CCWC-COAL-11',
                     'EPA-CORIVWCH_WQX-128', 'EPA-CORIVWCH_WQX-4135', 'EPA-CORIVWCH_WQX-126', 'EPA-CCWC-IR-01', 'EPA-CCWC-KEY-02', 'EPA-CORIVWCH_WQX-294',
                     'EPA-CORIVWCH_WQX-127', 'EPA-21COL001_WQX-FCA_BLMS', 'EPA-CORIVWCH_WQX-300', 'EPA-CORIVWCH_WQX-853', 'EPA-CORIVWCH_WQX-395',
                     'EPA-CORIVWCH_WQX-539', 'EPA-CCWC-COAL-12', 'EPA-CORIVWCH_WQX-3582', 'EPA-CORIVWCH_WQX-7898', 'EPA-CORIVWCH_WQX-393', 'EPA-CORIVWCH_WQX-3583',
                     'EPA-CCWC-ELK-00', 'EPA-CCWC-COAL-10', 'EPA-CCWC-COAL-01', 'EPA-CCWC-KEY-01', 'EPA-CCWC-BOG-OPP-2', 'EPA-CCWC-EVANS', 'EPA-CORIVWCH_WQX-269',
                     'EPA-21COL001_WQX-10601A1', 'EPA-CCWC-COAL-15', 'EPA-CCWC-COAL-02', 'EPA-21COL001_WQX-10661A', 'EPA-CCWC-COAL-30', 'EPA-CCWC-SP-00',
                     'EPA-CORIVWCH_WQX-3942', 'EPA-CCWC-EVANS EAST', 'EPA-CORIVWCH_WQX-3941', 'EPA-21COL001_WQX-10290A1', 'EPA-CCWC-AS-2', 'EPA-CORIVWCH_WQX-3944',
                     'EPA-21COL001_WQX-10580A1', 'EPA-21COL001_WQX-10290A2', 'EPA-CCWC-MM-29 WASH #3', 'EPA-CCWC-IR-00', 'EPA-CCWC-MM-29 WASH #1',
                     'EPA-CORIVWCH_WQX-3940', 'EPA-21COL001_WQX-10580A2', 'EPA-CCWC-MM-29 WASH #2', 'EPA-21COL001_WQX-10661B', 'EPA-21COL001_WQX-10601A2']

site_list_test = ['EPA-CCWC-COAL-26', 'EPA-CORIVWCH_WQX-650', 'EPA-CCWC-COAL-00', 'EPA-CORIVWCH_WQX-649', 'EPA-CORIVWCH_WQX-159', 'EPA-CORIVWCH_WQX-508',
                  'EPA-CORIVWCH_WQX-258', 'EPA-11NPSWRD_WQX-CURE_381633107054700', 'EPA-CORIVWCH_WQX-7897', 'EPA-CORIVWCH_WQX-646', 'EPA-CORIVWCH_WQX-297',
                  'EPA-CORIVWCH_WQX-392', 'EPA-CCWC-COAL-20', 'EPA-CORIVWCH_WQX-272', 'EPA-CORIVWCH_WQX-875', 'EPA-CORIVWCH_WQX-831', 'EPA-CORIVWCH_WQX-3943',
                  'EPA-CORIVWCH_WQX-644', 'EPA-CORIVWCH_WQX-176', 'EPA-CORIVWCH_WQX-3586', 'EPA-CORIVWCH_WQX-262', 'EPA-CORIVWCH_WQX-972', 'EPA-CCWC-KEY DITCH',
                  'EPA-CORIVWCH_WQX-876', 'EPA-CCWC-KEY-00', 'EPA-CORIVWCH_WQX-260', 'EPA-CORIVWCH_WQX-3584', 'EPA-CORIVWCH_WQX-4134', 'EPA-CORIVWCH_WQX-537',
                  'EPA-CORIVWCH_WQX-624', 'EPA-CORIVWCH_WQX-3580', 'EPA-CORIVWCH_WQX-3188', 'EPA-CORIVWCH_WQX-125', 'EPA-CCWC-COAL-06', 'EPA-CCWC-COAL-11',
                  'EPA-CORIVWCH_WQX-128', 'EPA-CORIVWCH_WQX-4135', 'EPA-CORIVWCH_WQX-126', 'EPA-CCWC-IR-01', 'EPA-CCWC-KEY-02', 'EPA-CORIVWCH_WQX-294',
                  'EPA-CORIVWCH_WQX-127', 'EPA-21COL001_WQX-FCA_BLMS', 'EPA-CORIVWCH_WQX-300', 'EPA-CORIVWCH_WQX-853', 'EPA-CORIVWCH_WQX-395',
                  'EPA-CORIVWCH_WQX-539', 'EPA-CCWC-COAL-12']


@pytest.mark.parametrize('additional_filters, epa_data_resource, epa_loc_resource, expected_results',
                         [
                             # all-good
                             ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"]},
                              "epa_data1.csv", "epa_data1_locs.json",
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 4,
                               "synthesis_msgs": ["Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                             # all-good-2
                             ({"monitoring_feature": ["EPA-11NPSWRD_WQX-BLCA_09128000", "EPA-11NPSWRD_WQX-CURE_09127000", "11NPSWRD_WQX-CURE_38193410713350"], "observed_property": ["SWL"], "start_date": '2005-01-01', "end_date": "2007-12-31"},
                              "epa_data2_SWL.csv", "epa_data2_locs.json",
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 4}),
                             # filter-stat-no-spec
                             ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"], "statistic": "MEAN"},
                              "epa_data1.csv", "epa_data1_locs.json",
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 0,
                               "synthesis_msgs": ["EPA: No resultPhysChem results matched the query"]}),
                             # filter-result_quality
                             ({"monitoring_feature": arsenic_site_list, "observed_property": ["As"], "result_quality": ["UNVALIDATED", "VALIDATED"]},
                              "epa_data3_arsenic.csv", "epa_data3_locs.json",
                              {"statistic": None, "result_quality": ['UNVALIDATED', 'VALIDATED'], "aggregation_duration": None, "count": 66,
                               "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                             # filter-test-validated
                             ({"monitoring_feature": site_list_test, "observed_property": ["As"], "result_quality": ["VALIDATED"]},
                              "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                              {"statistic": None, "result_quality": ['VALIDATED'], "aggregation_duration": None, "count": 45,
                               "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                             # filter-test-estimated
                             ({"monitoring_feature": site_list_test, "observed_property": ["As"], "result_quality": ["ESTIMATED"]},
                              "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                              {"statistic": None, "result_quality": ['ESTIMATED'], "aggregation_duration": None, "count": 2,
                               "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                             # filter-test-stat-total
                             ({"monitoring_feature": site_list_test, "observed_property": ["As"], "statistic": ["TOTAL"]},
                              "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                              {"statistic": ["TOTAL"], "result_quality": [], "aggregation_duration": None, "count": 1,
                               "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                             # filter-test-stat-multiple
                             ({"monitoring_feature": site_list_test, "observed_property": ["As"], "statistic": ["MIN", "MEAN"]},
                              "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                              {"statistic": ["MIN", "MEAN"], "result_quality": [], "aggregation_duration": None, "count": 3,
                               "synthesis_msgs": []}),
                             # filter-multiple
                             ({"monitoring_feature": site_list_test, "observed_property": ["As"], "statistic": ["MEAN"], "result_quality": ["VALIDATED"]},
                              "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                              {"statistic": ["MEAN"], "result_quality": ["VALIDATED"], "aggregation_duration": None, "count": 2,
                               "synthesis_msgs": []}),
                             # aggregation-day
                             ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"], "aggregation_duration": "DAY"},
                              "epa_data1.csv", "epa_data1_locs.json",
                              {"statistic": None, "result_quality": [], "aggregation_duration": "DAY", "count": 1,
                               "synthesis_msgs": ["Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                             # empty-return
                             ({"monitoring_feature": ["EPA-21COL001-00058"], "observed_property": ["Hg"]},
                              "epa_empty_data.csv", "epa_loc_not_called.json",
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 0,
                               "synthesis_msgs": ["EPA: No resultPhysChem results matched the query"]}),
                             # unsupported-geocoord
                             ({"monitoring_feature": [(-106.7, -106.5, 38.5, 39.9)], "observed_property": ["Hg"]},
                              "epa_empty_data.csv", "epa_loc_not_called.json",
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 0,
                               "synthesis_msgs": ["Data source EPA requires specification of monitoring feature identifier."]}),
                         ],
                         ids=['all-good', 'all-good-2', 'filter-stat-no-spec', 'filter-result_quality', 'filter-test-validated',
                              'filter-test-estimated', 'filter-test-stat-total', 'filter-test-stat-multiple', "filter-multiple",
                              'aggregation-day', 'empty_return', 'unsupported-geocoord'])
def test_measurement_timeseries_tvp_observations_epa_v2_2(additional_filters, epa_data_resource, epa_loc_resource, expected_results, monkeypatch):
    """
    Test EPA Timeseries data query for API version 2.2

    :param additional_filters: additional query parameters
    :param epa_data_resource: resource file containing the EPA data
    :param epa_loc_resource: resource file containing the EPA location data
    :param expected_results: expected results
    :param monkeypatch: pytest fixture

    """

    monkeypatch.setattr(basin3d.plugins.epa, 'EPA_WQP_API_VERSION', '2.2')
    data_sub_dir = 'epa_v2-2'

    def get_csv_dict(dummyvar):
        with open(os.path.join(dirname(__file__), "resources", data_sub_dir, epa_data_resource)) as data_file:
            for row in csv.DictReader(data_file):
                yield row
    monkeypatch.setattr(basin3d.plugins.epa, '_get_csv_dict_reader', get_csv_dict)

    monkeypatch.setattr(basin3d.plugins.epa, '_post_wqp_search', mock_post_wqp)
    mock_get_url = MagicMock(side_effect=list([get_url_json(get_text(epa_loc_resource))]))
    monkeypatch.setattr(basin3d.plugins.epa, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])

    aggregation_duration = TimeFrequencyEnum.NONE
    if "aggregation_duration" in additional_filters.keys():
        aggregation_duration = additional_filters.get("aggregation_duration")

    start_date = additional_filters.get("start_date", "2010-01-01")
    end_date = additional_filters.get("end_date", "2011-01-01")

    query = {
        "start_date": start_date,
        "end_date": end_date,
        "aggregation_duration": aggregation_duration,
        **additional_filters
    }

    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    # loop through generator and serialized the object, get actual object and compare
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            data = json.loads(timeseries.to_json())
            count += 1
            print(timeseries.id)
            if expected_results.get("statistic"):
                assert data["statistic"]["attr_mapping"]["basin3d_vocab"] in expected_results.get("statistic")

            if expected_results.get("result_quality"):
                for idx, result_quality in enumerate(data["result_quality"]):
                    assert result_quality["attr_mapping"]["basin3d_vocab"] in expected_results.get("result_quality")

            if expected_results.get("aggregation_duration"):
                assert data["aggregation_duration"]["attr_mapping"]["basin3d_vocab"] == expected_results.get("aggregation_duration")

        assert count == expected_results.get("count")
        # if expected_results.get('synthesis_msgs'):
        expected_msgs = expected_results.get('synthesis_msgs')
        msgs = measurement_timeseries_tvp_observations.synthesis_response.messages
        for msg in msgs:
            if expected_msgs and 'EPA: No resultPhysChem results matched the query' in expected_msgs:
                assert 'EPA: No resultPhysChem results matched the query' in msg.msg
            else:
                assert msg.msg in expected_msgs
            print(msg.msg)
        if not expected_msgs:
            assert msgs == []
    else:
        pytest.fail("Returned object must be iterator")


@pytest.mark.parametrize('additional_filters, epa_data_resource, epa_loc_resource, expected_results',
                         [
                          # all-good
                          ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"]},
                           "epa_data1.csv", "epa_data1_locs.json",
                           {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 5,
                            "synthesis_msgs": ["Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                          # all-good-2
                          ({"monitoring_feature": ["EPA-11NPSWRD_WQX-BLCA_09128000", "EPA-11NPSWRD_WQX-CURE_09127000", "EPA-11NPSWRD_WQX-CURE_38193410713350"], "observed_property": ["SWL"], "start_date": '2005-01-01', "end_date": "2007-12-31"},
                           "epa_data2_SWL.csv", "epa_data2_locs.json",
                           {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 4}),
                          # filter-stat-no-spec
                          ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"], "statistic": "MEAN"},
                           "epa_data1.csv", "epa_data1_locs.json",
                           {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 0,
                            "synthesis_msgs": ["EPA: No resultPhysChem results matched the query"]}),
                          # filter-result_quality
                          ({"monitoring_feature": arsenic_site_list, "observed_property": ["As"], "result_quality": ["UNVALIDATED", "VALIDATED"]},
                           "epa_data3_arsenic.csv", "epa_data3_locs.json",
                           {"statistic": None, "result_quality": ['UNVALIDATED', 'VALIDATED'], "aggregation_duration": None, "count": 66,
                            "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                          # filter-test-validated
                          ({"monitoring_feature": site_list_test, "observed_property": ["As"], "result_quality": ["VALIDATED"]},
                           "epa_data3_arsenic.csv", "epa_data4_locs.json",
                           {"statistic": None, "result_quality": ['VALIDATED'], "aggregation_duration": None, "count": 44,
                            "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                           # filter-test-estimated
                           ({"monitoring_feature": site_list_test, "observed_property": ["As"], "result_quality": ["ESTIMATED"]},
                            "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                            {"statistic": None, "result_quality": ['ESTIMATED'], "aggregation_duration": None, "count": 2,
                             "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                           # filter-test-stat-total
                           ({"monitoring_feature": site_list_test, "observed_property": ["As"], "statistic": ["TOTAL"]},
                            "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                            {"statistic": ["TOTAL"], "result_quality": [], "aggregation_duration": None, "count": 1,
                             "synthesis_msgs": ["Could not parse expected numerical measurement value <5.00", "Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                           # filter-test-stat-multiple
                           ({"monitoring_feature": site_list_test, "observed_property": ["As"], "statistic": ["MIN", "MEAN"]},
                            "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                            {"statistic": ["MIN", "MEAN"], "result_quality": [], "aggregation_duration": None, "count": 4,
                             "synthesis_msgs": []}),
                           # filter-multiple
                           ({"monitoring_feature": site_list_test, "observed_property": ["As"], "statistic": ["MEAN"], "result_quality": ["VALIDATED"]},
                            "epa_data4_arsenic_test.csv", "epa_data4_locs.json",
                            {"statistic": ["MEAN"], "result_quality": ["VALIDATED"], "aggregation_duration": None, "count": 2,
                             "synthesis_msgs": []}),
                          # aggregation-day
                          ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"], "aggregation_duration": "DAY"},
                           "epa_data1.csv", "epa_data1_locs.json",
                           {"statistic": None, "result_quality": [], "aggregation_duration": "DAY", "count": 1,
                            "synthesis_msgs": ["Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                          # filter-stat-no-spec
                          ({"monitoring_feature": ["EPA-21COL001-00058"], "observed_property": ["Hg"]},
                           "epa_empty_data.csv", "epa_loc_not_called.json",
                           {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 0,
                            "synthesis_msgs": ["EPA: No resultPhysChem results matched the query"]}),
                         ],
                         ids=['all-good', 'all-good-2', 'filter-stat-no-spec', 'filter-result_quality', 'filter-test-validated',
                              'filter-test-estimated', 'filter-test-stat-total', 'filter-test-stat-multiple', "filter-multiple",
                              'aggregation-day', 'empty_return'])
def test_measurement_timeseries_tvp_observations_epa_v3_0(additional_filters, epa_data_resource, epa_loc_resource, expected_results, monkeypatch):
    """
    Test EPA Timeseries data query for API version 3.0

    :param additional_filters: additional query parameters
    :param epa_data_resource: resource file containing the EPA data
    :param epa_loc_resource: resource file containing the EPA location data
    :param expected_results: expected results
    :param monkeypatch: pytest fixture

    data1 query: https://www.waterqualitydata.us/beta/#siteid=CCWC-COAL-26&siteid=CCWC-MM-29%20WASH%20%233&characteristicName=Arsenic&characteristicName=Arsenic%2C%20Inorganic&characteristicName=Dissolved%20oxygen%20(DO)&characteristicName=Temperature%2C%20water&mimeType=csv&dataProfile=fullPhysChem&providers=STORET
    modifications to test aggregation_duration and temperature basis.

    data2 query: https://www.waterqualitydata.us/beta/#siteid=11NPSWRD_WQX-BLCA_09128000&siteid=11NPSWRD_WQX-CURE_09127000&siteid=11NPSWRD_WQX-CURE_381934107133500&characteristicName=Height%2C%20gage&characteristicName=Stream%20water%20level&startDateLo=01-01-2005&startDateHi=12-31-2007&mimeType=csv&dataProfile=fullPhysChem&providers=STORET

    data3 query: https://www.waterqualitydata.us/beta/#huc=1402*&characteristicName=Arsenic&characteristicName=Arsenic%2C%20Inorganic&startDateLo=01-01-2010&startDateHi=12-31-2010&mimeType=csv&dataProfile=fullPhysChem&providers=STORET

    data4 query: same as data3 but with modifications to test statistic and result quality.

    """

    monkeypatch.setattr(basin3d.plugins.epa, 'EPA_WQP_API_VERSION', '3.0')
    data_sub_dir = 'epa_v3-0'

    def get_csv_dict(dummyvar):
        with open(os.path.join(dirname(__file__), "resources", data_sub_dir, epa_data_resource)) as data_file:
            for row in csv.DictReader(data_file):
                yield row
    monkeypatch.setattr(basin3d.plugins.epa, '_get_csv_dict_reader', get_csv_dict)

    monkeypatch.setattr(basin3d.plugins.epa, '_post_wqp_search', mock_post_wqp)
    mock_get_url = MagicMock(side_effect=list([get_url_json(get_text(epa_loc_resource))]))
    monkeypatch.setattr(basin3d.plugins.epa, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])

    aggregation_duration = TimeFrequencyEnum.NONE
    if "aggregation_duration" in additional_filters.keys():
        aggregation_duration = additional_filters.get("aggregation_duration")

    start_date = additional_filters.get("start_date", "2010-01-01")
    end_date = additional_filters.get("end_date", "2011-01-01")

    query = {
        "start_date": start_date,
        "end_date": end_date,
        "aggregation_duration": aggregation_duration,
        **additional_filters
    }

    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    # loop through generator and serialized the object, get actual object and compare
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            data = json.loads(timeseries.to_json())
            count += 1
            print(timeseries.id)
            if expected_results.get("statistic"):
                assert data["statistic"]["attr_mapping"]["basin3d_vocab"] in expected_results.get("statistic")

            if expected_results.get("result_quality"):
                for idx, result_quality in enumerate(data["result_quality"]):
                    assert result_quality["attr_mapping"]["basin3d_vocab"] in expected_results.get("result_quality")

            if expected_results.get("aggregation_duration"):
                assert data["aggregation_duration"]["attr_mapping"]["basin3d_vocab"] == expected_results.get("aggregation_duration")

        assert count == expected_results.get("count")
        # if expected_results.get('synthesis_msgs'):
        expected_msgs = expected_results.get('synthesis_msgs')
        msgs = measurement_timeseries_tvp_observations.synthesis_response.messages
        for msg in msgs:
            if expected_msgs and 'EPA: No resultPhysChem results matched the query' in expected_msgs:
                assert 'EPA: No resultPhysChem results matched the query' in msg.msg
            else:
                assert msg.msg in expected_msgs
            print(msg.msg)
        if not expected_msgs:
            assert msgs == []
    else:
        pytest.fail("Returned object must be iterator")


@pytest.mark.parametrize('additional_query_params',
                         [({"monitoring_feature": ["EPA-WIDNR_WQX-001"], "observed_property": []}),
                          ({"observed_property": ["RDC"]})],
                         ids=['missing-variables', 'missing-monitoring_features'])
def test_measurement_timeseries_tvp_observations_epa_errors(additional_query_params, monkeypatch):
    # Test EPA Timeseries data query

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])

    query = {
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.NONE,
        **additional_query_params
    }
    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query)


def test_measurement_timeseries_tvp_observation_bad_response(monkeypatch):

    def mock_post_wqp(dummyarg):
        return type('Dummy', (object,), {
            "status_code": 400,
            "url": "/testurl"})
    monkeypatch.setattr(basin3d.plugins.epa, '_post_wqp_search', mock_post_wqp)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])

    query = {
        "start_date": "2020-04-01",
        "aggregation_duration": TimeFrequencyEnum.NONE,
        "observation_property": ['DO'],
        "monitoring_feature": ['EPA-test']
    }
    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query)
