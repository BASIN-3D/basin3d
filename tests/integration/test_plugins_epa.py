import json
import pytest

from pydantic import ValidationError
from typing import Iterator, Optional

import basin3d
import basin3d.plugins.epa
from basin3d.core.models import Base, MonitoringFeature
from basin3d.core.schema.enum import TimeFrequencyEnum, FeatureTypeEnum
from basin3d.synthesis import register


@pytest.mark.integration
@pytest.mark.parametrize("query, expected_count",
                         [({"parent_feature": "EPA-1402"}, 2488),
                          ({"parent_feature": "EPA-14020001"}, 365),
                          ({"parent_feature": "EPA-1402000101"}, 40),
                          ({"parent_feature": "EPA-140200010101"}, 3),
                          ({"monitoring_feature": ['EPA-0801417-CB-AS-1']}, 1),
                          ({"monitoring_feature": ['EPA-WIDNR_WQX-001', 'EPA-11NPSWRD_WQX-BLCA_NURE_0002', 'EPA-CCWC-MM-29 WASH #3']}, 3),
                          ({"monitoring_feature": ['EPA-WIDNR_WQX-001', 'EPA-invalid']}, 1),
                          ({"monitoring_feature": ['EPA-invalid']}, 0),
                          ({"parent_feature": ['EPA-1402001']}, 0)
                          ],
                         ids=["huc-wildcard", "huc-8", "huc-10", "huc-12", "single-mf", "multiple-mf", "one-invalid-mf",
                              "mf-invalid", "huc-invalid"])
def test_epa_monitoring_features(query, expected_count):
    """ Test EPA monitoring feature list """

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


@pytest.mark.integration
@pytest.mark.parametrize("query, mf_id",
                         [({"id": "EPA-MCHD-88"}, "EPA-MCHD-88"),
                          ({"id": "EPA-invalid"}, None)],
                         ids=["valid-mf", "invalid-mf"])
def test_epa_monitoring_feature_id(query, mf_id):
    # Test EPA Monitoring Feature search by id

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


@pytest.mark.integration
@pytest.mark.parametrize('additional_filters, expected_results',
                         [
                             # all-good
                             ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"]},
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 5,
                               "synthesis_msgs": ["Could not parse expected numerical measurement value <0.500", "Could not parse expected numerical measurement value <2.50"]}),
                             # all-good-2
                             ({"monitoring_feature": ["EPA-11NPSWRD_WQX-BLCA_09128000", "EPA-11NPSWRD_WQX-CURE_09127000", "11NPSWRD_WQX-CURE_38193410713350"], "observed_property": ["SWL"], "start_date": '2005-01-01', "end_date": "2007-12-31"},
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 4}),
                             # filter-stat-no-spec
                             ({"monitoring_feature": ["EPA-CCWC-COAL-26", "EPA-CCWC-MM-29 WASH #3"], "observed_property": ["As", "DO", "WT"], "statistic": "MEAN"},
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 0,
                               "synthesis_msgs": ["EPA: No resultPhysChem results matched the query"]}),
                             # empty-return
                             ({"monitoring_feature": ["EPA-21COL001-00058"], "observed_property": ["Hg"]},
                              {"statistic": None, "result_quality": [], "aggregation_duration": None, "count": 0,
                               "synthesis_msgs": ["EPA: No resultPhysChem results matched the query"]}),
                         ],
                         ids=['all-good', 'all-good-2', 'filter-stat-no-spec', 'empty_return'])
def test_measurement_timeseries_tvp_observations_epa_v2_2(additional_filters, expected_results, monkeypatch):
    """
    Test EPA Timeseries data query for API version 2.2

    :param additional_filters: additional query parameters
    :param epa_data_resource: resource file containing the EPA data
    :param epa_loc_resource: resource file containing the EPA location data
    :param expected_results: expected results
    :param monkeypatch: pytest fixture

    """

    monkeypatch.setattr(basin3d.plugins.epa, 'EPA_WQP_API_VERSION', '2.2')

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
                if 'WFS Geoserver did not respond' in msg.msg:
                    # ignore the fail over if present
                    continue
                assert msg.msg in expected_msgs
            print(msg.msg)
        if not expected_msgs:
            if msgs:
                assert len(msgs) == 1 and 'WFS Geoserver did not respond' in msgs[0].msg
            else:
                assert msgs == []
    else:
        pytest.fail("Returned object must be iterator")

@pytest.mark.integration
def test_measurement_timeseries_tvp_observation_bad_response(monkeypatch):

    # ToDo: mock a invalid request
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
