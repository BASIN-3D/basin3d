import logging
from pathlib import Path
import pytest
from typing import Iterator

from basin3d.core.models import MonitoringFeature
from basin3d.core.schema.enum import AggregationDurationEnum, StatisticEnum, ResultQualityEnum, SamplingMediumEnum
from basin3d.synthesis import register


TEST_DATASETS_PATH = str(Path.cwd() / 'tests/resources/essdive_hydrological_monitoring_rf')


@pytest.mark.parametrize("query, expected_result", [({"id": "ESSDIVE-LOCGRP1-Site1"}, (34.0087, -123.456)),
                                                    ({"id": "ESSDIVE-LOCGRP2-Site2"}, (34.0587, -123.856)),
                                                    ({"id": "ESSDIVE-LOCGRP3-LAT34.0087_LON-123.456"}, (34.0087, -123.456)),
                                                    ({"id": "ESSDIVE-LOCGRP5-Site2"}, (34.0587, -123.856)),
                                                    ({"id": "ESSDIVE-LOCGRP4-Site2"}, (34.0587, -123.856)),
                                                    ({"id": "ESSDIVE-LOCGRP6-Site1"}, (34.0087, -123.456)),
                                                    ({"id": "ESSDIVE-LOCGRP7-LAT34.0087_LON-123.456"}, (34.0087, -123.456)),
                                                    ({"id": "ESSDIVE-LOCGRP8-Site1"}, (34.0087, -123.456)),
                                                    ({"id": "ESSDIVE-LOCGRP9-Site2"}, (34.0587, -123.856)),
                                                    ({"id": "ESSDIVE-LOCGRP9-Site3"}, None),
                                                    ({"id": "ESSDIVE-LOCGRPA-Site3"}, None),
                                                    ({"id": "FOO-LOCGRP1-Site1"}, None),
                                                    ],
                         ids=["id-simple", "id-def-2-places", "lat-long-id-grp3", "id-grp5", "id-grp4", "id-grp4", "lat-long-id-grp7", "id-grp8", "id-grp9", "wrong-id", "wrong-dataset", "wrong-plugin"])
def test_essdive_monitoring_feature(query, expected_result, monkeypatch):
    """Test ESSDIVE search by id  """

    monkeypatch.setenv('ESSDIVE_DATASETS_PATH', TEST_DATASETS_PATH)

    synthesizer = register(['basin3d.plugins.essdive.ESSDIVEDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    if expected_result:
        assert monitoring_feature is not None
        assert isinstance(monitoring_feature, MonitoringFeature)
        assert monitoring_feature.id == query["id"]
        assert monitoring_feature.coordinates.absolute.horizontal_position
        horizontal_position = monitoring_feature.coordinates.absolute.horizontal_position
        assert len(horizontal_position) == 1
        expected_lat, expected_long = expected_result
        assert horizontal_position[0].latitude == expected_lat
        assert horizontal_position[0].longitude == expected_long
        # print(f'{monitoring_feature.id} -- {monitoring_feature.description}')
    else:
        assert monitoring_feature is None


def test_essdive_monitoring_features_all(monkeypatch):
    """Test ESSDIVE search for all monitoring features """

    monkeypatch.setenv('ESSDIVE_DATASETS_PATH', TEST_DATASETS_PATH)

    synthesizer = register(['basin3d.plugins.essdive.ESSDIVEDataSourcePlugin'])
    results = synthesizer.monitoring_features()

    assert isinstance(results, Iterator)

    expected_results_details = {'ESSDIVE-LOCGRP1-Site1': {'detail': 'ESSDIVE-LOCGRP1-Site1; Site One; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP1; pid: 0001. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 1.2 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP1-Site2': {'detail': 'ESSDIVE-LOCGRP1-Site2; Site Two; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP1; pid: 0001. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 1.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP2-Site1': {'detail': 'ESSDIVE-LOCGRP2-Site1; Site One; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP2; pid: 0002. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 12.0 meters below ground surface. These values may differ from those reported within the data files.',
                                                          'desc2': 'ESSDIVE dataset: LOCGRP2; pid: 0200. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 12.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP2-Site2': {'detail': 'ESSDIVE-LOCGRP2-Site2; Site Two; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP2; pid: 0002. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 10.0 meters below ground surface. These values may differ from those reported within the data files.',
                                                          'desc2': 'ESSDIVE dataset: LOCGRP2; pid: 0200. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 9.5 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP2-Site3': {'detail': 'ESSDIVE-LOCGRP2-Site3; Site Three; LAT 34.2087; LON -123.256; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP2; pid: 0200. Observations at known depths: 8.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP3-LAT34.0087_LON-123.456': {'detail': 'ESSDIVE-LOCGRP3-LAT34.0087_LON-123.456; LAT34.0087_LON-123.456; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                                           'desc': 'ESSDIVE dataset: LOCGRP3; pid: 0003. Observations at known elevations: 1001.2 None. These values may differ from those reported within the data files. Observations at known depths: -0.2 None. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP3-LAT34.0587_LON-123.856': {'detail': 'ESSDIVE-LOCGRP3-LAT34.0587_LON-123.856; LAT34.0587_LON-123.856; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                                           'desc': 'ESSDIVE dataset: LOCGRP3; pid: 0003. Observations at known elevations: 1001.0 None. These values may differ from those reported within the data files. Observations at known depths: -0.25 None. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP4-Site1': {'detail': 'ESSDIVE-LOCGRP4-Site1; Site1; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP4; pid: 0004.'},
                                'ESSDIVE-LOCGRP4-Site2': {'detail': 'ESSDIVE-LOCGRP4-Site2; Site2; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP4; pid: 0004.'},
                                'ESSDIVE-LOCGRP5-Site1': {'detail': 'ESSDIVE-LOCGRP5-Site1; Site1; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP5; pid: 0005.'},
                                'ESSDIVE-LOCGRP5-Site2': {'detail': 'ESSDIVE-LOCGRP5-Site2; Site2; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP5; pid: 0005.'},
                                'ESSDIVE-LOCGRP6-Site1': {'detail': 'ESSDIVE-LOCGRP6-Site1; Site One; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP6; pid: 0006.'},
                                'ESSDIVE-LOCGRP6-Site2': {'detail': 'ESSDIVE-LOCGRP6-Site2; Site Two; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP6; pid: 0006.'},
                                'ESSDIVE-LOCGRP7-LAT34.0087_LON-123.456': {'detail': 'ESSDIVE-LOCGRP7-LAT34.0087_LON-123.456; LAT34.0087_LON-123.456; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                                           'desc': 'ESSDIVE dataset: LOCGRP7; pid: 007.'},
                                'ESSDIVE-LOCGRP8-Site1': {'detail': 'ESSDIVE-LOCGRP8-Site1; Site One; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP8; pid: 008. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 12.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP8-Site2': {'detail': 'ESSDIVE-LOCGRP8-Site2; Site Two; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP8; pid: 008. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 10.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP9-Site1': {'detail': 'ESSDIVE-LOCGRP9-Site1; Site One; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP9; pid: 009. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 12.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP9-Site2': {'detail': 'ESSDIVE-LOCGRP9-Site2; Site Two; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                          'desc': 'ESSDIVE dataset: LOCGRP9; pid: 009. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 10.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP10-Site1': {'detail': 'ESSDIVE-LOCGRP10-Site1; Site One; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                           'desc': 'ESSDIVE dataset: LOCGRP10; pid: 0010. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 1.2 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP10-Site2': {'detail': 'ESSDIVE-LOCGRP10-Site2; Site Two; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                           'desc': 'ESSDIVE dataset: LOCGRP10; pid: 0010. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 1.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP11-Site2': {'detail': 'ESSDIVE-LOCGRP11-Site2; Site Two; LAT 34.0587; LON -123.856; DEPTH Nope; ELEV Nope',
                                                           'desc': 'ESSDIVE dataset: LOCGRP11; pid: 0011. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 9.5 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP11-Site3': {'detail': 'ESSDIVE-LOCGRP11-Site3; Site Three; LAT 34.2087; LON -123.256; DEPTH Nope; ELEV Nope',
                                                           'desc': 'ESSDIVE dataset: LOCGRP11; pid: 0011. Observations at known depths: 8.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                'ESSDIVE-LOCGRP11-Site1': {'detail': 'ESSDIVE-LOCGRP11-Site1; Site One; LAT 34.0087; LON -123.456; DEPTH Nope; ELEV Nope',
                                                           'desc': 'ESSDIVE dataset: LOCGRP11; pid: 0011. Observations at known elevations: 1000.0 meters above mean sea level (NAVD88). These values may differ from those reported within the data files. Observations at known depths: 12.0 meters below ground surface. These values may differ from those reported within the data files.'},
                                }

    count = 0
    for monitoring_feature in results:
        assert monitoring_feature.id in expected_results_details.keys()
        count += 1

        expected_info = expected_results_details[monitoring_feature.id]

        lat = monitoring_feature.coordinates.absolute.horizontal_position[0].latitude
        long = monitoring_feature.coordinates.absolute.horizontal_position[0].longitude
        alt = 'Nope'
        if monitoring_feature.coordinates.absolute.vertical_extent:
            alt = monitoring_feature.coordinates.absolute.vertical_extent[0].value
        depth = 'Nope'
        if monitoring_feature.coordinates.representative:
            depth = monitoring_feature.coordinates.representative.vertical_position.value
        mf_details = f'{monitoring_feature.id}; {monitoring_feature.name}; LAT {lat}; LON {long}; DEPTH {depth}; ELEV {alt}'
        assert mf_details == expected_info.get('detail')
        if monitoring_feature.id == 'ESSDIVE-LOCGRP2-Site2' or monitoring_feature.id == 'ESSDIVE-LOCGRP2-Site1':
            assert monitoring_feature.description == expected_info.get('desc') or monitoring_feature.description == expected_info.get('desc2')
        else:
            assert monitoring_feature.description == expected_info.get('desc')

    assert count == 23


@pytest.mark.parametrize("query, expected_result, synthesis_msgs",
                         [({"monitoring_feature": ["ESSDIVE-LOCGRP1-Site1"]}, 1, None),
                          ({"monitoring_feature": ["ESSDIVE-LOCGRP1-Site1", "ESSDIVE-LOCGRP1-Site2"]}, 2, None),
                          ({"monitoring_feature": ["ESSDIVE-LOCGRP1-Site1", "ESSDIVE-LOCGRP2-Site2",
                                                   "ESSDIVE-LOCGRP3-LAT34.0587_LON-123.856", "ESSDIVE-LOCGRP7-LAT34.0087_LON-123.456"]}, 4, None),
                          ({"monitoring_feature": ["ESSDIVE-LOCGRP1-Site4", "ESSDIVE-LOC1-Site2"]}, 0, None),
                          ({"parent_feature": ["ESSDIVE-LOCGRP1-Site1"]}, 0, 'Dataset ESSDIVE does not support query by parent_feature'),
                          ],
                         ids=["single-mf", "multiple-mf-same", "mutiple-mf-diff", "invalid", "parent_feature"])
def test_essdive_monitoring_features(query, expected_result, synthesis_msgs, monkeypatch, caplog):
    """Test ESSDIVE search by query """

    monkeypatch.setenv('ESSDIVE_DATASETS_PATH', TEST_DATASETS_PATH)

    synthesizer = register(['basin3d.plugins.essdive.ESSDIVEDataSourcePlugin'])
    results = synthesizer.monitoring_features(**query)

    assert isinstance(results, Iterator)

    caplog.clear()
    with caplog.at_level(logging.INFO):
        count = 0
        for monitoring_feature in results:
            if query.get("monitoring_feature"):
                assert monitoring_feature.id in query.get("monitoring_feature")
                count += 1

    assert count == expected_result

    if synthesis_msgs:
        captured = [rec.message for rec in caplog.records]
        assert synthesis_msgs in captured


@pytest.mark.parametrize("query, expected_result, features_of_interest, number_timesteps, synthesis_msgs",
                         [({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           2, ['ESSDIVE-LOCGRP1-Site1-ELEV1000.0-DEPTH1.2', 'ESSDIVE-LOCGRP1-Site2-ELEV1000.0-DEPTH1.0'], 8, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP2-Site1', 'ESSDIVE-LOCGRP2-Site2', 'ESSDIVE-LOCGRP2-Site3'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           5, ['ESSDIVE-LOCGRP2-Site1-ELEV1000.0-DEPTH0.2', 'ESSDIVE-LOCGRP2-Site1-ELEV1000.0-DEPTH1.2',
                               'ESSDIVE-LOCGRP2-Site2-ELEV1000.0-DEPTH1.0', 'ESSDIVE-LOCGRP2-Site2-ELEV1000.0-DEPTH10.0', 'ESSDIVE-LOCGRP2-Site2-ELEV1000.0-DEPTH9.5',
                               'ESSDIVE-LOCGRP2-Site3-DEPTH8.0'], 8, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP3-LAT34.0087_LON-123.456', 'ESSDIVE-LOCGRP3-LAT34.0587_LON-123.856'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           2, ['ESSDIVE-LOCGRP3-LAT34.0087_LON-123.456-ELEV1001.2-DEPTH-0.2', 'ESSDIVE-LOCGRP3-LAT34.0587_LON-123.856-ELEV1001.0-DEPTH-0.25'], 8, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP4-Site1', 'ESSDIVE-LOCGRP4-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           4, ['ESSDIVE-LOCGRP4-Site1-DEPTH1.2', 'ESSDIVE-LOCGRP4-Site1-DEPTH0.2',
                               'ESSDIVE-LOCGRP4-Site2-DEPTH1.0', 'ESSDIVE-LOCGRP4-Site2-DEPTH0.1'], 4, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP5-Site1', 'ESSDIVE-LOCGRP5-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           2, ['ESSDIVE-LOCGRP5-Site1-ELEV1200.0', 'ESSDIVE-LOCGRP5-Site2-ELEV1000.0'], 8, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP6-Site1', 'ESSDIVE-LOCGRP6-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           3, ['ESSDIVE-LOCGRP6-Site1-DEPTH1.2', 'ESSDIVE-LOCGRP6-Site2-DEPTH1.0'], 8, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP7-LAT34.0087_LON-123.456'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           2, ['ESSDIVE-LOCGRP7-LAT34.0087_LON-123.456'], 8, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP8-Site1', 'ESSDIVE-LOCGRP8-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           3, ['ESSDIVE-LOCGRP8-Site1-ELEV1000.0-DEPTH1.2', 'ESSDIVE-LOCGRP8-Site1-ELEV1000.0-DEPTH0.2', 'ESSDIVE-LOCGRP8-Site2-ELEV1000.0-DEPTH1.0'], 8, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP9-Site1', 'ESSDIVE-LOCGRP9-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           0, [], {}, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP10-Site1', 'ESSDIVE-LOCGRP10-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           6, ['ESSDIVE-LOCGRP10-Site1-ELEV1000.0-DEPTH1.2', 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH0.95', 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH0.97',
                               'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH1.0', 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH1.01', 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH1.02'],
                           {'ESSDIVE-LOCGRP10-Site1-ELEV1000.0-DEPTH1.2': 8, 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH0.95': 2, 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH0.97': 1,
                            'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH1.0': 3, 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH1.01': 1, 'ESSDIVE-LOCGRP10-Site2-ELEV1000.0-DEPTH1.02': 1}, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP11-Site1', 'ESSDIVE-LOCGRP11-Site2', 'ESSDIVE-LOCGRP11-Site3'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           2, ['ESSDIVE-LOCGRP11-Site2-ELEV1000.0-DEPTH9.5', 'ESSDIVE-LOCGRP11-Site3-DEPTH8.0'], 7, None),
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP12-Site1', 'ESSDIVE-LOCGRP12-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           0, [], {}, None),
                          ],
                         ids=['LOCGRP1', 'LOCGRP2', 'LOCGRP3', 'LOCGRP4', 'LOCGRP5', 'LOCGRP6', 'LOCGRP7', 'LOCGRP8', 'LOCGRP9',
                              'LOCGRP10', 'LOCGRP11', 'LOCGRP12'])
def test_essdive_measurement_timeseries_tvp(query, expected_result, synthesis_msgs, features_of_interest, number_timesteps, monkeypatch, caplog):
    """Test ESSDIVE search by query """

    monkeypatch.setenv('ESSDIVE_DATASETS_PATH', TEST_DATASETS_PATH)

    synthesizer = register(['basin3d.plugins.essdive.ESSDIVEDataSourcePlugin'])
    results = synthesizer.measurement_timeseries_tvp_observations(**query)

    assert isinstance(results, Iterator)

    caplog.clear()
    with caplog.at_level(logging.INFO):
        count = 0
        for mtvpo in results:
            assert mtvpo.feature_of_interest.id in features_of_interest
            if isinstance(number_timesteps, int):
                assert len(mtvpo.result.value) == number_timesteps
            elif isinstance(number_timesteps, dict):
                no_ts = number_timesteps.get(mtvpo.feature_of_interest.id)
                assert len(mtvpo.result.value) == no_ts
            count += 1

    assert count == expected_result

    if synthesis_msgs:
        captured = [rec.message for rec in caplog.records]
        assert synthesis_msgs in captured


@pytest.mark.parametrize("query, expected_result, features_of_interest, number_timesteps, synthesis_msgs",
                         [
                          # startdate-no_result: startdate outside range --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2023-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           0, [], {}, None),
                          # enddate-no_result: enddate outside range  --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2021-01-01', 'end_date': '2021-12-31', 'aggregation_duration': AggregationDurationEnum.NONE},
                           0, [], {}, None),
                          # no-mapped-vars: no mapped variables --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           0, [], {}, None),
                          # date-subset: dates within the file range (change file) --> subset
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH'], 'start_date': '2022-01-01', 'end_date': '2022-04-30', 'aggregation_duration': AggregationDurationEnum.NONE},
                           2, ['ESSDIVE-LOCGRP1-Site1-ELEV1000.0-DEPTH1.2', 'ESSDIVE-LOCGRP1-Site2-ELEV1000.0-DEPTH1.0'],
                           {'ESSDIVE-LOCGRP1-Site1-ELEV1000.0-DEPTH1.2': 6, 'ESSDIVE-LOCGRP1-Site2-ELEV1000.0-DEPTH1.0': 3}, None),
                          # variable-subset: one of 2 variables present --> subset
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP2-Site1', 'ESSDIVE-LOCGRP2-Site2', 'ESSDIVE-LOCGRP2-Site3'],
                            'observed_property': ['WT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           2, ['ESSDIVE-LOCGRP2-Site2-ELEV1000.0-DEPTH10.0', 'ESSDIVE-LOCGRP2-Site3-DEPTH8.0', 'ESSDIVE-LOCGRP2-Site2-ELEV1000.0-DEPTH9.5'], 8, None),
                          # invalid-monitoring-features: no valid monitoring features --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site9', 'ESSDIVE-LOCGRP3-LAT44.0087_LON-123.456', 'USGS-09129600'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           0, [], {}, None),
                          # agg_duration_query: different agg_duration --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.DAY},
                           0, [], {}, None),
                          # statistic_query: Statistic --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE, 'statistic': [StatisticEnum.MEAN]},
                           0, [], {}, None),
                          # sampling-medium-water: sampling_medium --> all results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE, 'sampling_medium': [SamplingMediumEnum.WATER]},
                           2, ['ESSDIVE-LOCGRP1-Site1-ELEV1000.0-DEPTH1.2', 'ESSDIVE-LOCGRP1-Site2-ELEV1000.0-DEPTH1.0'], 8, None),
                          # sampling-medium-solid: sampling_medium --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE, 'sampling_medium': [SamplingMediumEnum.SOLID_PHASE]},
                           0, [], {}, None),
                          # quality-query: result quality --> no results
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP1-Site1', 'ESSDIVE-LOCGRP1-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE, 'result_quality': [ResultQualityEnum.VALIDATED]},
                           0, [], {}, None),
                          # subset-site:
                          ({'monitoring_feature': ['ESSDIVE-LOCGRP11-Site1', 'ESSDIVE-LOCGRP11-Site2'],
                            'observed_property': ['PH', 'WT', 'AT'], 'start_date': '2022-01-01', 'aggregation_duration': AggregationDurationEnum.NONE},
                           1, ['ESSDIVE-LOCGRP11-Site2-ELEV1000.0-DEPTH9.5'], 7, None),
                          ],
                         ids=['startdate-no_result', 'enddate-no_result', 'no-mapped-vars', 'date-subset', 'variable-subset',
                              'invalid-monitoring-features', 'agg_duration_query', 'statistic_query', 'sampling-medium-water',
                              'sampling-medium-solid', 'quality-query', 'subset-site'])
def test_essdive_measurement_timeseries_tvp2(query, expected_result, synthesis_msgs, features_of_interest, number_timesteps, monkeypatch, caplog):
    """Test ESSDIVE search by query """

    monkeypatch.setenv('ESSDIVE_DATASETS_PATH', TEST_DATASETS_PATH)

    synthesizer = register(['basin3d.plugins.essdive.ESSDIVEDataSourcePlugin'])
    results = synthesizer.measurement_timeseries_tvp_observations(**query)

    assert isinstance(results, Iterator)

    caplog.clear()
    with caplog.at_level(logging.INFO):
        count = 0
        for mtvpo in results:
            assert mtvpo.feature_of_interest.id in features_of_interest
            if isinstance(number_timesteps, int):
                assert len(mtvpo.result.value) == number_timesteps
            elif isinstance(number_timesteps, dict):
                no_ts = number_timesteps.get(mtvpo.feature_of_interest.id)
                assert len(mtvpo.result.value) == no_ts
            count += 1

    assert count == expected_result

    if synthesis_msgs:
        captured = [rec.message for rec in caplog.records]
        assert synthesis_msgs in captured


def test_unit_handler():
    """
    Some tests for the UnitHandler. This could be expanded.
    """
    from basin3d.plugins.essdive import UnitHandler, HydroRFTerms

    rf_variables = HydroRFTerms.variables.value
    rf_options_str = 'rf_options'
    convert_str = 'convert'

    gage_height = rf_variables.get('Gage_Height')
    gh_rf_units = gage_height.get(rf_options_str)
    gh_convert = gage_height.get(convert_str)

    water_surface_elevation = rf_variables.get('Water_Surface_Elevation')
    wse_rf_units = water_surface_elevation.get(rf_options_str)
    wse_convert = water_surface_elevation.get(convert_str)

    unit_handler = UnitHandler()

    assert unit_handler.match_unit(gh_rf_units, 'm') == 'meters'
    assert unit_handler.match_unit(gh_rf_units, 'cm') == 'centimeters'
    assert unit_handler.match_unit(gh_rf_units, 'foo') is None
    assert unit_handler.match_unit(wse_rf_units, 'meter') == 'meters_above_mean_sea_level_NAVD88'
    assert unit_handler.match_unit(wse_rf_units, 'foo') is None

    assert unit_handler.convert_value('meters', gh_convert) == 1
    assert unit_handler.convert_value('centimeters', gh_convert) == 0.01
    assert unit_handler.convert_value('meter', wse_convert) == 1

    assert unit_handler.convert_value('milli', 'to_micro') == 1000
    assert unit_handler.convert_value('micro', 'to_micro') == 1


def test_parse_data_file_header_rows():
    """
    Test for non-compliant data file headers
    The parser does not check the first header row format.
    """
    from basin3d.plugins.essdive import _parse_data_file_header_rows

    mock_header_rows = ['Skip', 'Bad header']
    assert _parse_data_file_header_rows(mock_header_rows) == {}

    mock_header_rows = ['Skip', '# HeaderRows_Format:', 'Bad Header']
    assert _parse_data_file_header_rows(mock_header_rows) == {}


def test_has_valid_datetime_col():
    """
    Confirm that simple datetime or datetime_start columns exist
    """
    from basin3d.plugins.essdive import _has_valid_datetime_col, HydroRFTerms

    assert _has_valid_datetime_col({}) is False
    assert _has_valid_datetime_col({HydroRFTerms.date_time.value: {}}) is True
    assert _has_valid_datetime_col({HydroRFTerms.date_time_end.value: {}}) is False
    assert _has_valid_datetime_col({HydroRFTerms.date_time_end.value: {},
                                    HydroRFTerms.date_time_start.value: {}}) is True
    assert _has_valid_datetime_col({HydroRFTerms.date_time.value: {},
                                    HydroRFTerms.date_time_start.value: {}}) is False
    assert _has_valid_datetime_col({HydroRFTerms.date_time_start.value: {}}) is True


def test_get_primary_date_time_col():
    """
    """

    from basin3d.plugins.essdive import _get_primary_date_time_col, HydroRFTerms

    assert _get_primary_date_time_col({HydroRFTerms.date_time.value: {}}) == HydroRFTerms.date_time.value
    assert _get_primary_date_time_col({HydroRFTerms.date_time_start.value: {},
                                       HydroRFTerms.date_time_end.value: {}}) == HydroRFTerms.date_time_start.value


def test_hydrorfterms_get_attr_name():
    """
    """
    from basin3d.plugins.essdive import HydroRFTerms

    assert HydroRFTerms.get_attr_name('foo') is None


def test_ess_dive_datasets_handler():
    """
    """
    from basin3d.plugins.essdive import _ess_dive_datasets_handler

    assert _ess_dive_datasets_handler(None) == {}
