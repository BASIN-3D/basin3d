import os
from typing import Iterator

import pytest

from basin3d.core.schema.enum import FeatureTypeEnum
from basin3d.synthesis import register


AMF_USER_NAME = os.environ.get('AMF_USER_NAME')
AMF_USER_EMAIL = os.environ.get('AMF_USER_EMAIL')


# ================================
# TESTS for AMFMonitoringFeatureAccess


@pytest.mark.integration
@pytest.mark.parametrize(
    'query, expected_count',
    [
        pytest.param({},
                     407, id='all-sites'),
        pytest.param({'feature_type': 'POINT'},
                     407, id='all-sites-with-feature-type'),
        pytest.param({'monitoring_feature': ['AMF-US-ZF2', 'AMF-US-PFd', 'AMF-US-notreal', 'BAD-US-ARM']},
                     2, id='named-sites-valid-and-invalid'),
        pytest.param({'monitoring_feature': ['AMF-US-notreal', 'BAD-US-ARM']},
                     0, id='named-sites-invalid'),
        pytest.param({'monitoring_feature': [(-120.3, 29.0, -115.0, 33.0)]},
                     0, id='one-bounding-box-no-fluxnet-years'),
        pytest.param({'monitoring_feature': [(-108.80, 26.98, -108.78, 27.01), (-90.5, 45.0, -90.0, 46.0)]},
                     19, id='mutually-exclusive-bounding-boxes'),
        pytest.param({'monitoring_feature': [(-90.5, 45.2, -90.0, 46.0), (-90.5, 45.0, -90.0, 46.0)]},
                     18, id='overlapping-bounding-boxes'),
        pytest.param({'monitoring_feature': ['AMF-US-ZF2', 'AMF-US-PFd', (-90.5, 45.0, -90.0, 46.0)]},
                     19, id='named-sites-and-bounding-box')])
def test_amf_monitoring_features(query, expected_count):
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    results = list(monitoring_features)

    assert len(results) == expected_count
    assert all(feature.id.startswith('AMF-') for feature in results)
    assert all(feature.feature_type == FeatureTypeEnum.POINT for feature in results)
    assert len({feature.id for feature in results}) == expected_count
    assert monitoring_features.synthesis_response.citations == []


@pytest.mark.integration
@pytest.mark.parametrize(
    'query,expected_message',
    [
        pytest.param({'feature_type': 'BASIN'},"AmeriFlux does not support specified feature type: BASIN. Only feature types ['POINT'] are supported.",
                     id='unsupported-feature-type'),
        pytest.param({'parent_feature': 'AMF-US-TEST'}, 'AmeriFlux does not support filtering monitoring features by parent feature specification.',
                     id='unsupported-parent-feature'),
    ])
def test_amf_monitoring_features_unsupported_metadata(query, expected_message):
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    assert list(monitoring_features) == []
    assert [message.msg for message in monitoring_features.synthesis_response.messages] == [expected_message]


# ================================
# TESTS for AMFMeasurementTimeseriesTVPObservationAccess


@pytest.mark.integration
@pytest.mark.skipif(
    not AMF_USER_NAME or not AMF_USER_EMAIL,
    reason='AMF_USER_NAME and AMF_USER_EMAIL are required for AmeriFlux data requests.',
)
@pytest.mark.parametrize(
    'query, expected_site, expected_results, expected_citation_count',
    [pytest.param({
        'monitoring_feature': ['AMF-US-Me7', 'AMF-US-MEF'],
        'observed_property': ['AT'],
        'start_date': '2023-12-25',
        'end_date': '2023-12-31',
        'aggregation_duration': 'DAY',
    }, ['AMF-US-Me7'], 1, 1, id='me7-daily'),
     pytest.param({
         'monitoring_feature': ['AMF-US-MEF'],
         'observed_property': ['APA'],
         'start_date': '2024-01-01',
         'end_date': '2024-01-15',
         'aggregation_duration': 'MONTH',
     }, ['AMF-US-MEF'], 1, 1, id='mef-month')],
)
def test_amf_measurement_timeseries_tvp_observations(
        query, expected_site, expected_results,expected_citation_count):
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    assert isinstance(observations, Iterator)
    results = list(observations)

    assert results
    assert len(results) == expected_results
    assert all(observation.feature_of_interest.id in expected_site for observation in results)
    assert all(observation.result.value for observation in results)
    assert len(observations.synthesis_response.citations) == expected_citation_count


@pytest.mark.integration
@pytest.mark.skipif(
    not AMF_USER_NAME or not AMF_USER_EMAIL,
    reason='AMF_USER_NAME and AMF_USER_EMAIL are required for AmeriFlux data requests.',
)
def test_amf_measurement_timeseries_tvp_observation_hourly():
    query = {
        'monitoring_feature': ['AMF-US-Me7'],
        'observed_property': ['AT'],
        'start_date': '2023-01-01',
        'end_date': '2023-01-02',
        'aggregation_duration': 'HOUR',
    }
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])

    observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    results = list(observations)

    assert results
    assert len(results) == 1
    assert results[0].feature_of_interest.id == 'AMF-US-Me7'
    assert results[0].result is not None
    assert len(results[0].result.value) == 96
    assert results[0].result.result_quality is not None
    assert len(results[0].result.result_quality) == 96
    assert len(results[0].result.value) == len(results[0].result.result_quality)
    assert all(observation.result.value for observation in results)
    assert all('T' in result.timestamp for observation in results
               for result in observation.result.value)
    assert len(observations.synthesis_response.citations) == 1
