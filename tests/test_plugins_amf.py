import json
import re
from copy import deepcopy
from pathlib import Path
from pydantic import ValidationError
from unittest.mock import Mock

import pytest

from basin3d.core.models import Observation
from basin3d.core.schema.enum import FeatureTypeEnum, ResultQualityEnum
from basin3d.synthesis import register


RESOURCE_DIR = Path(__file__).parent / 'resources'
AMF_METADATA_RESOURCE = 'amf_site_info_display_AmeriFlux.json'
AMF_DOWNLOAD_RESOURCE = 'amf_data_download_response.json'
AMF_ZIP_RESOURCES = {
    'US-Me7': 'AMF_US-Me7_FLUXNET_2022-2023_v1.3_r1.zip',
    'US-MEF': 'AMF_US-MEF_FLUXNET_2024-2025_v1.3_r1.zip',
    'US-Ro5': 'AMF_US-Ro5_FLUXNET_2017-2024_v1.3_r1.zip',
}
AMF_ZIP_CHECKSUMS = {
    'US-Me7': 'ce3cfad68bc133a1d71a1b6e95be8514',
    'US-MEF': '40dda3a99795184e13bd97b0cbef9a9b',
}
AMF_UTC_OFFSETS = {'AMF-US-Me7': -8.0, 'AMF-US-MEF': -7.0}


# ================================
# SHARED TEST HELPERS


def load_resource(resource_name):
    with (RESOURCE_DIR / resource_name).open() as resource:
        return json.load(resource)


def fluxnet_site_ids():
    metadata = load_resource(AMF_METADATA_RESOURCE)
    return sorted(
        f'AMF-{site["site_id"]}' for site in metadata['values']
        if site.get('grp_publish_fluxnet')
        and site.get('grp_location', {}).get('location_lat') is not None
        and site.get('grp_location', {}).get('location_long') is not None)


def json_response(data, status_code=200):
    response = Mock(status_code=status_code)
    response.json.return_value = data
    return response


def streaming_response(resource_name):
    response = Mock(status_code=200, text='')
    response.iter_content.return_value = [(RESOURCE_DIR / resource_name).read_bytes()]
    return response


def amf_download_response(site_ids):
    response = load_resource(AMF_DOWNLOAD_RESOURCE)
    response['data_urls'] = [
        {'site_id': site_id,
         'url': f'https://amf.amf/FLUXNET/AMF_{site_id}.zip',
         'download_checksum': AMF_ZIP_CHECKSUMS[site_id],
         'data_product': 'FLUXNET'}
        for site_id in site_ids]
    return response


def register_amf_plugin():
    return register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])


def configure_amf_measurement_access(monkeypatch, tmp_path):
    monkeypatch.setattr('basin3d.plugins.ameriflux.AMF_USER_NAME', 'test-user')
    monkeypatch.setattr('basin3d.plugins.ameriflux.AMF_USER_EMAIL', 'test@amf.amf')
    monkeypatch.setattr('basin3d.plugins.ameriflux.LOCAL_TEMP_DIR', str(tmp_path))


def synthesis_message_texts(access_iterator):
    return [message.msg for message in access_iterator.synthesis_response.messages]


# ================================
# TESTS for AMFMonitoringFeatureAccess


@pytest.mark.parametrize(
    'query, expected_count, expected_site_ids',
    [pytest.param({}, 407, fluxnet_site_ids(), id='all-sites'),
     pytest.param({'feature_type': 'POINT'}, 407, fluxnet_site_ids(),
                  id='all-sites-with-feature-type'),
     pytest.param(
         {'monitoring_feature': [
             'AMF-US-ZF2', 'AMF-US-PFd', 'AMF-US-notreal', 'BAD-US-ARM']},
         2, ['AMF-US-PFd', 'AMF-US-ZF2'], id='named-sites-valid-and-invalid'),
     pytest.param({'monitoring_feature': ['AMF-US-notreal', 'BAD-US-ARM']},
                  0, [], id='named-sites-invalid'),
     pytest.param(
         {'monitoring_feature': [(-120.3, 29.0, -115.0, 33.0)]},
         0, [], id='one-bounding-box-no-fluxnet-years'),
     pytest.param(
         {'monitoring_feature': [
             (-108.80, 26.98, -108.78, 27.01),
             (-90.5, 45.0, -90.0, 46.0)]},
         19,
         ['AMF-MX-Aog', 'AMF-US-PFL', 'AMF-US-PFb', 'AMF-US-PFc',
          'AMF-US-PFd', 'AMF-US-PFe', 'AMF-US-PFf', 'AMF-US-PFg',
          'AMF-US-PFh', 'AMF-US-PFi', 'AMF-US-PFj', 'AMF-US-PFk',
          'AMF-US-PFm', 'AMF-US-PFn', 'AMF-US-PFp', 'AMF-US-PFq',
          'AMF-US-PFr', 'AMF-US-PFt', 'AMF-US-WCr'],
         id='mutually-exclusive-bounding-boxes'),
     pytest.param(
         {'monitoring_feature': [
             (-90.5, 45.2, -90.0, 46.0),
             (-90.5, 45.0, -90.0, 46.0)]},
         18,
         ['AMF-US-PFL', 'AMF-US-PFb', 'AMF-US-PFc', 'AMF-US-PFd',
          'AMF-US-PFe', 'AMF-US-PFf', 'AMF-US-PFg', 'AMF-US-PFh',
          'AMF-US-PFi', 'AMF-US-PFj', 'AMF-US-PFk', 'AMF-US-PFm',
          'AMF-US-PFn', 'AMF-US-PFp', 'AMF-US-PFq', 'AMF-US-PFr',
          'AMF-US-PFt', 'AMF-US-WCr'],
         id='overlapping-bounding-boxes'),
     pytest.param(
         {'monitoring_feature': [
             'AMF-US-ZF2', 'AMF-US-PFd', (-90.5, 45.0, -90.0, 46.0)]},
         19,
         ['AMF-US-ZF2', 'AMF-US-PFL', 'AMF-US-PFb', 'AMF-US-PFc',
          'AMF-US-PFd', 'AMF-US-PFe', 'AMF-US-PFf', 'AMF-US-PFg',
          'AMF-US-PFh', 'AMF-US-PFi', 'AMF-US-PFj', 'AMF-US-PFk',
          'AMF-US-PFm', 'AMF-US-PFn', 'AMF-US-PFp', 'AMF-US-PFq',
          'AMF-US-PFr', 'AMF-US-PFt', 'AMF-US-WCr'],
         id='named-sites-and-bounding-box')],
)
def test_monitoring_features(query, expected_count, expected_site_ids, monkeypatch):
    monkeypatch.setattr(
        'basin3d.plugins.ameriflux.get_url',
        lambda *args, **kwargs: json_response(load_resource(AMF_METADATA_RESOURCE)))
    synthesizer = register_amf_plugin()
    monitoring_features = synthesizer.monitoring_features(**query)

    results = list(monitoring_features)
    actual_site_ids = sorted(feature.id for feature in results)

    assert len(results) == expected_count
    assert actual_site_ids == sorted(expected_site_ids)
    assert all(feature.id.startswith('AMF-') for feature in results)
    assert all(feature.feature_type == FeatureTypeEnum.POINT for feature in results)
    assert len(actual_site_ids) == len(set(actual_site_ids))


@pytest.mark.parametrize(
    'query',
    [pytest.param({'monitoring_feature': [(1, 2, 3)]}, id='malformed-bbox'),
     pytest.param({'feature_type': 'not-a-feature-type'}, id='invalid-feature-type')])
def test_monitoring_features_errors(query):
    """Exercise unsupported feature types and malformed monitoring features."""
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])

    with pytest.raises(ValidationError):
        synthesizer.monitoring_features(**query)


@pytest.mark.parametrize(
    'query,expected_message',
    [
        pytest.param({'feature_type': 'BASIN'},"AmeriFlux does not support specified feature type: BASIN. Only feature types ['POINT'] are supported.",
                     id='unsupported-feature-type'),
        pytest.param({'parent_feature': 'AMF-US-TEST'}, 'AmeriFlux does not support filtering monitoring features by parent feature specification.',
                     id='unsupported-parent-feature'),
    ])
def test_monitoring_features_metadata_failure(query, expected_message):
    """Exercise metadata request and empty metadata failures."""
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    assert list(monitoring_features) == []
    assert [message.msg for message in monitoring_features.synthesis_response.messages] == [expected_message]


# ================================
# TESTS for AMFMeasurementTimeseriesTVPObservationAccess


@pytest.mark.parametrize(
    'aggregation_duration, start_date, end_date, timestamp_pattern, expected_timestamps, expected_values, expected_apa',
    [pytest.param('DAY', '2023-12-25', '2024-01-15', r'\d{4}-\d{2}-\d{2}',
                  ['2023-12-25', '2023-12-26', '2023-12-27'], [0.325, 2.588, 2.24],
                  {'AMF-US-Me7': (['2023-12-25', '2023-12-26', '2023-12-27'],
                                  [657.954, 656.259, 652.771]),
                   'AMF-US-MEF': (['2024-01-01', '2024-01-02', '2024-01-03'],
                                  [574.225, 574.082, 571.517])},
                  id='daily-two-sites-two-properties'),
     pytest.param('MONTH', '2023-11-25', '2024-01-15', r'\d{4}-\d{2}',
                  ['2023-12'], [2.392],
                  {'AMF-US-Me7': (['2023-12'], [656.034]),
                   'AMF-US-MEF': (['2024-01'], [569.334])},
                  id='monthly-two-sites-two-properties')])
def test_measurement_timeseries_tvp_observation(
        aggregation_duration, start_date, end_date, timestamp_pattern,
        expected_timestamps, expected_values, expected_apa, monkeypatch, tmp_path):
    """Exercise two sites, TA, APA, and the supported calendar resolutions."""
    site_ids = ['US-Me7', 'US-MEF']
    configure_amf_measurement_access(monkeypatch, tmp_path)

    def get_response(url, stream=False):
        if stream:
            site_id = url.rsplit('/', 1)[-1].split('.')[0].removeprefix('AMF_')
            return streaming_response(AMF_ZIP_RESOURCES[site_id])
        return json_response(load_resource(AMF_METADATA_RESOURCE))

    monkeypatch.setattr('basin3d.plugins.ameriflux.get_url', get_response)
    monkeypatch.setattr(
        'basin3d.plugins.ameriflux.post_url',
        lambda *args, **kwargs: json_response(amf_download_response(site_ids)))

    synthesizer = register_amf_plugin()
    observations = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_feature=[f'AMF-{site_id}' for site_id in site_ids],
        observed_property=['AT', 'APA'],
        start_date=start_date,
        end_date=end_date,
        aggregation_duration=aggregation_duration)
    results = list(observations)

    assert len(results) == 4
    assert set(observation.feature_of_interest.id for observation in results) == {
        'AMF-US-Me7', 'AMF-US-MEF'}
    for site_id in ('AMF-US-Me7', 'AMF-US-MEF'):
        site_results = [result for result in results
                        if result.feature_of_interest.id == site_id]
        assert [result.observed_property.get_basin3d_vocab() for result in site_results].count('AT') == 1
        assert [result.observed_property.get_basin3d_vocab() for result in site_results].count('APA') == 1
        assert all(result.result.value for result in site_results)
        assert next(result for result in site_results
                    if result.observed_property.get_basin3d_vocab() == 'AT').unit_of_measurement == 'C'
        assert next(result for result in site_results
                    if result.observed_property.get_basin3d_vocab() == 'APA').unit_of_measurement == 'mm Hg'
        assert all(result.utc_offset == AMF_UTC_OFFSETS[site_id] for result in site_results)
        assert all(re.fullmatch(timestamp_pattern, pair.timestamp)
                   for result in site_results for pair in result.result.value)

    ta_result = next(result for result in results
                     if result.feature_of_interest.id == 'AMF-US-Me7'
                     and result.observed_property.get_basin3d_vocab() == 'AT')
    assert [pair.timestamp for pair in ta_result.result.value[:len(expected_timestamps)]] == expected_timestamps
    assert [pair.value for pair in ta_result.result.value[:len(expected_values)]] == pytest.approx(expected_values)
    for site_id, (apa_timestamps, apa_values) in expected_apa.items():
        apa_result = next(result for result in results
                          if result.feature_of_interest.id == site_id
                          and result.observed_property.get_basin3d_vocab() == 'APA')
        assert [pair.timestamp for pair in apa_result.result.value[:len(apa_timestamps)]] == apa_timestamps
        assert [pair.value for pair in apa_result.result.value[:len(apa_values)]] == pytest.approx(apa_values)
    assert all('No data variable found for PA at' in msg for msg in synthesis_message_texts(observations))


@pytest.mark.parametrize(
    'result_quality, start_date, end_date, expected_observation_count, expected_value_count, expected_quality, expected_filtered_count',
    [pytest.param([ResultQualityEnum.ESTIMATED], '2022-06-16', '2022-06-17', 1, 25,
                  ['ESTIMATED', 'ESTIMATED'], 0, id='quality-filtered'),
     pytest.param(None, '2022-01-01', '2022-01-02', 1, 96,
                  'ESTIMATED', 0, id='quality-not-specified'),
     pytest.param([ResultQualityEnum.VALIDATED], '2022-06-16', '2022-06-16', 1, 23,
                  'VALIDATED', 25, id='quality-partially-filtered'),
     pytest.param(None, '2022-06-16', '2022-06-16', 1, 48,
                  ['VALIDATED', 'ESTIMATED', 'ESTIMATED'], 0, id='quality-mix-no-filter'),
     pytest.param([ResultQualityEnum.VALIDATED], '2022-01-01', '2022-01-02', 0, 0,
                  None, 96, id='quality-no-match')])
def test_measurement_timeseries_tvp_observation_hourly_quality(
        result_quality, start_date, end_date, expected_observation_count,
        expected_value_count, expected_quality, expected_filtered_count,
        monkeypatch, tmp_path):
    """Exercise HH timestamps, UTC offsets, and QC filtering."""
    site_ids = ['US-Me7']
    configure_amf_measurement_access(monkeypatch, tmp_path)

    def get_response(url, stream=False):
        if stream:
            return streaming_response(AMF_ZIP_RESOURCES['US-Me7'])
        return json_response(load_resource(AMF_METADATA_RESOURCE))

    monkeypatch.setattr('basin3d.plugins.ameriflux.get_url', get_response)
    monkeypatch.setattr(
        'basin3d.plugins.ameriflux.post_url',
        lambda *args, **kwargs: json_response(amf_download_response(site_ids)))

    synthesizer = register_amf_plugin()
    query = {
        'monitoring_feature': ['AMF-US-Me7'],
        'observed_property': ['AT'],
        'start_date': start_date,
        'end_date': end_date,
        'aggregation_duration': 'HOUR',
    }
    if result_quality is not None:
        query['result_quality'] = result_quality
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert len(results) == expected_observation_count
    messages = synthesis_message_texts(observations)
    filtered_message = (f'Filtered {expected_filtered_count} values for US-Me7 variable TA_F '
                        'based on the result quality query.')
    if expected_filtered_count:
        assert filtered_message in messages
    else:
        assert filtered_message not in messages

    if not results:
        return

    observation = results[0]
    assert observation.feature_of_interest.id == 'AMF-US-Me7'
    assert observation.observed_property.get_basin3d_vocab() == 'AT'
    assert observation.unit_of_measurement == 'C'
    assert observation.utc_offset == -8.0
    assert len(observation.result.value) == expected_value_count
    assert all(re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}-\d{2}:\d{2}', pair.timestamp)
               for pair in observation.result.value)
    assert all(pair.timestamp.endswith('-08:00') for pair in observation.result.value)
    if isinstance(expected_quality, list):
        assert [quality.get_basin3d_vocab() for quality in observation.result_quality] == expected_quality
        assert set(quality.get_basin3d_vocab() for quality in observation.result.result_quality) == set(expected_quality)
    else:
        assert [quality.get_basin3d_vocab() for quality in observation.result_quality] == [expected_quality]
        assert all(quality.get_basin3d_vocab() == expected_quality
                   for quality in observation.result.result_quality)


@pytest.mark.parametrize(
    'observed_property, source_prefix, expected_unit, expected_message',
    [pytest.param('STM', 'TS_F_MDS', 'C', None, id='indexed-soil-temperature'),
     pytest.param('SMO', 'SWC_F_MDS', '%',
                  'Unit for SWC_F_MDS was unexpected and unit conversion to BASIN-3D unit could not be assessed. '
                  'Returning values in AMF native unit %.', id='indexed-soil-water-content')])
def test_measurement_timeseries_tvp_observation_indexed_variables(
        observed_property, source_prefix, expected_unit, expected_message,
        monkeypatch, tmp_path):
    """Exercise all indexed TS_F_MDS and SWC_F_MDS variables."""
    site_ids = ['US-MEF']
    configure_amf_measurement_access(monkeypatch, tmp_path)

    def get_response(url, stream=False):
        if stream:
            return streaming_response(AMF_ZIP_RESOURCES['US-MEF'])
        return json_response(load_resource(AMF_METADATA_RESOURCE))

    monkeypatch.setattr('basin3d.plugins.ameriflux.get_url', get_response)
    monkeypatch.setattr(
        'basin3d.plugins.ameriflux.post_url',
        lambda *args, **kwargs: json_response(amf_download_response(site_ids)))

    synthesizer = register_amf_plugin()
    observations = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_feature=['AMF-US-MEF'],
        observed_property=[observed_property],
        start_date='2024-01-01',
        end_date='2024-01-01',
        aggregation_duration='HOUR')
    results = list(observations)

    expected_variables = [f'{source_prefix}_{index}' for index in range(1, 4)]
    assert len(results) == len(expected_variables)
    assert sorted(result.id.rsplit('--', 1)[-1] for result in results) == expected_variables
    assert all(result.feature_of_interest.id == 'AMF-US-MEF' for result in results)
    assert all(result.observed_property.get_basin3d_vocab() == observed_property
               for result in results)
    assert all(result.unit_of_measurement == expected_unit for result in results)
    assert all(result.utc_offset == -7.0 for result in results)
    assert all(result.result.value for result in results)
    assert all(result.result_quality for result in results)
    assert all(re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}-\d{2}:\d{2}', pair.timestamp)
               for result in results for pair in result.result.value)
    assert all('_QC' not in result.id for result in results)
    messages = synthesis_message_texts(observations)
    if expected_message:
        assert messages == [expected_message] * len(expected_variables)
    else:
        assert not messages


def test_measurement_timeseries_tvp_observation_errors():
    """Exercise credentials, download, checksum, and temporary directory failures."""
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(
            monitoring_feature=['AMF-US-Me7'],
            observed_property=[],
            start_date='2023-01-01',
            end_date='2023-01-02',
            aggregation_duration='DAY')

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(
            monitoring_feature=[(1, 2, 3)],
            observed_property=['AT'],
            start_date='2023-01-01',
            end_date='2023-01-02',
            aggregation_duration='DAY')

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(
            monitoring_feature=['AMF-US-Me7'],
            observed_property=['AT'],
            end_date='2023-01-02',
            aggregation_duration='DAY')


def test_measurement_timeseries_tvp_observation_invalid_metadata():
    """Exercise invalid metadata specifications"""
    synthesizer = register(['basin3d.plugins.ameriflux.AMFDataSourcePlugin'])

    mvp = synthesizer.measurement_timeseries_tvp_observations(
              monitoring_feature=['AMF-US-Me7'],
              observed_property=['RDC'],
              start_date='2023-01-01',
              end_date='2023-01-02',
              aggregation_duration='DAY')
    assert list(mvp) == []

    # Unsupported aggregation duration.
    mvp = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_feature=['AMF-US-Me7'],
        observed_property=['AT'],
        start_date='2023-01-01',
        end_date='2023-01-02',
        aggregation_duration='MINUTE')
    assert list(mvp) == []

    # Unsupported statistic.
    mvp = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_feature=['AMF-US-Me7'],
        observed_property=['AT'],
        start_date='2023-01-01',
        statistic='MIN',
        aggregation_duration='DAY')
    assert list(mvp) == []

    # Monitoring feature does not exist.
    mvp = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_feature=['BAD-US-Me7'],
        observed_property=['AT'],
        start_date='2023-01-01',
        end_date='2023-01-02',
        aggregation_duration='HOUR')
    assert list(mvp) == []
