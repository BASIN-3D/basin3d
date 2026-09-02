import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import xarray as xr
from pydantic import ValidationError

from basin3d.core.schema.enum import FeatureTypeEnum, ResultQualityEnum, TimeFrequencyEnum
from basin3d.synthesis import register


RESOURCE_DIR = Path(__file__).parent / 'resources'
ARM_METADATA_RESOURCE = 'arm_metadata_dataset_metb1.json'

# The query URL uses ARM's ordering of a bbox: west, north, east, south.
ARM_BBOX_METADATA_RESOURCES = {
    (-98.7, 35.85, -95.5, 37.0): (
        'https://metadata-api.svcs.arm.gov/metadata/data_product?'
        'data_product=met&page_from=0&page_size=100&bbox=-98.7%2C37.0%2C-95.5%2C35.85',
        'arm_metadata_bbox_1.json',
    ),
    (-98.7, 35.85, -97.5, 36.3): (
        'https://metadata-api.svcs.arm.gov/metadata/data_product?'
        'data_product=met&page_from=0&page_size=100&bbox=-98.7%2C36.3%2C-97.5%2C35.85',
        'arm_metadata_bbox_2a.json',
    ),
    (-97.5, 36.3, -95.5, 37.0): (
        'https://metadata-api.svcs.arm.gov/metadata/data_product?'
        'data_product=met&page_from=0&page_size=100&bbox=-97.5%2C37.0%2C-95.5%2C36.3',
        'arm_metadata_bbox_2b.json',
    ),
    (-106.71, 39.58, -106.7, 39.59): (
        'https://metadata-api.svcs.arm.gov/metadata/data_product?'
        'data_product=met&page_from=0&page_size=100&bbox=-106.71%2C39.59%2C-106.7%2C39.58',
        'arm_metadata_bbox_empty.json',
    ),
}


def load_resource(resource_name):
    with (RESOURCE_DIR / resource_name).open() as resource:
        return json.load(resource)


def json_response(data):
    response = Mock(status_code=200)
    response.json.return_value = data
    return response


def streaming_response(resource_name):
    response = Mock(status_code=200, text='')
    response.iter_content.return_value = [(RESOURCE_DIR / resource_name).read_bytes()]
    return response


@pytest.mark.parametrize(
    'additional_query_params',
    [pytest.param({'monitoring_feature': ['ARM-SGP-E12', 'ARM-SGP-E15'], 'observed_property': []},
                  id='missing-observed-properties'),
     pytest.param({'observed_property': ['AT']},
                  id='missing-monitoring-features'),
     pytest.param({'monitoring_feature': [(1, 2, 3)], 'observed_property': ['AT']},
                  id='malformed-bbox-monitoring-feature')],
)
def test_measurement_timeseries_tvp_observation_errors(additional_query_params):
    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    query = {
        'start_date': '2020-04-01',
        'end_date': '2020-04-30',
        'aggregation_duration': TimeFrequencyEnum.MINUTE,
        'result_quality': [ResultQualityEnum.VALIDATED],
        **additional_query_params,
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query)


def mock_named_measurement_endpoints(tmp_path, monkeypatch):
    metadata = load_resource(ARM_METADATA_RESOURCE)
    e15_data_query = load_resource('arm_query_1.json')
    e12_data_query = load_resource('arm_query_empty_result.json')
    metadata_and_query_urls = []

    def mock_get_url(url, *args, **kwargs):
        metadata_and_query_urls.append(url)
        if 'metadata/data_product' in url:
            return json_response(metadata)
        if 'sgpmetE15.b1' in url:
            return json_response(e15_data_query)
        if 'sgpmetE12.b1' in url:
            return json_response(e12_data_query)
        raise AssertionError(f'Unexpected ARM endpoint: {url}')

    e15_files = sorted(e15_data_query['files'])
    expected_file_batches = [e15_files[:30], e15_files[30:]]
    mod_get = Mock(side_effect=[
        streaming_response('arm_data1a_sgpmetE15.b1.20230401-20230430.cdf'),
        streaming_response('arm_data1b_sgpmetE15.b1.20230501-20230504.cdf'),
    ])

    monkeypatch.setattr('basin3d.plugins.arm.get_url', mock_get_url)
    monkeypatch.setattr('basin3d.plugins.arm.requests.get', mod_get)
    monkeypatch.setattr('basin3d.plugins.arm.ARM_NAME', 'test-user')
    monkeypatch.setattr('basin3d.plugins.arm.ARM_TOKEN', 'test-token')
    monkeypatch.setattr('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(tmp_path))


    return metadata_and_query_urls, expected_file_batches, mod_get


@pytest.mark.parametrize(
    'query, source_variable, expected_unit',
    [pytest.param({'monitoring_feature': ['ARM-SGP-E12', 'ARM-SGP-E15'], 'observed_property': ['AT'],
                   'start_date': '2023-04-01', 'end_date': '2023-05-04', 'aggregation_duration': 'MINUTE'},
                  'temp_mean', 'C', id='named-sites-air-temperature'),
     pytest.param({'monitoring_feature': ['ARM-SGP-E12', 'ARM-SGP-E15'], 'observed_property': ['W_SPD'],
                   'start_date': '2023-04-01', 'end_date': '2023-05-04', 'aggregation_duration': 'MINUTE',
                   'statistic': ['MEAN'], 'result_quality': ['VALIDATED']},
                  'wspd_arith_mean', 'm/s', id='named-sites-wind-speed-mean')],
)
def test_measurement_timeseries_tvp_observation_named_sites(
        query, source_variable, expected_unit, tmp_path, monkeypatch):

    metadata_and_query_urls, expected_file_batches, mod_get = mock_named_measurement_endpoints(
        tmp_path, monkeypatch)

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert len(results) == 1
    observation = results[0]
    assert observation.feature_of_interest.id == 'ARM-SGP-E15'
    assert observation.unit_of_measurement == expected_unit
    assert len(observation.result.value) == 48955

    with xr.open_dataset(RESOURCE_DIR / 'arm_data1a_sgpmetE15.b1.20230401-20230430.cdf') as april_data, \
            xr.open_dataset(RESOURCE_DIR / 'arm_data1b_sgpmetE15.b1.20230501-20230504.cdf') as may_data:
        expected_first_values = april_data[source_variable].values[:3].tolist()
        expected_last_values = may_data[source_variable].values[-3:].tolist()

    actual_values = [result.value for result in observation.result.value]
    assert actual_values[:3] == pytest.approx(expected_first_values)
    assert actual_values[-3:] == pytest.approx(expected_last_values)
    assert [quality.get_basin3d_vocab() for quality in observation.result_quality] == ['VALIDATED']

    assert any('sgpmetE12.b1' in url for url in metadata_and_query_urls)
    assert any('sgpmetE15.b1' in url for url in metadata_and_query_urls)
    assert len(mod_get.call_args_list) == 2
    assert [call.kwargs['json'] for call in mod_get.call_args_list] == expected_file_batches

    expected_message = 'SGP-E12 does not have any files that match the query parameters. Skipping'
    actual_messages = [message.msg for message in observations.synthesis_response.messages]
    assert actual_messages.count(expected_message) == 1


def mock_no_result_measurement_endpoints(tmp_path, monkeypatch, data_query_resources):
    metadata = load_resource(ARM_METADATA_RESOURCE)
    metadata_and_query_urls = []

    def mock_get_url(url, *args, **kwargs):
        metadata_and_query_urls.append(url)
        if 'metadata/data_product' in url:
            return json_response(metadata)
        for dataset_name, resource_name in data_query_resources.items():
            if dataset_name in url:
                return json_response(load_resource(resource_name))
        raise AssertionError(f'Unexpected ARM endpoint: {url}')

    mod_get = Mock()
    monkeypatch.setattr('basin3d.plugins.arm.get_url', mock_get_url)
    monkeypatch.setattr('basin3d.plugins.arm.requests.get', mod_get)
    monkeypatch.setattr('basin3d.plugins.arm.ARM_NAME', 'test-user')
    monkeypatch.setattr('basin3d.plugins.arm.ARM_TOKEN', 'test-token')
    monkeypatch.setattr('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(tmp_path))

    return metadata_and_query_urls, mod_get


def test_measurement_timeseries_tvp_observation_no_matching_observed_property(tmp_path, monkeypatch):
    query = {
        'monitoring_feature': ['ARM-SGP-E12', 'ARM-SGP-E15'],
        'observed_property': ['RDC'],
        'start_date': '2000-01-01',
        'end_date': '2000-01-15',
        'aggregation_duration': 'MINUTE',
    }
    metadata_and_query_urls, mod_get = mock_no_result_measurement_endpoints(
        tmp_path, monkeypatch, {})

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert results == []
    assert metadata_and_query_urls == []
    assert mod_get.call_count == 0

    actual_messages = {message.msg for message in observations.synthesis_response.messages}
    assert 'Translated query for datasource ARM is not valid.' in actual_messages


def test_measurement_timeseries_tvp_observation_no_matching_monitoring_feature(tmp_path, monkeypatch):
    query = {
        'monitoring_feature': ['A-1'],
        'observed_property': ['AT'],
        'start_date': '2000-01-01',
        'end_date': '2000-01-15',
        'aggregation_duration': 'MINUTE',
    }
    metadata_and_query_urls, mod_get = mock_no_result_measurement_endpoints(
        tmp_path, monkeypatch, {})

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert results == []
    assert metadata_and_query_urls == []
    assert mod_get.call_count == 0

    actual_messages = {message.msg for message in observations.synthesis_response.messages}
    assert 'No monitoring features for ARM were specified or they were not specified with the ARM prefix.' in actual_messages


def test_measurement_timeseries_tvp_observation_unsupported_aggregation_duration(tmp_path, monkeypatch):
    query = {
        'monitoring_feature': ['ARM-SGP-E15'],
        'observed_property': ['AT'],
        'start_date': '2023-04-01',
        'end_date': '2023-04-30',
        'aggregation_duration': 'DAY',
    }
    metadata_and_query_urls, mod_get = mock_no_result_measurement_endpoints(
        tmp_path, monkeypatch, {})

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert results == []
    assert metadata_and_query_urls == []
    assert mod_get.call_count == 0

    actual_messages = {message.msg for message in observations.synthesis_response.messages}
    assert 'Translated query for datasource ARM is not valid.' in actual_messages


def test_measurement_timeseries_tvp_observation_invalid_zarr_path(tmp_path, monkeypatch, caplog):
    invalid_zarr_path = tmp_path / 'missing'
    monkeypatch.setattr('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(invalid_zarr_path))
    mock_get_url = Mock(side_effect=AssertionError('ARM endpoints should not be called'))
    mock_get = Mock(side_effect=AssertionError('ARM data files should not be downloaded'))
    monkeypatch.setattr('basin3d.plugins.arm.get_url', mock_get_url)
    monkeypatch.setattr('basin3d.plugins.arm.requests.get', mock_get)

    query = {
        'monitoring_feature': ['ARM-SGP-E15'],
        'observed_property': ['AT'],
        'start_date': '2023-04-01',
        'end_date': '2023-04-30',
        'aggregation_duration': 'MINUTE',
    }
    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    expected_message = (f'ARM zarr path does not exist: {invalid_zarr_path}. The local directory for temporary files '
                       'should be set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your '
                       'configuration.')
    assert results == []
    assert [message.msg for message in observations.synthesis_response.messages] == [expected_message]
    assert expected_message in caplog.text
    assert mock_get_url.call_count == 0
    assert mock_get.call_count == 0


def test_measurement_timeseries_tvp_observation_named_sites_no_variable(tmp_path, monkeypatch):
    query = {
        'monitoring_feature': ['ARM-SGP-E12', 'ARM-SGP-E15'],
        'observed_property': ['SD'],
        'start_date': '2000-01-01',
        'end_date': '2000-01-15',
        'aggregation_duration': 'MINUTE',
    }
    metadata_and_query_urls, mod_get = mock_no_result_measurement_endpoints(
        tmp_path, monkeypatch, {})

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert results == []
    assert len(metadata_and_query_urls) == 1
    assert 'metadata/data_product' in metadata_and_query_urls[0]
    assert mod_get.call_count == 0

    actual_messages = {message.msg for message in observations.synthesis_response.messages}
    assert {
        'SGP-E12 does not have any of the queried observed properties.',
        'SGP-E15 does not have any of the queried observed properties.',
    }.issubset(actual_messages)


def test_measurement_timeseries_tvp_observation_named_sites_empty_results(tmp_path, monkeypatch):
    query = {
        'monitoring_feature': ['ARM-SGP-E12', 'ARM-SGP-E15'],
        'observed_property': ['W_SPD'],
        'start_date': '2000-01-01',
        'end_date': '2000-01-15',
        'aggregation_duration': 'MINUTE',
        'statistic': ['MEAN'],
    }
    metadata_and_query_urls, mod_get = mock_no_result_measurement_endpoints(
        tmp_path, monkeypatch,
        {
            'sgpmetE12.b1': 'arm_query_empty_result.json',
            'sgpmetE15.b1': 'arm_query_empty_result.json',
        })

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert results == []
    assert any('sgpmetE12.b1' in url for url in metadata_and_query_urls)
    assert any('sgpmetE15.b1' in url for url in metadata_and_query_urls)
    assert len(metadata_and_query_urls) == 3
    assert mod_get.call_count == 0

    actual_messages = {message.msg for message in observations.synthesis_response.messages}
    assert {
        'SGP-E12 does not have any files that match the query parameters. Skipping',
        'SGP-E15 does not have any files that match the query parameters. Skipping',
    }.issubset(actual_messages)


@pytest.mark.parametrize(
    'query',
    [pytest.param({'monitoring_feature': [(-98.7, 35.85, -97.5, 36.3), (-98.7, 35.85, -95.5, 37.0)],
                   'observed_property': ['APA'], 'start_date': '2025-04-01', 'end_date': '2025-04-05',
                   'aggregation_duration': 'MINUTE', 'result_quality': ['VALIDATED']},
                  id='bbox-sites-precipitation')],
)
def test_measurement_timeseries_tvp_observation_bbox(query, tmp_path, monkeypatch):
    metadata_resources = {
        ARM_BBOX_METADATA_RESOURCES[bbox][0]: ARM_BBOX_METADATA_RESOURCES[bbox][1]
        for bbox in [(-98.7, 35.85, -97.5, 36.3), (-98.7, 35.85, -95.5, 37.0)]
    }
    data_resources = {
        'E12': ('arm_query5_E12.json', 'arm_data5_sgpmetE12.b1.20250401-20250405.cdf', 7050),
        'E13': ('arm_query5_E13.json', 'arm_data5_sgpmetE13.b1.20250401-20250405.cdf', 7200),
        'E32': ('arm_query5_E32.json', 'arm_data5_sgpmetE32.b1.20250401-20250405.cdf', 7200),
        'E33': ('arm_query5_E33.json', 'arm_data5_sgpmetE33.b1.20250401-20250405.cdf', 7200),
        'E37': ('arm_query5_E37.json', 'arm_data5_sgpmetE37.b1.20250401-20250405.cdf', 7200),
        'E39': ('arm_query5_E39.json', 'arm_data5_sgpmetE39.b1.20250401-20250405.cdf', 7198),
    }
    metadata_and_query_urls = []

    def mock_get_url(url, *args, **kwargs):
        metadata_and_query_urls.append(url)
        if url in metadata_resources:
            return json_response(load_resource(metadata_resources[url]))
        for location, (query_resource, _, _) in data_resources.items():
            if f'sgpmet{location}.b1' in url:
                return json_response(load_resource(query_resource))
        if 'armlive/query' in url:
            return json_response(load_resource('arm_query_empty_result.json'))
        raise AssertionError(f'Unexpected ARM endpoint: {url}')

    def mock_mod_get(url, *args, **kwargs):
        for location, (_, data_resource, _) in data_resources.items():
            file_batch = kwargs.get('json', [])
            if any(f'sgpmet{location}.b1' in file_name for file_name in file_batch):
                return streaming_response(data_resource)
        raise AssertionError(f'Unexpected ARM data endpoint: {url}')

    mod_get = Mock(side_effect=mock_mod_get)
    monkeypatch.setattr('basin3d.plugins.arm.get_url', mock_get_url)
    monkeypatch.setattr('basin3d.plugins.arm.requests.get', mod_get)
    monkeypatch.setattr('basin3d.plugins.arm.ARM_NAME', 'test-user')
    monkeypatch.setattr('basin3d.plugins.arm.ARM_TOKEN', 'test-token')
    monkeypatch.setattr('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(tmp_path))

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert len(results) == len(data_resources)
    observations_by_site = {
        observation.feature_of_interest.id.removeprefix('ARM-SGP-'): observation
        for observation in results
    }
    assert set(observations_by_site) == set(data_resources)

    for location, (_, data_resource, expected_length) in data_resources.items():
        observation = observations_by_site[location]
        with xr.open_dataset(RESOURCE_DIR / data_resource) as data:
            source_values = data['atmos_pressure'].values.tolist()
            quality_values = data['qc_atmos_pressure'].values.tolist()
            expected_values = [value * 0.4 for value, quality in zip(source_values, quality_values)
                               if quality == 0]

        assert len(observation.result.value) == expected_length
        assert len(observation.result.result_quality) == expected_length
        assert len(observation.result.value) == len(expected_values)
        assert observation.unit_of_measurement == 'mm Hg'
        actual_values = [result.value for result in observation.result.value]
        assert actual_values[:3] == pytest.approx(expected_values[:3])
        assert actual_values[-3:] == pytest.approx(expected_values[-3:])
        assert [quality.get_basin3d_vocab() for quality in observation.result_quality] == ['VALIDATED']

    assert len(metadata_and_query_urls) == 17
    assert all(metadata_url in metadata_and_query_urls for metadata_url in metadata_resources)
    assert sum('armlive/query' in url for url in metadata_and_query_urls) == 15
    assert mod_get.call_count == len(data_resources)

    actual_messages = {message.msg for message in observations.synthesis_response.messages}
    assert 'Filtered 4 values for SGP-E12 variable atmos_pressure based on the result quality query.' in actual_messages
    assert {
        f'SGP-{location} does not have any files that match the query parameters. Skipping'
        for location in {'E11', 'E15', 'E34', 'E35', 'E36', 'E38', 'E40', 'E41', 'S4'}
    }.issubset(actual_messages)


def test_measurement_timeseries_tvp_observation_bbox_empty_variables(tmp_path, monkeypatch):
    bbox = (-98.7, 35.85, -97.5, 36.3)
    metadata_url, _ = ARM_BBOX_METADATA_RESOURCES[bbox]
    metadata_and_query_urls = []

    def mock_get_url(url, *args, **kwargs):
        metadata_and_query_urls.append(url)
        if url == metadata_url:
            return json_response(load_resource('arm_metadata_bbox_empty_variables.json'))
        raise AssertionError(f'Unexpected ARM endpoint: {url}')

    mod_get = Mock()
    monkeypatch.setattr('basin3d.plugins.arm.get_url', mock_get_url)
    monkeypatch.setattr('basin3d.plugins.arm.requests.get', mod_get)
    monkeypatch.setattr('basin3d.plugins.arm.ARM_NAME', 'test-user')
    monkeypatch.setattr('basin3d.plugins.arm.ARM_TOKEN', 'test-token')
    monkeypatch.setattr('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(tmp_path))

    query = {
        'monitoring_feature': [bbox],
        'observed_property': ['APA'],
        'start_date': '2025-04-01',
        'end_date': '2025-04-05',
        'aggregation_duration': 'MINUTE',
    }
    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert results == []
    assert metadata_and_query_urls == [metadata_url]
    assert mod_get.call_count == 0

    actual_messages = [message.msg for message in observations.synthesis_response.messages]
    expected_message = 'The metadata for SGP-E36 does not specify any variables. Skipping'
    assert actual_messages.count(expected_message) == 1


def test_measurement_timeseries_tvp_observation_missing_qc_variable(tmp_path, monkeypatch):
    query_resource = 'arm_query_no_qc_sgpmetE15.json'
    data_resource = 'arm_data_no_qc_sgpmetE15.b1.20220701.000000.cdf'
    metadata_and_query_urls = []

    def mock_get_url(url, *args, **kwargs):
        metadata_and_query_urls.append(url)
        if 'metadata/data_product' in url:
            return json_response(load_resource(ARM_METADATA_RESOURCE))
        if 'sgpmetE15.b1' in url:
            return json_response(load_resource(query_resource))
        raise AssertionError(f'Unexpected ARM endpoint: {url}')

    mod_get = Mock(return_value=streaming_response(data_resource))
    monkeypatch.setattr('basin3d.plugins.arm.get_url', mock_get_url)
    monkeypatch.setattr('basin3d.plugins.arm.requests.get', mod_get)
    monkeypatch.setattr('basin3d.plugins.arm.ARM_NAME', 'test-user')
    monkeypatch.setattr('basin3d.plugins.arm.ARM_TOKEN', 'test-token')
    monkeypatch.setattr('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(tmp_path))

    query = {
        'monitoring_feature': ['ARM-SGP-E15'],
        'observed_property': ['AT'],
        'start_date': '2022-07-01',
        'end_date': '2022-07-01',
        'aggregation_duration': 'MINUTE',
        'result_quality': ['VALIDATED'],
    }
    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    observations = synthesizer.measurement_timeseries_tvp_observations(**query)
    results = list(observations)

    assert len(results) == 1
    observation = results[0]
    assert observation.feature_of_interest.id == 'ARM-SGP-E15'

    with xr.open_dataset(RESOURCE_DIR / data_resource) as data:
        expected_values = data['temp_mean'].values.tolist()

    assert len(observation.result.value) == len(expected_values)
    assert [result.value for result in observation.result.value] == pytest.approx(expected_values)
    assert observation.result.result_quality == []
    assert observation.result_quality == []
    assert len(metadata_and_query_urls) == 2
    assert any('metadata/data_product' in url for url in metadata_and_query_urls)
    assert any('sgpmetE15.b1' in url for url in metadata_and_query_urls)
    assert mod_get.call_count == 1

    actual_messages = [message.msg for message in observations.synthesis_response.messages]
    expected_message = ('SGP-E15 variable temp_mean did not have quality control information to filter on. '
                        'Returning all values.')
    assert actual_messages.count(expected_message) == 1
    assert not any(message.startswith('Filtered ') for message in actual_messages)


@pytest.mark.parametrize(
    'query',
    [pytest.param({'monitoring_feature': [(1, 2, 3)]}, id='malformed-bbox'),
     pytest.param({'feature_type': 'not-a-feature-type'}, id='invalid-feature-type')],
)
def test_monitoring_features_errors(query):
    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])

    if query.get('feature_type') == 'not-a-feature-type':
        with pytest.raises(ValidationError):
            synthesizer.monitoring_features(**query)
    else:
        with pytest.raises(ValidationError):
            synthesizer.monitoring_features(**query)


def test_monitoring_features_unsupported_feature_type():
    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(feature_type='BASIN')

    assert list(monitoring_features) == []
    assert [message.msg for message in monitoring_features.synthesis_response.messages] == [
        "ARM does not specified feature type: BASIN. Only feature types ['POINT'] are supported."
    ]


@pytest.mark.parametrize(
    'query, expected_count, expected_metadata_calls',
    [pytest.param({'datasource': 'ARM'}, 66, [None],
                  id='datasource-all-points'),
     pytest.param({'monitoring_feature': ['ARM-SGP-E15']}, 1, [None],
                  id='one-point-by-name'),
     pytest.param({'feature_type': 'point'}, 66, [None],
                  id='all-points'),
     pytest.param({'monitoring_feature': ['ARM-TMP-M1', 'ARM-GUC-M1', 'ARM-SGP-E12'], 'feature_type': 'point'},
                  3, [None],
                  id='multiple-named'),
     pytest.param({'monitoring_feature': [(-98.7, 35.85, -95.5, 37.0)], 'feature_type': 'point'},
                  15, [(-98.7, 35.85, -95.5, 37.0)],
                  id='one-bbox'),
     pytest.param({'monitoring_feature': ['ARM-GUC-M1', (-98.7, 35.85, -95.5, 37.0)], 'feature_type': 'point'},
                  16, [(-98.7, 35.85, -95.5, 37.0), None],
                  id='non-overlapping-name-and-bbox'),
     pytest.param({'monitoring_feature': ['ARM-SGP-12', (-98.7, 35.85, -95.5, 37.0)], 'feature_type': 'point'},
                  15, [(-98.7, 35.85, -95.5, 37.0), None],
                  id='overlapping-name-and-bbox'),
     pytest.param({'monitoring_feature': [(-98.7, 35.85, -97.5, 36.3), (-97.5, 36.3, -95.5, 37.0)], 'feature_type': 'point'},
                  11, [(-98.7, 35.85, -97.5, 36.3), (-97.5, 36.3, -95.5, 37.0)],
                  id='non-overlapping-bboxes'),
     pytest.param({'monitoring_feature': [(-98.7, 35.85, -97.5, 36.3), (-98.7, 35.85, -95.5, 37.0)],
                   'feature_type': 'point'},
                  15, [(-98.7, 35.85, -97.5, 36.3), (-98.7, 35.85, -95.5, 37.0)],
                  id='overlapping-bboxes'),
     pytest.param({'monitoring_feature': [(-106.71, 39.58, -106.7, 39.59)], 'feature_type': 'point'},
                  0, [(-106.71, 39.58, -106.7, 39.59)],
                  id='empty-bbox')],
)
def test_monitoring_features(query, expected_count, expected_metadata_calls, monkeypatch):
    full_metadata = load_resource(ARM_METADATA_RESOURCE)
    metadata_calls = []

    def mock_get_arm_metadata(_url, bbox, _synthesis_messages):
        metadata_calls.append(bbox)
        if bbox is None:
            return full_metadata
        return load_resource(ARM_BBOX_METADATA_RESOURCES[bbox][1])

    monkeypatch.setattr('basin3d.plugins.arm._get_arm_metadata', Mock(side_effect=mock_get_arm_metadata))

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    results = list(monitoring_features)

    assert len(results) == expected_count
    for monitoring_feature in results:
        assert monitoring_feature.feature_type == FeatureTypeEnum.POINT

    assert metadata_calls == expected_metadata_calls
