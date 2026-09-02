import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest
import requests
import numpy as np
import xarray as xr

from basin3d.core.catalog import CatalogSqlAlchemy
from basin3d.core.models import GeographicCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature
from basin3d.core.schema.enum import FeatureTypeEnum, NO_MAPPING_TEXT, SpatialSamplingShapes
from basin3d.plugins.arm import (ARMDataSourcePlugin, _batch_files, _get_arm_data_files,
                                 _collect_to_zarr, _create_zarr_temp_dir, _fetch_timeseries,
                                 _get_arm_metadata, _get_arm_request, _get_mf_files,
                                 _get_selected_arm_metadata, _load_mf_object,
                                 _parse_arm_metadata, _build_tvp_results, _validate_zarr_path)


class DummyResponse:
    def __init__(self, data=None, status_code=200, text=None):
        self.data = data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.data


def arm_data_query_record(resource_name='arm_data_query_1.json'):
    resource_path = Path(__file__).parent / 'resources' / resource_name
    with resource_path.open() as resource:
        resource.readline()
        return json.loads(resource.readline())


# ================================
# TESTS for _fetch_timeseries - help from Codex

def arm_netcdf_bytes(resource_name='arm_tmpmetM1.b1.20140603-20140604.cdf'):
    resource_path = Path(__file__).parent / 'resources' / resource_name
    return resource_path.read_bytes()


def streaming_response(chunks, status_code=200, text=''):
    response = Mock(status_code=status_code, text=text)
    response.iter_content.return_value = chunks
    return response


def test_fetch_timeseries_returns_none_when_response_is_none():
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []

    with patch('basin3d.plugins.arm.requests.get', return_value=None), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request returned no response for {url}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_fetch_timeseries_returns_none_for_request_exception():
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    exception = requests.RequestException('network unavailable')

    with patch('basin3d.plugins.arm.requests.get', side_effect=exception), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request failed for {url}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_fetch_timeseries_returns_none_for_non_200_response():
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    response = streaming_response([], status_code=404, text='file not found')

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request returned HTTP 404 for {url}: file not found'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    response.close.assert_called_once_with()


def test_fetch_timeseries_truncates_long_non_200_response_text():
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    response_text = 'x' * 600
    response = streaming_response([], status_code=500, text=response_text)

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.logger.warning'):
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request returned HTTP 500 for {url}: {response_text[:500]}...'
    assert result is None
    assert synthesis_messages == [expected_message]
    assert response_text not in synthesis_messages[0]
    response.close.assert_called_once_with()


def test_fetch_timeseries_loads_requested_variables_and_deletes_temp_file():
    url = 'https://adc.arm.gov/armlive/mod'
    file_batch = ['arm_tmpmetM1.b1.20140603-20140604.cdf']
    variables = ['time', 'atmos_pressure', 'temp_mean']
    synthesis_messages = []
    content = arm_netcdf_bytes()
    response = streaming_response([content])
    created_files = []
    real_named_temporary_file = tempfile.NamedTemporaryFile

    def named_temporary_file_spy(*args, **kwargs):
        temporary_file = real_named_temporary_file(*args, **kwargs)
        created_files.append(temporary_file)
        return temporary_file

    with patch('basin3d.plugins.arm.requests.get', return_value=response) as mock_get, \
            patch('basin3d.plugins.arm.tempfile.NamedTemporaryFile', side_effect=named_temporary_file_spy):
        result = _fetch_timeseries(url, file_batch, variables, synthesis_messages)

    mock_get.assert_called_once_with(url, json=file_batch, stream=True)
    assert len(created_files) == 1
    assert not Path(created_files[0].name).exists()
    assert set(result.variables) == set(variables)
    assert result.sizes['time'] == 2880
    assert synthesis_messages == []
    response.close.assert_called_once_with()

    with xr.open_dataset(Path(__file__).parent / 'resources' / file_batch[0]) as expected:
        assert result.equals(expected[variables].load())
    result.close()


def test_fetch_timeseries_skips_empty_response_chunks():
    url = 'https://adc.arm.gov/armlive/mod'
    variables = ['time', 'atmos_pressure']
    synthesis_messages = []
    content = arm_netcdf_bytes()
    midpoint = len(content) // 2
    response = streaming_response([b'', content[:midpoint], b'', content[midpoint:], b''])

    with patch('basin3d.plugins.arm.requests.get', return_value=response):
        result = _fetch_timeseries(url, ['file.cdf'], variables, synthesis_messages)

    assert set(result.variables) == set(variables)
    assert result.sizes['time'] == 2880
    assert synthesis_messages == []
    response.close.assert_called_once_with()
    result.close()


def test_fetch_timeseries_returns_existing_variables_and_warns_for_missing_variables():
    url = 'https://adc.arm.gov/armlive/mod'
    variables = ['time', 'missing_variable', 'temp_mean']
    synthesis_messages = []
    response = streaming_response([arm_netcdf_bytes()])

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], variables, synthesis_messages)

    expected_message = (f'ARM data response for {url} did not contain requested variables: '
                       'missing_variable')
    assert set(result.variables) == {'time', 'temp_mean'}
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    response.close.assert_called_once_with()
    result.close()


def test_fetch_timeseries_returns_none_when_all_variables_are_missing():
    url = 'https://adc.arm.gov/armlive/mod'
    variables = ['missing_a', 'missing_b']
    synthesis_messages = []
    response = streaming_response([arm_netcdf_bytes()])

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], variables, synthesis_messages)

    expected_message = (f'ARM data response for {url} did not contain requested variables: '
                       'missing_a, missing_b')
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    response.close.assert_called_once_with()


def test_fetch_timeseries_closes_response_when_streaming_raises():
    url = 'https://adc.arm.gov/armlive/mod'
    response = streaming_response([])
    response.iter_content.side_effect = RuntimeError('stream interrupted')
    synthesis_messages = []

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request processing failed for {url}: stream interrupted'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    response.close.assert_called_once_with()


@pytest.mark.parametrize('exception', [ValueError('invalid NetCDF content'),
                                      TypeError('invalid NetCDF type'),
                                      KeyError('missing NetCDF field')])
def test_fetch_timeseries_returns_none_when_dataset_processing_fails(exception):
    url = 'https://adc.arm.gov/armlive/mod'
    response = streaming_response([b'invalid netcdf'])
    synthesis_messages = []

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.xr.open_dataset', side_effect=exception), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request processing failed for {url}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    response.close.assert_called_once_with()


def test_fetch_timeseries_returns_none_when_streaming_request_fails():
    url = 'https://adc.arm.gov/armlive/mod'
    response = streaming_response([])
    exception = requests.RequestException('connection interrupted')
    response.iter_content.side_effect = exception
    synthesis_messages = []

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request processing failed for {url}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    response.close.assert_called_once_with()


def test_fetch_timeseries_closes_response_when_temp_file_write_raises(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    response = streaming_response([arm_netcdf_bytes()])
    temporary_file_path = tmp_path / 'arm-test.nc'
    synthesis_messages = []

    class FailingTemporaryFile:
        name = str(temporary_file_path)

        def __enter__(self):
            temporary_file_path.touch()
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            temporary_file_path.unlink(missing_ok=True)
            return False

        def write(self, chunk):
            raise OSError('disk full')

        def flush(self):
            pass

    with patch('basin3d.plugins.arm.requests.get', return_value=response), \
            patch('basin3d.plugins.arm.tempfile.NamedTemporaryFile', return_value=FailingTemporaryFile()), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _fetch_timeseries(url, ['file.cdf'], ['time'], synthesis_messages)

    expected_message = f'ARM data request processing failed for {url}: disk full'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    assert not temporary_file_path.exists()
    response.close.assert_called_once_with()


# ================================
# TESTS for _validate_zarr_path - help from Codex
# The cases that raise validation errors are incorporated into the _collect_to_zarr tests below.

def test_validate_zarr_path_returns_true_for_existing_writable_directory(tmp_path):
    synthesis_messages = []

    with patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _validate_zarr_path(tmp_path, synthesis_messages)

    assert result is True
    assert synthesis_messages == []
    mock_logger.assert_not_called()


# ================================
# TESTS for _creat_zarr_temp_dir - help from Codex
def test_create_zarr_temp_dir_creates_child_and_preserves_parent_contents(tmp_path):
    parent_file = tmp_path / 'keep.txt'
    parent_file.write_text('keep')
    synthesis_messages = []

    # OK to pass a Path instead of a str b/c the conversion of str to path is not expected to be a failure point
    child_path = _create_zarr_temp_dir(tmp_path, synthesis_messages)

    assert child_path.parent == tmp_path
    assert child_path.is_dir()
    assert parent_file.read_text() == 'keep'
    assert synthesis_messages == []

    child_path.rmdir()


def test_create_zarr_temp_dir_returns_none_for_invalid_path_string(tmp_path):
    zarr_path = tmp_path / 'missing'
    synthesis_messages = []

    with patch('basin3d.plugins.arm.tempfile.mkdtemp') as mock_mkdtemp, \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _create_zarr_temp_dir(str(zarr_path), synthesis_messages)

    expected_message = (f'ARM zarr path does not exist: {zarr_path}. The local directory for temporary files should be '
                       'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    mock_mkdtemp.assert_not_called()


def test_create_zarr_temp_dir_returns_none_when_creation_fails(tmp_path):
    synthesis_messages = []
    exception = OSError(28, 'No space left on device')

    with patch('basin3d.plugins.arm.tempfile.mkdtemp', side_effect=exception), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        # OK to pass a Path instead of a str b/c the conversion of str to path is not expected to be a failure point
        result = _create_zarr_temp_dir(tmp_path, synthesis_messages)

    expected_message = f'Failed to create ARM zarr temporary directory in {tmp_path}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


# ================================
# TESTS for _collect_to_zarr - help from Codex

# This tests exercise _validate_zarr_path indirectly through _collect_to_zarr.
def test_collect_to_zarr_returns_none_for_missing_path(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    zarr_path = tmp_path / 'missing'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._fetch_timeseries') as mock_fetch, \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, zarr_path)

    expected_message = (f'ARM zarr path does not exist: {zarr_path}. The local directory for temporary files should be '
                       'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    mock_fetch.assert_not_called()


# This tests exercise _validate_zarr_path indirectly through _collect_to_zarr.
def test_collect_to_zarr_returns_none_for_unwritable_path(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []

    with patch('basin3d.plugins.arm.os.access', return_value=False), \
            patch('basin3d.plugins.arm._fetch_timeseries') as mock_fetch, \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = (f'ARM zarr path is not writable: {tmp_path}. The local directory for temporary files can be '
                       'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR, otherwise the default working directory '
                       'is used. Please double check your configuration.')
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    mock_fetch.assert_not_called()


# This tests exercise _validate_zarr_path indirectly through _collect_to_zarr.
def test_collect_to_zarr_returns_none_for_path_that_is_a_file(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    zarr_path = tmp_path / 'not-a-directory'
    zarr_path.touch()
    synthesis_messages = []

    with patch('basin3d.plugins.arm._fetch_timeseries') as mock_fetch, \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, zarr_path)

    expected_message = (f'ARM zarr path is not a directory: {zarr_path}. The local directory for temporary files should be '
                       'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    mock_fetch.assert_not_called()

# Tests the persistence of the zarr subdirectory within the MeasurementTimeseriesTVPObservation list function
def test_measurement_list_removes_child_zarr_dir_and_preserves_parent(tmp_path):
    parent_file = tmp_path / 'keep.txt'
    parent_file.write_text('keep')
    created_paths = []
    query = Mock(monitoring_feature=['ARM-ENA-C1'], observed_property=['temp_mean'],
                 start_date='2014-06-03', end_date=None)
    mf_lookup = {'ARM-ENA-C1': {'site_id': 'ENA', 'facility_id': 'C1', 'variables': ['temp_mean']}}

    def collect_to_zarr(url, file_batches, query_vars, synthesis_messages, zarr_path):
        created_paths.append(zarr_path)
        (zarr_path / 'store').mkdir()
        (zarr_path / 'store' / 'data').touch()
        return None

    access = arm_measurement_timeseries_access()
    with patch('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(tmp_path)), \
            patch('basin3d.plugins.arm._get_selected_arm_metadata', return_value=({'ARM-ENA-C1'}, mf_lookup)), \
            patch('basin3d.plugins.arm._get_mf_files', return_value=[['file.cdf']]), \
            patch('basin3d.plugins.arm._collect_to_zarr', side_effect=collect_to_zarr):
        list(access.list(query))

    assert len(created_paths) == 1
    assert created_paths[0].parent == tmp_path
    assert not created_paths[0].exists()
    assert parent_file.read_text() == 'keep'


# Tests the case in MeasurementTimeseriesTVPObservation list method where the batched xarrays are not consistent;
#    partial results are ignored and None is returned.
def test_measurement_list_skips_site_when_batch_structures_mismatch(tmp_path):
    parent_file = tmp_path / 'keep.txt'
    parent_file.write_text('keep')
    query = Mock(monitoring_feature=['ARM-ENA-C1'], observed_property=['temp_mean', 'rh_mean'],
                 start_date='2014-06-03', end_date=None)
    mf_lookup = {'ARM-ENA-C1': {'site_id': 'ENA', 'facility_id': 'C1',
                                'variables': ['temp_mean', 'rh_mean']}}
    first_part = MagicMock(data_vars=['temp_mean', 'rh_mean'], sizes={'time': 1})
    second_part = MagicMock(data_vars=['temp_mean'], sizes={'time': 1})

    access = arm_measurement_timeseries_access()
    with patch('basin3d.plugins.arm.LOCAL_TEMP_DIR', str(tmp_path)), \
            patch('basin3d.plugins.arm._get_selected_arm_metadata',
                  return_value=({'ARM-ENA-C1'}, mf_lookup)), \
            patch('basin3d.plugins.arm._get_mf_files', return_value=[['first.cdf'], ['second.cdf']]), \
            patch('basin3d.plugins.arm._fetch_timeseries', side_effect=[first_part, second_part]):
        results = list(access.list(query))

    assert results == []
    assert parent_file.read_text() == 'keep'
    assert list(tmp_path.iterdir()) == [parent_file]


def test_collect_to_zarr_preserves_no_results_message_for_valid_empty_directory(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []

    with patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = _collect_to_zarr(url, [], ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = f'No ARM timeseries datasets were successfully retrieved for {url}.'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_collect_to_zarr_returns_none_when_initial_zarr_write_fails(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    part = Mock(data_vars=[], sizes={})
    exception = OSError(28, 'No space left on device')
    part.to_zarr.side_effect = exception

    with patch('basin3d.plugins.arm._fetch_timeseries', return_value=part), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = f'Failed to write ARM timeseries data to zarr path {tmp_path}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    part.to_zarr.assert_called_once_with(tmp_path, mode='w', consolidated=False)
    part.close.assert_called_once_with()


@pytest.mark.parametrize('exception', [ValueError('invalid zarr metadata'),
                                      TypeError('invalid zarr encoding'),
                                      KeyError('missing zarr variable')])
def test_collect_to_zarr_returns_none_when_initial_zarr_write_raises_expected_error(tmp_path, exception):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    part = Mock(data_vars=[], sizes={})
    part.to_zarr.side_effect = exception

    with patch('basin3d.plugins.arm._fetch_timeseries', return_value=part), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = f'Failed to write ARM timeseries data to zarr path {tmp_path}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    part.close.assert_called_once_with()


def test_collect_to_zarr_returns_none_when_zarr_append_fails(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    first_part = Mock(data_vars=[], sizes={})
    second_part = Mock(data_vars=[], sizes={})
    exception = OSError(28, 'No space left on device')
    second_part.to_zarr.side_effect = exception

    with patch('basin3d.plugins.arm._fetch_timeseries', side_effect=[first_part, second_part]), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, [['first.cdf'], ['second.cdf']], ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = f'Failed to write ARM timeseries data to zarr path {tmp_path}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    first_part.to_zarr.assert_called_once_with(tmp_path, mode='w', consolidated=False)
    second_part.to_zarr.assert_called_once_with(tmp_path, mode='a', append_dim='time', consolidated=False)
    first_part.close.assert_called_once_with()
    second_part.close.assert_called_once_with()


def test_collect_to_zarr_returns_none_when_opening_zarr_fails(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    part = Mock(data_vars=[], sizes={})
    exception = ValueError('invalid zarr store')

    with patch('basin3d.plugins.arm._fetch_timeseries', return_value=part), \
            patch('basin3d.plugins.arm.xr.open_zarr', side_effect=exception), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = f'Failed to open ARM timeseries zarr path {tmp_path}: {exception}'
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    part.close.assert_called_once_with()


def test_collect_to_zarr_writes_single_batch_and_returns_opened_zarr(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    file_batch = ['first.cdf']
    meas_variables = ['temp_mean']
    synthesis_messages = []
    part = Mock(data_vars=[], sizes={})
    expected_result = Mock()

    with patch('basin3d.plugins.arm._fetch_timeseries', return_value=part) as mock_fetch, \
            patch('basin3d.plugins.arm.xr.open_zarr', return_value=expected_result) as mock_open_zarr:
        result = _collect_to_zarr(url, [file_batch], meas_variables, synthesis_messages, tmp_path)

    assert result is expected_result
    mock_fetch.assert_called_once_with(url, file_batch, ['time'] + meas_variables, synthesis_messages)
    part.to_zarr.assert_called_once_with(tmp_path, mode='w', consolidated=False)
    part.close.assert_called_once_with()
    mock_open_zarr.assert_called_once_with(tmp_path, consolidated=False)
    assert synthesis_messages == []


def test_collect_to_zarr_appends_multiple_batches_and_returns_opened_zarr(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    file_batches = [['first.cdf'], ['second.cdf']]
    meas_variables = ['temp_mean']
    synthesis_messages = []
    first_part = Mock(data_vars=[], sizes={})
    second_part = Mock(data_vars=[], sizes={})
    expected_result = Mock()

    with patch('basin3d.plugins.arm._fetch_timeseries', side_effect=[first_part, second_part]) as mock_fetch, \
            patch('basin3d.plugins.arm.xr.open_zarr', return_value=expected_result) as mock_open_zarr:
        result = _collect_to_zarr(url, file_batches, meas_variables, synthesis_messages, tmp_path)

    assert result is expected_result
    assert mock_fetch.call_args_list == [
        call(url, file_batches[0], ['time'] + meas_variables, synthesis_messages),
        call(url, file_batches[1], ['time'] + meas_variables, synthesis_messages),
    ]
    first_part.to_zarr.assert_called_once_with(tmp_path, mode='w', consolidated=False)
    second_part.to_zarr.assert_called_once_with(tmp_path, mode='a', append_dim='time', consolidated=False)
    first_part.close.assert_called_once_with()
    second_part.close.assert_called_once_with()
    mock_open_zarr.assert_called_once_with(tmp_path, consolidated=False)
    assert synthesis_messages == []


def test_collect_to_zarr_allows_different_time_lengths(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    file_batches = [['first.cdf'], ['second.cdf']]
    synthesis_messages = []
    first_part = Mock(data_vars=[], sizes={'time': 1})
    second_part = Mock(data_vars=[], sizes={'time': 2})
    expected_result = Mock()

    with patch('basin3d.plugins.arm._fetch_timeseries', side_effect=[first_part, second_part]), \
            patch('basin3d.plugins.arm.xr.open_zarr', return_value=expected_result):
        result = _collect_to_zarr(url, file_batches, ['temp_mean'], synthesis_messages, tmp_path)

    assert result is expected_result
    second_part.to_zarr.assert_called_once_with(tmp_path, mode='a', append_dim='time', consolidated=False)
    assert synthesis_messages == []


def test_collect_to_zarr_returns_none_for_batch_variable_mismatch(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    file_batches = [['first.cdf'], ['second.cdf']]
    synthesis_messages = []
    first_part = MagicMock(data_vars=['temp_mean', 'rh_mean'], sizes={'time': 1})
    second_part = MagicMock(data_vars=['temp_mean'], sizes={'time': 1})

    with patch('basin3d.plugins.arm._fetch_timeseries', side_effect=[first_part, second_part]), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger, \
            patch('basin3d.plugins.arm.xr.open_zarr') as mock_open_zarr:
        result = _collect_to_zarr(url, file_batches, ['temp_mean', 'rh_mean'], synthesis_messages, tmp_path)

    expected_message = (f'ARM timeseries batch variable mismatch for {url}: expected variables '
                        "['rh_mean', 'temp_mean'], but received variables ['temp_mean'].")
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    mock_open_zarr.assert_not_called()
    first_part.to_zarr.assert_called_once_with(tmp_path, mode='w', consolidated=False)
    second_part.to_zarr.assert_not_called()
    first_part.close.assert_called_once_with()
    second_part.close.assert_called_once_with()


def test_collect_to_zarr_returns_none_for_non_time_dimension_mismatch(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    file_batches = [['first.cdf'], ['second.cdf']]
    synthesis_messages = []
    first_part = Mock(data_vars=[], sizes={'time': 1, 'height': 2})
    second_part = Mock(data_vars=[], sizes={'time': 1, 'height': 3})

    with patch('basin3d.plugins.arm._fetch_timeseries', side_effect=[first_part, second_part]), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, file_batches, ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = (f'ARM timeseries batch non-time dimension mismatch for {url}: expected '
                        "{'height': 2}, but received {'height': 3}.")
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    first_part.close.assert_called_once_with()
    second_part.close.assert_called_once_with()


def test_collect_to_zarr_reports_variable_and_dimension_mismatches(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    file_batches = [['first.cdf'], ['second.cdf']]
    synthesis_messages = []
    first_part = MagicMock(data_vars=['temp_mean', 'rh_mean'], sizes={'time': 1, 'height': 2})
    second_part = MagicMock(data_vars=['temp_mean'], sizes={'time': 1, 'height': 3})

    with patch('basin3d.plugins.arm._fetch_timeseries', side_effect=[first_part, second_part]), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        result = _collect_to_zarr(url, file_batches, ['temp_mean', 'rh_mean'], synthesis_messages, tmp_path)

    expected_message = (f'ARM timeseries batch variable mismatch for {url}: expected variables '
                        "['rh_mean', 'temp_mean'], but received variables ['temp_mean']. "
                        "Non-time dimensions also differ: expected {'height': 2}, but received {'height': 3}.")
    assert result is None
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)
    first_part.close.assert_called_once_with()
    second_part.close.assert_called_once_with()


def test_collect_to_zarr_skips_none_parts_and_returns_no_results(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    file_batch = ['missing.cdf']
    synthesis_messages = []

    with patch('basin3d.plugins.arm._fetch_timeseries', return_value=None) as mock_fetch, \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = _collect_to_zarr(url, [file_batch], ['temp_mean'], synthesis_messages, tmp_path)

    expected_message = f'No ARM timeseries datasets were successfully retrieved for {url}.'
    assert result is None
    mock_fetch.assert_called_once_with(url, file_batch, ['time', 'temp_mean'], synthesis_messages)
    mock_logger.assert_called_once_with(expected_message)
    assert synthesis_messages == [expected_message]


def test_collect_to_zarr_updates_fill_value_from_missing_value(tmp_path):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    variable = Mock(encoding={'_FillValue': -1.0, 'missing_value': -9999.0})
    part = MagicMock(data_vars=['temp_mean'])
    part.__getitem__.return_value = variable
    expected_result = Mock()

    with patch('basin3d.plugins.arm._fetch_timeseries', return_value=part), \
            patch('basin3d.plugins.arm.xr.open_zarr', return_value=expected_result):
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, tmp_path)

    assert result is expected_result
    assert variable.encoding['_FillValue'] == -9999.0
    part.to_zarr.assert_called_once_with(tmp_path, mode='w', consolidated=False)
    part.close.assert_called_once_with()


@pytest.mark.parametrize(
    'encoding',
    [pytest.param({'_FillValue': -1.0, 'missing_value': None}, id='missing_value_none'),
     pytest.param({'_FillValue': -1.0}, id='missing_value_key_absent')])
def test_collect_to_zarr_preserves_fill_value_when_missing_value_is_none_or_absent(tmp_path, encoding):
    url = 'https://adc.arm.gov/armlive/mod'
    synthesis_messages = []
    variable = Mock(encoding=encoding)
    part = MagicMock(data_vars=['temp_mean'])
    part.__getitem__.return_value = variable
    expected_result = Mock()

    with patch('basin3d.plugins.arm._fetch_timeseries', return_value=part), \
            patch('basin3d.plugins.arm.xr.open_zarr', return_value=expected_result):
        result = _collect_to_zarr(url, [['file.cdf']], ['temp_mean'], synthesis_messages, tmp_path)

    assert result is expected_result
    assert variable.encoding['_FillValue'] == -1.0
    part.to_zarr.assert_called_once_with(tmp_path, mode='w', consolidated=False)
    part.close.assert_called_once_with()


# ================================
# TESTS for _batch_files - help from Codex

@pytest.mark.parametrize(
    'file_count,batch_size,expected_batch_lengths',
    [pytest.param(0, 30, [], id='empty_file_list'),
     pytest.param(1, 30, [1], id='one_file'),
     pytest.param(30, 30, [30], id='exact_batch_size'),
     pytest.param(62, 30, [30, 30, 2], id='remainder_batch'),
     pytest.param(4, 1, [1, 1, 1, 1], id='batch_size_one'),
     pytest.param(25, 10.1, [10, 10, 5], id='float_batch_size_coerced_to_int')])
def test_batch_files_splits_file_list(file_count, batch_size, expected_batch_lengths):
    file_list = [f'file_{index}' for index in range(file_count)]

    result = _batch_files(file_list, batch_size)

    assert [len(batch) for batch in result] == expected_batch_lengths
    assert [file_name for batch in result for file_name in batch] == file_list


@pytest.mark.parametrize('batch_size', [0, -1, 0.5], ids=['zero', 'negative', 'coerces_to_zero'])
def test_batch_files_rejects_non_positive_batch_size(batch_size):
    with pytest.raises(ValueError, match='batch_size must be greater than zero'):
        _batch_files(['file_0'], batch_size)


# ================================
# TESTS for _get_mf_files - help from Codex

@pytest.mark.parametrize(
    'data_files',
    [pytest.param({}, id='empty_dict'),
     pytest.param(None, id='none'),
     pytest.param(False, id='false'),
     pytest.param([], id='empty_list')])
def test_get_mf_files_returns_empty_for_falsy_data_files(data_files):
    data_url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_data_files', return_value=data_files) as mock_get_data_files:
        results = _get_mf_files(data_url, synthesis_messages)

    mock_get_data_files.assert_called_once_with(data_url, None, synthesis_messages)
    assert results == []
    assert synthesis_messages == []


def test_get_mf_files_warns_when_files_key_is_missing():
    data_url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []
    data_files = {'status': 'success'}

    with patch('basin3d.plugins.arm._get_arm_data_files', return_value=data_files), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        results = _get_mf_files(data_url, synthesis_messages)

    expected_message = f'No ARM timeseries datasets were successfully retrieved for {data_url}.'
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


@pytest.mark.parametrize('files', [[], None], ids=['empty_list', 'none'])
def test_get_mf_files_warns_when_files_value_is_empty(files):
    data_url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_data_files', return_value={'files': files}), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        results = _get_mf_files(data_url, synthesis_messages)

    expected_message = f'No ARM timeseries datasets were successfully retrieved for {data_url}.'
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


@pytest.mark.parametrize('files', [{'file': 'value'}, 'file_name'], ids=['dict', 'string'])
def test_get_mf_files_returns_empty_for_non_list_files(files):
    data_url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_data_files', return_value={'files': files}), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        results = _get_mf_files(data_url, synthesis_messages)

    expected_message = (
        f'ARM timeseries files for {data_url} were not in expected list format. '
        f'It was {files.__class__.__name__}. Cannot batch.'
    )
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


@pytest.mark.parametrize(
    'files,expected_batches',
    [pytest.param(['file_a', 'file_b', 'file_c'], [['file_a', 'file_b', 'file_c']], id='already_sorted'),
     pytest.param(['file_c', 'file_a', 'file_b'], [['file_a', 'file_b', 'file_c']], id='sorts_before_batching')])
def test_get_mf_files_returns_expected_batches(files, expected_batches):
    data_url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_data_files', return_value={'files': files}) as mock_get_data_files:
        results = _get_mf_files(data_url, synthesis_messages)

    mock_get_data_files.assert_called_once_with(data_url, None, synthesis_messages)
    assert results == expected_batches
    assert synthesis_messages == []


def test_get_mf_files_forwards_synthesis_messages():
    data_url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_data_files', return_value={'files': []}) as mock_get_data_files, \
            patch('basin3d.plugins.arm.logger.warning'):
        results = _get_mf_files(data_url, synthesis_messages)

    mock_get_data_files.assert_called_once_with(data_url, None, synthesis_messages)
    assert results == []
    assert len(synthesis_messages) == 1


# ================================
# TESTS for _get_arm_request - help from Codex

def test_get_arm_request_returns_json_without_bbox():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    response_data = [{'name': 'Surface Meteorological Instrumentation'}]
    synthesis_messages = []

    with patch('basin3d.plugins.arm.get_url', return_value=DummyResponse(response_data)) as mock_get_url:
        results = _get_arm_request(url, None, synthesis_messages, [])

    mock_get_url.assert_called_once_with(url)
    assert results == response_data
    assert synthesis_messages == []


def test_get_arm_request_reorders_bbox_for_arm():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox = (-99.553900, 34.775248, -94.785833, 36.929929)
    synthesis_messages = []

    with patch('basin3d.plugins.arm.get_url', return_value=DummyResponse([])) as mock_get_url:
        _get_arm_request(url, bbox, synthesis_messages, [])

    mock_get_url.assert_called_once_with(
        f'{url}&bbox=-99.5539%2C36.929929%2C-94.785833%2C34.775248'
    )
    assert synthesis_messages == []


@pytest.mark.parametrize('status_code', [400, 500])
def test_get_arm_request_records_non_200_response(status_code):
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    synthesis_messages = []

    with patch('basin3d.plugins.arm.get_url', return_value=DummyResponse(status_code=status_code)), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        results = _get_arm_request(url, None, synthesis_messages, [])

    expected_message = f'ARM request for {url} returned error code {status_code}.'
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


@pytest.mark.parametrize(
    'response_data, response_text, expected_detail',
    [({'message': 'invalid request'}, None, 'invalid request'),
     ({'error': 'service unavailable'}, None, 'service unavailable'),
     ({}, 'metadata service unavailable', 'metadata service unavailable')])
def test_get_arm_request_includes_response_error_detail(response_data, response_text, expected_detail):
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    status_code = 400
    synthesis_messages = []

    with patch(
        'basin3d.plugins.arm.get_url',
        return_value=DummyResponse(response_data, status_code, response_text),
    ), patch('basin3d.plugins.arm.logger.error') as mock_logger:
        results = _get_arm_request(url, None, synthesis_messages, [])

    expected_message = f'ARM request for {url} returned error code {status_code}: {expected_detail}.'
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_get_arm_request_prefers_json_message_over_error_and_text():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    response = DummyResponse(
        {'message': 'invalid request', 'error': 'service unavailable'},
        status_code=400,
        text='raw response detail',
    )
    synthesis_messages = []

    with patch('basin3d.plugins.arm.get_url', return_value=response), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        _get_arm_request(url, None, synthesis_messages, [])

    expected_message = f'ARM request for {url} returned error code 400: invalid request.'
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_get_arm_request_records_no_response():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    synthesis_messages = []

    with patch('basin3d.plugins.arm.get_url', return_value=None), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        results = _get_arm_request(url, None, synthesis_messages, [])

    expected_message = f'ARM request for {url} returned error code NO RESPONSE.'
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_get_arm_request_records_request_exception():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    synthesis_messages = []
    error = TimeoutError('request timed out')

    with patch('basin3d.plugins.arm.get_url', side_effect=error), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        results = _get_arm_request(url, None, synthesis_messages, [])

    expected_message = f'ARM request for {url} failed: {error}'
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_get_arm_request_records_json_exception():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    synthesis_messages = []
    response = Mock(status_code=200)
    error = ValueError('invalid JSON')
    response.json.side_effect = error

    with patch('basin3d.plugins.arm.get_url', return_value=response), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        results = _get_arm_request(url, None, synthesis_messages, [])

    expected_message = f'ARM request for {url} failed: {error}'
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


# ================================
# TESTS for _get_arm_metadata - help from Codex

def test_get_arm_metadata_returns_json_without_bbox():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    response_data = [{'name': 'Surface Meteorological Instrumentation'}]
    synthesis_messages = []
    empty_result = []

    with patch('basin3d.plugins.arm._get_arm_request', return_value=response_data) as mock_get_request:
        results = _get_arm_metadata(url, None, synthesis_messages)

    mock_get_request.assert_called_once_with(url, None, synthesis_messages, empty_result)
    assert results == response_data
    assert synthesis_messages == []


def test_get_arm_metadata_returns_empty_list_for_non_list_response():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    response_data = {'message': 'invalid metadata response'}
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_request', return_value=response_data), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        results = _get_arm_metadata(url, None, synthesis_messages)

    expected_message = (
        f'ARM metadata results for {url} was not in expected list format. '
        'It was dict class. Cannot parse.'
    )
    assert results == []
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


# ================================
# TESTS for _get_arm_data_files - help from Codex

def test_get_arm_data_files_returns_resource_dictionary():
    url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    response_data = arm_data_query_record()
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_request', return_value=response_data) as mock_get_request:
        results = _get_arm_data_files(url, None, synthesis_messages)

    mock_get_request.assert_called_once_with(url, None, synthesis_messages, {})
    assert results is response_data
    assert results['files']
    assert synthesis_messages == []


@pytest.mark.parametrize(
    'response_data',
    [pytest.param({}, id='empty_dictionary'),
     pytest.param({'files': ['file_a'], 'status': 'success'}, id='arbitrary_dictionary_keys')])
def test_get_arm_data_files_returns_dictionary_unchanged(response_data):
    url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_request', return_value=response_data) as mock_get_request:
        results = _get_arm_data_files(url, None, synthesis_messages)

    mock_get_request.assert_called_once_with(url, None, synthesis_messages, {})
    assert results is response_data
    assert synthesis_messages == []


@pytest.mark.parametrize(
    'bbox',
    [pytest.param(None, id='without_bbox'),
     pytest.param((-99.553900, 34.775248, -94.785833, 36.929929), id='with_bbox')])
def test_get_arm_data_files_forwards_bbox_and_synthesis_messages(bbox):
    url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    response_data = {'files': []}
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_request', return_value=response_data) as mock_get_request:
        results = _get_arm_data_files(url, bbox, synthesis_messages)

    mock_get_request.assert_called_once_with(url, bbox, synthesis_messages, {})
    assert results is response_data
    assert synthesis_messages == []


@pytest.mark.parametrize(
    'response_data',
    [pytest.param([], id='list'),
     pytest.param(None, id='none'),
     pytest.param('invalid response', id='string'),
     pytest.param(42, id='integer')])
def test_get_arm_data_files_returns_empty_for_non_dictionary_response(response_data):
    url = 'https://adc.arm.gov/armlive/query?ds=sgpmetE15.b1'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_request', return_value=response_data), \
            patch('basin3d.plugins.arm.logger.error') as mock_logger:
        results = _get_arm_data_files(url, None, synthesis_messages)

    response_class = response_data.__class__.__name__
    expected_message = (
        f'ARM metadata results for {url} was not in expected dictionary format. '
        f'It was {response_class}. Cannot parse.'
    )
    assert results == {}
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


# ================================
# TESTS for _parse_arm_metadata - help from Codex

def arm_metadata_record(**overrides):
    record = {
        'spatialCoverage': {
            'containedInPlace': {
                'identifier': 'ENA',
                'name': 'Eastern North Atlantic',
            },
            'identifier': 'C1',
            'name': 'Graciosa Island, Azores, Portugal',
            'geo': {
                'latitude': 39.0916,
                'longitude': -28.0257,
            },
        },
        'variableMeasured': [
            {'name': 'air_temperature'},
            {'name': 'wind_speed'},
        ],
    }
    record.update(overrides)
    return record


def test_parse_arm_metadata_adds_valid_record():
    mf_lookup = {}
    synthesis_messages = []

    _parse_arm_metadata([arm_metadata_record()], mf_lookup, synthesis_messages)

    assert mf_lookup == {
        'ENA-C1': {
            'id': 'ENA-C1',
            'facility_id': 'C1',
            'name': 'Eastern North Atlantic - Graciosa Island, Azores, Portugal',
            'site_id': 'ENA',
            'site_name': 'Eastern North Atlantic',
            'lat': 39.0916,
            'long': -28.0257,
            'variables': ['air_temperature', 'wind_speed'],
        },
    }
    assert synthesis_messages == []


def test_parse_arm_metadata_skips_duplicate_mf_id():
    mf_lookup = {'ENA-C1': {'id': 'ENA-C1', 'name': 'original'}}
    synthesis_messages = []

    _parse_arm_metadata([arm_metadata_record()], mf_lookup, synthesis_messages)

    assert mf_lookup == {'ENA-C1': {'id': 'ENA-C1', 'name': 'original'}}
    assert synthesis_messages == []


def test_parse_arm_metadata_skips_duplicate_before_extracting_remaining_metadata():
    duplicate_record = {
        'spatialCoverage': {
            'containedInPlace': {'identifier': 'ENA'},
            'identifier': 'C1',
        },
    }
    mf_lookup = {'ENA-C1': {'id': 'ENA-C1', 'name': 'original'}}
    synthesis_messages = []

    _parse_arm_metadata([duplicate_record], mf_lookup, synthesis_messages)

    assert mf_lookup == {'ENA-C1': {'id': 'ENA-C1', 'name': 'original'}}
    assert synthesis_messages == []


@pytest.mark.parametrize(
    'missing_path',
    [('spatialCoverage', 'containedInPlace', 'identifier'),
     ('spatialCoverage', 'containedInPlace', 'name'),
     ('spatialCoverage', 'identifier'),
     ('spatialCoverage', 'name'),
     ('spatialCoverage', 'geo', 'latitude'),
     ('spatialCoverage', 'geo', 'longitude')])
def test_parse_arm_metadata_warns_and_skips_missing_spatial_value(missing_path):
    record = arm_metadata_record()
    target = record
    for key in missing_path[:-1]:
        target = target[key]
    target.pop(missing_path[-1])
    mf_lookup = {}
    synthesis_messages = []

    with patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        _parse_arm_metadata([record], mf_lookup, synthesis_messages)

    assert mf_lookup == {}
    assert len(synthesis_messages) == 1
    mock_logger.assert_called_once_with(synthesis_messages[0])
    assert 'missing required' in synthesis_messages[0]


def test_parse_arm_metadata_warns_and_skips_empty_spatial_value():
    record = arm_metadata_record()
    record['spatialCoverage']['geo']['latitude'] = None
    mf_lookup = {}
    synthesis_messages = []

    with patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        _parse_arm_metadata([record], mf_lookup, synthesis_messages)

    assert mf_lookup == {}
    assert synthesis_messages == [
        'ARM metadata record missing required spatial coverage information: missing geo.latitude'
    ]
    mock_logger.assert_called_once_with(synthesis_messages[0])


@pytest.mark.parametrize('variable_measured', [None, []])
def test_parse_arm_metadata_includes_record_without_variables(variable_measured):
    record = arm_metadata_record(variableMeasured=variable_measured)
    mf_lookup = {}
    synthesis_messages = []

    with patch('basin3d.plugins.arm.logger.info') as mock_logger:
        _parse_arm_metadata([record], mf_lookup, synthesis_messages)

    assert mf_lookup['ENA-C1']['variables'] == []
    assert len(synthesis_messages) == 1
    mock_logger.assert_called_once_with(synthesis_messages[0])
    assert 'has no variableMeasured metadata' in synthesis_messages[0]


def test_parse_arm_metadata_continues_after_invalid_record():
    invalid_record = {'spatialCoverage': {}}
    mf_lookup = {}
    synthesis_messages = []

    _parse_arm_metadata([invalid_record, arm_metadata_record()], mf_lookup, synthesis_messages)

    assert 'ENA-C1' in mf_lookup
    assert len(synthesis_messages) == 1


# ================================
# TESTS for _load_mf_object - help from Codex

def arm_monitoring_feature_access():
    catalog = CatalogSqlAlchemy()
    plugin = ARMDataSourcePlugin(catalog)
    catalog.initialize([plugin])
    return plugin.access_classes[MonitoringFeature]


def arm_measurement_timeseries_access():
    catalog = CatalogSqlAlchemy()
    plugin = ARMDataSourcePlugin(catalog)
    catalog.initialize([plugin])
    return plugin.access_classes[MeasurementTimeseriesTVPObservation]


def arm_mf_info(variables):
    return {
        'site_id': 'ENA',
        'site_name': 'Eastern North Atlantic',
        'facility_id': 'C1',
        'name': 'Eastern North Atlantic - Graciosa Island, Azores, Portugal',
        'lat': 39.0916,
        'long': -28.0257,
        'variables': variables,
    }


@pytest.mark.parametrize(
    'variables,expected_mappings',
    [pytest.param(['temp', 'rh', 'atmos_pressure'],
                  [('temp', 'AT'), ('rh', 'RH'), ('atmos_pressure', 'APA')],
                  id='all_variables_mapped'),
     pytest.param(['temp', 'not_in_arm_mapping'],
                  [('temp', 'AT'), ('not_in_arm_mapping', NO_MAPPING_TEXT)],
                  id='mixed_mapped_and_unmapped_variables'),
     pytest.param(['not_in_arm_mapping_a', 'not_in_arm_mapping_b'],
                  [('not_in_arm_mapping_a', NO_MAPPING_TEXT), ('not_in_arm_mapping_b', NO_MAPPING_TEXT)],
                  id='all_variables_unmapped'),
     pytest.param([], None, id='empty_variables')])
def test_load_mf_object_builds_monitoring_feature(variables, expected_mappings):
    datasource = arm_monitoring_feature_access()
    mf_info = arm_mf_info(variables)

    result = _load_mf_object(datasource, 'ENA-C1', mf_info)

    assert result.id == 'ARM-ENA-C1'
    assert result.name == mf_info['name']
    assert result.feature_type == FeatureTypeEnum.POINT
    assert result.shape == SpatialSamplingShapes.SHAPE_POINT
    assert result.coordinates.absolute.horizontal_position[0].latitude == mf_info['lat']
    assert result.coordinates.absolute.horizontal_position[0].longitude == mf_info['long']
    assert result.coordinates.absolute.horizontal_position[0].datum == 'WGS84'
    assert result.coordinates.absolute.horizontal_position[0].units == GeographicCoordinate.UNITS_DEC_DEGREES

    related_feature = result.related_sampling_feature_complex[0]
    assert related_feature.id == 'ARM-ENA'
    assert related_feature.related_sampling_feature == 'ARM-Eastern North Atlantic'
    assert related_feature.related_sampling_feature_type == FeatureTypeEnum.SITE
    assert related_feature.role == 'PARENT'

    if expected_mappings is None:
        assert result.observed_properties is None
    else:
        actual_mappings = [
            (mapping.get_datasource_vocab(), mapping.get_basin3d_vocab())
            for mapping in result.observed_properties
        ]
        assert actual_mappings == expected_mappings


# ================================
# TESTS for _get_selected_arm_metadata - help from Codex

def populate_selected_metadata(metadata_results, mf_lookup, synthesis_messages):
    for mf_id in metadata_results:
        mf_lookup[mf_id] = {'id': mf_id}


def metadata_for_bbox(metadata_by_bbox):
    def get_metadata(url, bbox, synthesis_messages):
        return metadata_by_bbox.get(bbox, [])

    return get_metadata


def test_get_selected_arm_metadata_selects_one_named_feature():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_metadata', return_value=['ENA-C1']) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata) as mock_parse:
        mf_set, mf_lookup = _get_selected_arm_metadata(url, ['ENA-C1'], synthesis_messages)

    assert mf_set == {'ENA-C1'}
    assert mf_lookup == {'ENA-C1': {'id': 'ENA-C1'}}
    mock_get_metadata.assert_called_once_with(url, None, synthesis_messages)
    mock_parse.assert_called_once_with(['ENA-C1'], mf_lookup, synthesis_messages)
    assert synthesis_messages == []


def test_get_selected_arm_metadata_selects_multiple_named_features():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_metadata', return_value=['ENA-C1', 'SGP-C1']) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata):
        mf_set, mf_lookup = _get_selected_arm_metadata(url, ['ENA-C1', 'SGP-C1'], synthesis_messages)

    assert mf_set == {'ENA-C1', 'SGP-C1'}
    assert set(mf_lookup) == {'ENA-C1', 'SGP-C1'}
    mock_get_metadata.assert_called_once_with(url, None, synthesis_messages)
    assert synthesis_messages == []


def test_get_selected_arm_metadata_excludes_missing_named_feature():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    synthesis_messages = []
    missing_id = 'SGP-C1'

    with patch('basin3d.plugins.arm._get_arm_metadata', return_value=['ENA-C1']), \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        mf_set, mf_lookup = _get_selected_arm_metadata(url, ['ENA-C1', missing_id], synthesis_messages)

    expected_message = f'{missing_id} not found in ARM metadata for met.b1 data products'
    assert mf_set == {'ENA-C1'}
    assert set(mf_lookup) == {'ENA-C1'}
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


def test_get_selected_arm_metadata_selects_one_bbox():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox = (-99.5, 34.7, -94.7, 36.9)
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_metadata', return_value=['ENA-C1']) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata) as mock_parse:
        mf_set, mf_lookup = _get_selected_arm_metadata(url, [bbox], synthesis_messages)

    assert mf_set == {'ENA-C1'}
    assert mf_lookup == {'ENA-C1': {'id': 'ENA-C1'}}
    mock_get_metadata.assert_called_once_with(url, bbox, synthesis_messages)
    mock_parse.assert_called_once_with(['ENA-C1'], mf_lookup, synthesis_messages)
    assert synthesis_messages == []


def test_get_selected_arm_metadata_combines_mutually_exclusive_bboxes():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox_one = (-99.5, 34.7, -94.7, 36.9)
    bbox_two = (-110.0, 40.0, -105.0, 45.0)
    synthesis_messages = []
    metadata_by_bbox = {bbox_one: ['ENA-C1'], bbox_two: ['SGP-C1']}

    with patch('basin3d.plugins.arm._get_arm_metadata', side_effect=metadata_for_bbox(metadata_by_bbox)) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata):
        mf_set, mf_lookup = _get_selected_arm_metadata(url, [bbox_one, bbox_two], synthesis_messages)

    assert mf_set == {'ENA-C1', 'SGP-C1'}
    assert set(mf_lookup) == {'ENA-C1', 'SGP-C1'}
    assert mock_get_metadata.call_args_list == [
        call(url, bbox_one, synthesis_messages),
        call(url, bbox_two, synthesis_messages),
    ]
    assert synthesis_messages == []


def test_get_selected_arm_metadata_deduplicates_overlapping_bboxes():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox_one = (-99.5, 34.7, -94.7, 36.9)
    bbox_two = (-98.0, 35.0, -94.0, 37.0)
    synthesis_messages = []
    metadata_by_bbox = {bbox_one: ['ENA-C1', 'SGP-C1'], bbox_two: ['SGP-C1', 'NSA-C1']}

    with patch('basin3d.plugins.arm._get_arm_metadata', side_effect=metadata_for_bbox(metadata_by_bbox)), \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata):
        mf_set, mf_lookup = _get_selected_arm_metadata(url, [bbox_one, bbox_two], synthesis_messages)

    assert mf_set == {'ENA-C1', 'SGP-C1', 'NSA-C1'}
    assert set(mf_lookup) == {'ENA-C1', 'SGP-C1', 'NSA-C1'}
    assert synthesis_messages == []


def test_get_selected_arm_metadata_combines_mutually_exclusive_bbox_and_named_results():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox = (-99.5, 34.7, -94.7, 36.9)
    synthesis_messages = []
    metadata_by_bbox = {bbox: ['ENA-C1'], None: ['SGP-C1']}

    with patch('basin3d.plugins.arm._get_arm_metadata', side_effect=metadata_for_bbox(metadata_by_bbox)) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata):
        mf_set, mf_lookup = _get_selected_arm_metadata(url, [bbox, 'SGP-C1'], synthesis_messages)

    assert mf_set == {'ENA-C1', 'SGP-C1'}
    assert set(mf_lookup) == {'ENA-C1', 'SGP-C1'}
    assert mock_get_metadata.call_args_list == [
        call(url, bbox, synthesis_messages),
        call(url, None, synthesis_messages),
    ]
    assert synthesis_messages == []


def test_get_selected_arm_metadata_reuses_overlapping_bbox_and_named_result():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox = (-99.5, 34.7, -94.7, 36.9)
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_metadata', return_value=['ENA-C1']) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata):
        mf_set, mf_lookup = _get_selected_arm_metadata(url, [bbox, 'ENA-C1'], synthesis_messages)

    assert mf_set == {'ENA-C1'}
    assert set(mf_lookup) == {'ENA-C1'}
    mock_get_metadata.assert_called_once_with(url, bbox, synthesis_messages)
    assert synthesis_messages == []


def test_get_selected_arm_metadata_skips_empty_bbox_metadata():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox = (-99.5, 34.7, -94.7, 36.9)
    synthesis_messages = []

    with patch('basin3d.plugins.arm._get_arm_metadata', return_value={}) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata') as mock_parse:
        mf_set, mf_lookup = _get_selected_arm_metadata(url, [bbox], synthesis_messages)

    assert mf_set == set()
    assert mf_lookup == {}
    mock_get_metadata.assert_called_once_with(url, bbox, synthesis_messages)
    mock_parse.assert_not_called()
    assert synthesis_messages == []


def test_get_selected_arm_metadata_missing_named_after_bbox_triggers_one_full_lookup():
    url = 'https://metadata-api.svcs.arm.gov/metadata/data_product?data_product=met'
    bbox = (-99.5, 34.7, -94.7, 36.9)
    missing_id = 'SGP-C1'
    synthesis_messages = []
    metadata_by_bbox = {bbox: ['ENA-C1'], None: ['ENA-C1']}

    with patch('basin3d.plugins.arm._get_arm_metadata', side_effect=metadata_for_bbox(metadata_by_bbox)) as mock_get_metadata, \
            patch('basin3d.plugins.arm._parse_arm_metadata', side_effect=populate_selected_metadata), \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        mf_set, mf_lookup = _get_selected_arm_metadata(url, [bbox, 'ENA-C1', missing_id], synthesis_messages)

    expected_message = f'{missing_id} not found in ARM metadata for met.b1 data products'
    assert mf_set == {'ENA-C1'}
    assert set(mf_lookup) == {'ENA-C1'}
    assert mock_get_metadata.call_args_list == [
        call(url, bbox, synthesis_messages),
        call(url, None, synthesis_messages),
    ]
    assert synthesis_messages == [expected_message]
    mock_logger.assert_called_once_with(expected_message)


# ================================
# TESTS for _build_tvp_results - help from Codex

def test_build_tvp_results_returns_empty_for_empty_inputs():
    result_TVPs, result_TVP_quality, filtered_count = _build_tvp_results([], [], -9999.0, 1)

    assert result_TVPs == []
    assert result_TVP_quality == []
    assert filtered_count == 0


def test_build_tvp_results_preserves_values_when_no_conversion_is_needed():
    time_values = ['2024-01-01T00:00:00', '2024-01-01T01:00:00']
    data_values = np.array([1, 2.5])
    quality_values = np.array([0, 0])

    result_TVPs, result_TVP_quality, filtered_count = _build_tvp_results(
        time_values, data_values, -9999.0, 1, quality_values)

    assert result_TVPs == list(zip(time_values, data_values))
    assert result_TVP_quality == quality_values.tolist()
    assert all(type(result.value) in (int, float) for result in result_TVPs)
    assert all(type(quality) is int for quality in result_TVP_quality)
    assert len(result_TVPs) == len(result_TVP_quality)
    assert filtered_count == 0


def test_build_tvp_results_converts_values_but_preserves_missing_values():
    time_values = ['2024-01-01T00:00:00', '2024-01-01T01:00:00', '2024-01-01T02:00:00']
    missing_value = -9999.0
    data_values = np.array([10, missing_value, 2.5])
    quality_values = np.array([0, 1, 0])

    result_TVPs, result_TVP_quality, filtered_count = _build_tvp_results(
        time_values, data_values, missing_value, 0.4, quality_values)

    assert result_TVPs == list(zip(time_values, [4.0, missing_value, 1.0]))
    assert result_TVP_quality == quality_values.tolist()
    assert all(type(result.value) in (int, float) for result in result_TVPs)
    assert all(type(quality) is int for quality in result_TVP_quality)
    assert len(result_TVPs) == len(result_TVP_quality)
    assert filtered_count == 0


def test_build_tvp_results_filters_measurements_and_aligned_quality_values():
    time_values = ['t0', 't1', 't2', 't3', 't4', 't5']
    data_values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    quality_values = [0, 1, 2, 4, 8, 16]

    result_TVPs, result_TVP_quality, filtered_count = _build_tvp_results(
        time_values, data_values, -9999.0, 1, quality_values, ['0', '1', '2', '4', '8'])

    assert result_TVPs == list(zip(['t0', 't1', 't2', 't3', 't4'], [10.0, 20.0, 30.0, 40.0, 50.0]))
    assert result_TVP_quality == [0, 1, 2, 4, 8]
    assert all(type(result.value) in (int, float) for result in result_TVPs)
    assert all(type(quality) is int for quality in result_TVP_quality)
    assert len(result_TVPs) == len(result_TVP_quality)
    assert filtered_count == 1


def test_build_tvp_results_keeps_all_values_without_a_quality_filter():
    time_values = ['t0', 't1']
    data_values = [10.0, 20.0]
    quality_values = [0, 1]

    result_TVPs, result_TVP_quality, filtered_count = _build_tvp_results(
        time_values, data_values, -9999.0, 1, quality_values, [])

    assert result_TVPs == list(zip(time_values, data_values))
    assert result_TVP_quality == quality_values
    assert len(result_TVPs) == len(result_TVP_quality)
    assert filtered_count == 0


def test_build_tvp_results_keeps_values_when_quality_data_is_missing():
    time_values = ['t0', 't1']
    data_values = [10.0, 20.0]

    result_TVPs, result_TVP_quality, filtered_count = _build_tvp_results(
        time_values, data_values, -9999.0, 1, None, ['0']
    )

    assert result_TVPs == list(zip(time_values, data_values))
    assert result_TVP_quality == []
    assert filtered_count == 0


# ================================
# TESTS for ARMMeasurementTimeseriesTVPObservationAccess._get_unit_conv - help from Codex

def arm_unit_mapping(units):
    return Mock(basin3d_desc=[Mock(units=units)])


def test_get_unit_conv_warns_when_units_are_not_in_lookup():
    datasource = arm_measurement_timeseries_access()
    synthesis_messages = []
    arm_variable = 'temperature'
    arm_unit = 'unknown_unit'
    b3d_unit = 'degC'
    expected_message = (f'Unit for {arm_variable} was unexpected and unit conversion to BASIN-3D unit '
                       f'could not be assessed. Returning values in ARM native unit {arm_unit}.')

    with patch.object(datasource, 'get_datasource_attribute_mapping',
                      return_value=arm_unit_mapping(b3d_unit)) as mock_mapping, \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = datasource._get_unit_conv(arm_unit, arm_variable, synthesis_messages)

    assert result == (1, arm_unit)
    mock_mapping.assert_called_once_with('OBSERVED_PROPERTY', arm_variable)
    mock_logger.assert_called_once_with(expected_message)
    assert synthesis_messages == [expected_message]


@pytest.mark.parametrize(
    'arm_unit,b3d_unit,expected_result',
    [pytest.param('cm', 'm', (100, 'm'), id='integer_conversion'),
     pytest.param('degC', 'C', (1, 'C'), id='identity_conversion'),
     pytest.param('kPa', 'mm Hg', (0.4, 'mm Hg'), id='fractional_conversion')])
def test_get_unit_conv_returns_lookup_conversion(arm_unit, b3d_unit, expected_result):
    datasource = arm_measurement_timeseries_access()
    synthesis_messages = []
    arm_variable = 'temperature'

    with patch.object(datasource, 'get_datasource_attribute_mapping',
                      return_value=arm_unit_mapping(b3d_unit)) as mock_mapping, \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = datasource._get_unit_conv(arm_unit, arm_variable, synthesis_messages)

    assert result == expected_result
    mock_mapping.assert_called_once_with('OBSERVED_PROPERTY', arm_variable)
    mock_logger.assert_not_called()
    assert synthesis_messages == []


def test_get_unit_conv_returns_arm_unit_without_warning_when_units_match():
    datasource = arm_measurement_timeseries_access()
    synthesis_messages = ['existing message']
    arm_variable = 'temperature'
    arm_unit = 'C'

    with patch.object(datasource, 'get_datasource_attribute_mapping',
                      return_value=arm_unit_mapping(arm_unit)) as mock_mapping, \
            patch('basin3d.plugins.arm.logger.warning') as mock_logger:
        result = datasource._get_unit_conv(arm_unit, arm_variable, synthesis_messages)

    assert result == (1, arm_unit)
    mock_mapping.assert_called_once_with('OBSERVED_PROPERTY', arm_variable)
    mock_logger.assert_not_called()
    assert synthesis_messages == ['existing message']
