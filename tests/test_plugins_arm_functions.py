from unittest.mock import Mock, patch

import pytest

from basin3d.plugins.arm import _get_arm_metadata, _get_arm_request, _parse_arm_metadata


class DummyResponse:
    def __init__(self, data=None, status_code=200, text=None):
        self.data = data
        self.status_code = status_code
        self.text = text

    def json(self):
        return self.data


# ================================
# TESTS for _fetch_timeseries

# ================================
# TESTS for _build_tvp_results

# ================================
# TESTS for _collect_to_zarr

# ================================
# TESTS for _batch_files

# ================================
# TESTS for _get_mf_files

# ================================
# TESTS for _get_arm_request - with help from Codex

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
    [
        ({'message': 'invalid request'}, None, 'invalid request'),
        ({'error': 'service unavailable'}, None, 'service unavailable'),
        ({}, 'metadata service unavailable', 'metadata service unavailable'),
    ],
)
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
# TESTS for _get_arm_metadata - with help from Codex

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
# TESTS for _get_arm_data_files

# ================================
# TESTS for _parse_arm_metadata - with help from Codex

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
    [
        ('spatialCoverage', 'containedInPlace', 'identifier'),
        ('spatialCoverage', 'containedInPlace', 'name'),
        ('spatialCoverage', 'identifier'),
        ('spatialCoverage', 'name'),
        ('spatialCoverage', 'geo', 'latitude'),
        ('spatialCoverage', 'geo', 'longitude'),
    ],
)
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
# TESTS for _load_mf_object

# ================================
# TESTS for _get_selected_arm_metadata

# ================================
# TESTS for ARMMeasurementTimeseriesTVPObservationAccess._get_unit_conv
