from copy import deepcopy
import hashlib
import io
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import zipfile

import numpy as np
import pandas as pd
import pytest

from basin3d.core.catalog import CatalogSqlAlchemy
from basin3d.core.models import MeasurementTimeseriesTVPObservation, MonitoringFeature
from basin3d.core.schema.enum import FeatureTypeEnum, NO_MAPPING_TEXT
from basin3d.core.types import SpatialSamplingShapes
import basin3d.plugins.ameriflux as ameriflux


# ================================
# SHARED TEST HELPERS


def amf_site_metadata(**overrides):
    site_info = {
        'site_id': 'US-Test',
        'site_name': 'Test site',
        'description': 'Test description.',
        'latitude': 44.0,
        'longitude': -121.0,
        'elevation': None,
        'start_year': '2020',
        'end_year': '2024',
        'igbp': '',
        'url': None,
    }
    site_info.update(overrides)
    return ameriflux._SiteMetadata(**site_info)


def amf_measurement_timeseries_access():
    catalog = CatalogSqlAlchemy()
    plugin = ameriflux.AMFDataSourcePlugin(catalog)
    catalog.initialize([plugin])
    return plugin.access_classes[MeasurementTimeseriesTVPObservation]


# ================================
# TESTS for _require_amf_dependencies


def test_require_amf_dependencies_succeeds_when_xarray_is_available(monkeypatch):
    monkeypatch.setattr(ameriflux, 'xr', object())

    ameriflux._require_amf_dependencies()


def test_require_amf_dependencies_reports_missing_xarray(monkeypatch):
    monkeypatch.setattr(ameriflux, 'xr', None)

    with pytest.raises(ImportError, match='AmeriFlux plugin dependencies are not installed') as error:
        ameriflux._require_amf_dependencies()

    assert 'pip install "basin3d[ameriflux]"' in str(error.value)


def test_data_csv_to_zarr_requires_amf_dependencies(monkeypatch, tmp_path):
    require_dependencies = Mock(side_effect=ImportError('dependencies missing'))
    monkeypatch.setattr(ameriflux, '_require_amf_dependencies', require_dependencies)

    with pytest.raises(ImportError, match='dependencies missing'):
        ameriflux._data_csv_to_zarr(
            Mock(), SimpleNamespace(filename='data.csv'), tmp_path / 'data.zarr',
            'TIMESTAMP', '%Y%m%d', '2024-01-01', None, 2, [])

    require_dependencies.assert_called_once_with()


def test_data_csv_to_zarr_calls_amf_dependency_guard(monkeypatch, tmp_path):
    require_dependencies = Mock()
    monkeypatch.setattr(ameriflux, '_require_amf_dependencies', require_dependencies)
    archive_buffer, archive, member = amf_data_csv_member(
        'TIMESTAMP,TA_F\n20240101,25.0\n')

    try:
        result = ameriflux._data_csv_to_zarr(
            archive, member, tmp_path / 'data.zarr', 'TIMESTAMP', '%Y%m%d',
            '2024-01-01', None, 2, [])
    finally:
        archive.close()
        archive_buffer.close()

    assert result is not None
    result.close()
    require_dependencies.assert_called_once_with()


# ================================
# TESTS for _get_metadata

def test_get_metadata_returns_json_body(monkeypatch):
    expected_metadata = {'sites': ['AMF-site']}
    response = SimpleNamespace(status_code=200, json=lambda: expected_metadata)
    monkeypatch.setattr(ameriflux, 'get_url', lambda url: response)
    synthesis_messages = []

    result = ameriflux._get_metadata('https://amf.amf/metadata', synthesis_messages)

    assert result == expected_metadata
    assert synthesis_messages == []


@pytest.mark.parametrize('response, expected_status', [
    pytest.param(SimpleNamespace(status_code=404), '404', id='http-error'),
    pytest.param(None, 'NO RESPONSE', id='no-response'),
])
def test_get_metadata_records_http_failure(monkeypatch, response, expected_status):
    monkeypatch.setattr(ameriflux, 'get_url', lambda url: response)
    synthesis_messages = []

    result = ameriflux._get_metadata('https://amf.amf/metadata', synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        f'AMF request for https://amf.amf/metadata returned error code {expected_status}.'
    ]


def test_get_metadata_records_request_exception(monkeypatch):
    def raise_error(url):
        raise RuntimeError('request failed')

    monkeypatch.setattr(ameriflux, 'get_url', raise_error)
    synthesis_messages = []

    result = ameriflux._get_metadata('https://amf.amf/metadata', synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        'AMF request for https://amf.amf/metadata failed: request failed'
    ]


def test_get_metadata_records_json_exception(monkeypatch):
    def raise_json_error():
        raise ValueError('invalid JSON')

    response = SimpleNamespace(status_code=200, json=raise_json_error)
    monkeypatch.setattr(ameriflux, 'get_url', lambda url: response)
    synthesis_messages = []

    result = ameriflux._get_metadata('https://amf.amf/metadata', synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        'AMF request for https://amf.amf/metadata failed: invalid JSON'
    ]


def test_post_json_returns_response_json(monkeypatch):
    expected_json = {'result': 'ok'}
    response = SimpleNamespace(status_code=200, json=lambda: expected_json)
    monkeypatch.setattr(ameriflux, 'post_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._post_json(
        'https://amf.amf/test', {'site_ids': ['US-ABC']}, synthesis_messages)

    assert result == expected_json
    assert synthesis_messages == []


@pytest.mark.parametrize('response_json, response_text, expected_detail', [
    pytest.param({'message': 'request rejected'}, 'fallback text', 'request rejected',
                 id='json-message'),
    pytest.param({'error': 'request failed'}, 'fallback text', 'request failed',
                 id='json-error'),
    pytest.param({}, 'response text', 'response text', id='text-fallback'),
])
def test_post_json_records_http_error_detail(
        monkeypatch, response_json, response_text, expected_detail):
    url = 'https://amf.amf/test'
    response = SimpleNamespace(
        status_code=400,
        json=lambda: response_json,
        text=response_text,
    )
    monkeypatch.setattr(ameriflux, 'post_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._post_json(url, {}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        f'AMF POST request for {url} returned error code 400: {expected_detail}.'
    ]


@pytest.mark.parametrize('response, expected_status', [
    pytest.param(None, 'NO RESPONSE', id='no-response'),
    pytest.param(SimpleNamespace(status_code=400), '400', id='http-error'),
])
def test_post_json_records_http_failure(monkeypatch, response, expected_status):
    url = 'https://amf.amf/test'
    monkeypatch.setattr(ameriflux, 'post_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._post_json(url, {}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        f'AMF POST request for {url} returned error code {expected_status}.'
    ]


def test_post_json_records_request_exception(monkeypatch):
    def raise_error(*args, **kwargs):
        raise RuntimeError('request failed')

    url = 'https://amf.amf/test'
    monkeypatch.setattr(ameriflux, 'post_url', raise_error)
    synthesis_messages = []

    result = ameriflux._post_json(url, {}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        f'AMF POST request for {url} failed: request failed'
    ]


def test_post_json_records_json_exception(monkeypatch):
    def raise_json_error():
        raise ValueError('invalid JSON')

    url = 'https://amf.amf/test'
    response = SimpleNamespace(status_code=200, json=raise_json_error)
    monkeypatch.setattr(ameriflux, 'post_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._post_json(url, {}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        f'AMF POST request for {url} failed: invalid JSON'
    ]


def test_get_citation_info_returns_values_and_builds_request(monkeypatch):
    url_base = 'https://amf.amf/api'
    site_ids = ['US-ABC', 'US-XYZ']
    expected_citations = [
        {'site_id': 'US-ABC', 'citation': 'Citation A'},
        {'site_id': 'US-XYZ', 'citation': 'Citation B'},
    ]
    synthesis_messages = []
    request = {}

    def post_request(url, payload, messages):
        request.update(url=url, payload=payload, messages=messages)
        return {'values': expected_citations}

    monkeypatch.setattr(ameriflux, '_post_json', post_request)

    result = ameriflux._get_citation_info(
        url_base, site_ids, synthesis_messages)

    assert result == expected_citations
    assert request == {
        'url': f'{url_base}/citations/FLUXNET',
        'payload': {'site_ids': site_ids},
        'messages': synthesis_messages,
    }
    assert synthesis_messages == []


@pytest.mark.parametrize('citation_response', [
    pytest.param({}, id='empty-dictionary'),
    pytest.param({'values': []}, id='empty-values'),
])
def test_get_citation_info_returns_empty_for_empty_response(
        monkeypatch, citation_response):
    synthesis_messages = []
    monkeypatch.setattr(
        ameriflux, '_post_json', lambda *args, **kwargs: citation_response)

    result = ameriflux._get_citation_info(
        'https://amf.amf/api', ['US-ABC'], synthesis_messages)

    assert result == []
    assert synthesis_messages == []


@pytest.mark.parametrize('citation_response', [
    pytest.param([], id='list'),
    pytest.param(None, id='none'),
    pytest.param('invalid', id='string'),
])
def test_get_citation_info_records_non_dictionary_response(
        monkeypatch, citation_response):
    url = 'https://amf.amf/api/citations/FLUXNET'
    synthesis_messages = []
    monkeypatch.setattr(
        ameriflux, '_post_json', lambda *args, **kwargs: citation_response)

    result = ameriflux._get_citation_info(
        'https://amf.amf/api', ['US-ABC'], synthesis_messages)

    assert result == []
    assert synthesis_messages == [
        f'AMF citation response from {url} was not in expected Dictionary format.'
    ]


@pytest.mark.parametrize('citation_response', [
    pytest.param({'values': None}, id='none'),
    pytest.param({'values': 'invalid'}, id='string'),
    pytest.param({'values': {}}, id='dictionary'),
])
def test_get_citation_info_records_invalid_values(
        monkeypatch, citation_response):
    synthesis_messages = []
    monkeypatch.setattr(
        ameriflux, '_post_json', lambda *args, **kwargs: citation_response)

    result = ameriflux._get_citation_info(
        'https://amf.amf/api', ['US-ABC'], synthesis_messages)

    assert result == []
    assert synthesis_messages == [
        'AMF citation information was not in expected format. '
        'Cannot extract citation information.'
    ]


def test_get_citation_info_preserves_post_error(monkeypatch):
    synthesis_messages = ['AMF POST request failed']
    monkeypatch.setattr(ameriflux, '_post_json', lambda *args, **kwargs: {})

    result = ameriflux._get_citation_info(
        'https://amf.amf/api', ['US-ABC'], synthesis_messages)

    assert result == []
    assert synthesis_messages == ['AMF POST request failed']


def test_parse_citations_maps_site_ids_to_citations():
    citation_list = [
        {'site_id': 'US-ABC', 'citation': 'Citation A'},
        {'site_id': 'US-XYZ', 'citation': 'Citation B'},
    ]

    # The AmeriFlux API is expected to return one citation record per unique site ID.
    assert ameriflux._parse_citations(citation_list) == {
        'US-ABC': 'Citation A',
        'US-XYZ': 'Citation B',
    }


def test_parse_citations_returns_empty_dictionary_for_empty_input():
    assert ameriflux._parse_citations([]) == {}


def test_parse_citations_skips_records_without_site_id():
    citation_list = [
        {'site_id': 'US-ABC', 'citation': 'Citation A'},
        {'citation': 'Citation without site ID'},
        {'site_id': None, 'citation': 'Citation with null site ID'},
    ]

    assert ameriflux._parse_citations(citation_list) == {
        'US-ABC': 'Citation A',
    }


def test_parse_citations_preserves_missing_citation_value():
    assert ameriflux._parse_citations([{'site_id': 'US-ABC'}]) == {
        'US-ABC': None,
    }


def test_parse_citations_preserves_empty_citation():
    assert ameriflux._parse_citations([
        {'site_id': 'US-ABC', 'citation': ''},
    ]) == {'US-ABC': ''}


# ================================
# TESTS for _parse_metadata


def amf_metadata():
    return {
        'values': [
            {
                'site_id': 'AMF-Test-A',
                'site_name': 'Test Site A',
                'site_desc': 'Test site A description.',
                'grp_location': {
                    'location_lat': '44.4534',
                    'location_long': '-121.5619',
                    'location_elev': '1253',
                },
                'grp_igbp': {'igbp': 'ENF'},
                'url_ameriflux': 'https://amf.amf/sites/AMF-Test-A',
                'grp_publish_fluxnet': [2022, 2023],
            },
            {
                'site_id': 'AMF-Test-B',
                'site_name': 'Test Site B',
                'site_desc': 'Test site B description.',
                'grp_location': {
                    'location_lat': '39.1006',
                    'location_long': '-105.1024',
                    'location_elev': '2367',
                },
                'grp_igbp': {'igbp': 'SAV'},
                'url_ameriflux': 'https://amf.amf/sites/AMF-Test-B',
                'grp_publish_fluxnet': [2024, 2025],
            },
        ]
    }


def amf_valid_metadata_record():
    return deepcopy(amf_metadata()['values'][0])


def test_parse_metadata_parses_fake_sites():
    synthesis_messages = []

    result = ameriflux._parse_metadata(amf_metadata(), synthesis_messages)

    assert synthesis_messages == []
    assert set(['AMF-Test-A', 'AMF-Test-B']) == set(result)

    site_a = result['AMF-Test-A']
    assert site_a.site_name == 'Test Site A'
    assert site_a.latitude == 44.4534
    assert site_a.longitude == -121.5619
    assert site_a.elevation == 1253.0
    assert site_a.start_year == 2022
    assert site_a.end_year == 2023
    assert site_a.igbp == 'ENF'
    assert site_a.url == 'https://amf.amf/sites/AMF-Test-A'

    site_b = result['AMF-Test-B']
    assert site_b.site_name == 'Test Site B'
    assert site_b.latitude == 39.1006
    assert site_b.longitude == -105.1024
    assert site_b.elevation == 2367.0
    assert site_b.start_year == 2024
    assert site_b.end_year == 2025
    assert site_b.igbp == 'SAV'
    assert site_b.url == 'https://amf.amf/sites/AMF-Test-B'


@pytest.mark.parametrize('metadata', [
    pytest.param({}, id='empty-metadata'),
    pytest.param({'values': []}, id='empty-values'),
    pytest.param({'values': {}}, id='mapping-values'),
    pytest.param({'values': 'invalid'}, id='invalid-values'),
])
def test_parse_metadata_returns_empty_for_invalid_top_level_metadata(metadata):
    synthesis_messages = []

    result = ameriflux._parse_metadata(metadata, synthesis_messages)

    assert result == {}
    assert synthesis_messages == ['AMF metadata information was not in expected format.']


@pytest.mark.parametrize('fluxnet_years', [
    pytest.param(None, id='none'), pytest.param([], id='empty-list'),
    pytest.param('2022-2023', id='range-string')])
def test_parse_metadata_skips_records_without_fluxnet_years(fluxnet_years):
    record = amf_valid_metadata_record()
    record['grp_publish_fluxnet'] = fluxnet_years
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == []


def test_parse_metadata_skips_record_without_site_id():
    record = amf_valid_metadata_record()
    del record['site_id']
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == []


@pytest.mark.parametrize('location', [
    pytest.param({}, id='empty-location'),
    pytest.param({'location_lat': '44.4534'}, id='missing-longitude'),
    pytest.param(
        {'location_lat': '44.4534', 'location_long': None},
        id='null-longitude'),
])
def test_parse_metadata_skips_records_without_valid_location(location):
    record = amf_valid_metadata_record()
    record['grp_location'] = location
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == []


def test_parse_metadata_accepts_zero_coordinate_values():
    record = amf_valid_metadata_record()
    record['grp_location'] = {'location_lat': 0, 'location_long': 0}
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result['AMF-Test-A'].latitude == 0.0
    assert result['AMF-Test-A'].longitude == 0.0
    assert synthesis_messages == []


@pytest.mark.parametrize('location', [
    pytest.param(
        {'location_lat': 'invalid', 'location_long': '-121.0'},
        id='invalid-latitude'),
    pytest.param(
        {'location_lat': '44.4534', 'location_long': 'invalid'},
        id='invalid-longitude'),
])
def test_parse_metadata_reports_invalid_location_values(location):
    record = amf_valid_metadata_record()
    record['grp_location'] = location
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result == {}
    assert synthesis_messages == [
        'AMF AMF-Test-A latitude and longitude values could not be converted to numeric values. Skipping site.'
    ]


def test_parse_metadata_keeps_site_when_elevation_is_invalid():
    record = amf_valid_metadata_record()
    record['grp_location']['location_elev'] = 'invalid'
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result['AMF-Test-A'].elevation is None
    assert synthesis_messages == [
        'AMF AMF-Test-A elevation values could not be converted to numeric values.'
    ]


def test_parse_metadata_uses_defaults_for_missing_optional_metadata():
    record = amf_valid_metadata_record()
    record.pop('site_name')
    record.pop('site_desc')
    record.pop('grp_igbp')
    record.pop('url_ameriflux')
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result['AMF-Test-A'].site_name == 'NO VALUE'
    assert result['AMF-Test-A'].description == 'NO VALUE'
    assert result['AMF-Test-A'].igbp == 'NO VALUE'
    assert result['AMF-Test-A'].url is None
    assert synthesis_messages == []


def test_parse_metadata_continues_after_invalid_record():
    invalid_record = amf_valid_metadata_record()
    invalid_record['grp_location']['location_lat'] = 'invalid'
    valid_record = deepcopy(amf_valid_metadata_record())
    valid_record['site_id'] = 'US-Valid'
    synthesis_messages = []

    result = ameriflux._parse_metadata(
        {'values': [invalid_record, valid_record]}, synthesis_messages)

    assert 'US-Valid' in result
    assert 'AMF-Test-A' not in result
    assert len(synthesis_messages) == 1


def test_parse_metadata_preserves_fluxnet_year_boundaries():
    record = amf_valid_metadata_record()
    record['grp_publish_fluxnet'] = [2018, 2020, 2024]
    synthesis_messages = []

    result = ameriflux._parse_metadata({'values': [record]}, synthesis_messages)

    assert result['AMF-Test-A'].start_year == 2018
    assert result['AMF-Test-A'].end_year == 2024
    assert synthesis_messages == []


# ================================
# TESTS for _inside_bbox


def test_inside_bbox_returns_true_for_point_inside():
    bbox = (-122.0, 39.0, -120.0, 45.0)

    assert ameriflux._inside_bbox(bbox, lat=42.0, lon=-121.0) is True


@pytest.mark.parametrize('lat, lon', [
    pytest.param(42.0, -122.0, id='west-boundary'),
    pytest.param(42.0, -120.0, id='east-boundary'),
    pytest.param(39.0, -121.0, id='south-boundary'),
    pytest.param(45.0, -121.0, id='north-boundary'),
])
def test_inside_bbox_includes_boundary_points(lat, lon):
    bbox = (-122.0, 39.0, -120.0, 45.0)

    assert ameriflux._inside_bbox(bbox, lat=lat, lon=lon) is True


@pytest.mark.parametrize('lat, lon', [
    pytest.param(42.0, -122.1, id='west-outside'),
    pytest.param(42.0, -119.9, id='east-outside'),
])
def test_inside_bbox_returns_false_for_longitude_outside_bbox(lat, lon):
    bbox = (-122.0, 39.0, -120.0, 45.0)

    assert ameriflux._inside_bbox(bbox, lat=lat, lon=lon) is False


@pytest.mark.parametrize('lat, lon', [
    pytest.param(38.9, -121.0, id='south-outside'),
    pytest.param(45.1, -121.0, id='north-outside'),
])
def test_inside_bbox_returns_false_for_latitude_outside_bbox(lat, lon):
    bbox = (-122.0, 39.0, -120.0, 45.0)

    assert ameriflux._inside_bbox(bbox, lat=lat, lon=lon) is False


def test_inside_bbox_handles_negative_coordinates():
    bbox = (-105.5, 38.5, -104.5, 39.5)

    assert ameriflux._inside_bbox(bbox, lat=39.1, lon=-105.1) is True


def test_inside_bbox_uses_longitude_for_horizontal_bounds():
    bbox = (-105.5, 38.5, -104.5, 39.5)

    assert ameriflux._inside_bbox(bbox, lat=-105.1, lon=39.1) is False


# ================================
# TESTS for _matches_named


@pytest.mark.parametrize('site_id, named_ids, expected', [
    pytest.param('Test-A', {'Test-A'}, True, id='exact-match'),
    pytest.param('Test-B', {'Test-A', 'Test-B'}, True, id='multiple-ids'),
    pytest.param('Test-C', {'Test-A', 'Test-B'}, False, id='missing-id'),
    pytest.param('Test-A1', {'Test-A'}, False, id='suffix-difference'),
    pytest.param('test-a', {'Test-A'}, False, id='case-difference'),
])
def test_matches_named_uses_exact_case_sensitive_matches(
        site_id, named_ids, expected):
    assert ameriflux._matches_named(site_id, named_ids) is expected


def test_matches_named_returns_false_for_empty_named_ids():
    assert ameriflux._matches_named('Test-A', set()) is False


# ================================
# TESTS for _matches_any_bbox


def test_matches_any_bbox_returns_true_for_matching_bbox():
    site_info = amf_site_metadata(latitude=42.0, longitude=-121.0)
    bounding_boxes = [(-122.0, 39.0, -120.0, 45.0)]

    assert ameriflux._matches_any_bbox(site_info, bounding_boxes) is True


def test_matches_any_bbox_returns_true_when_one_bbox_matches():
    site_info = amf_site_metadata(latitude=42.0, longitude=-121.0)
    bounding_boxes = [
        (-110.0, 35.0, -100.0, 40.0),
        (-122.0, 39.0, -120.0, 45.0),
    ]

    assert ameriflux._matches_any_bbox(site_info, bounding_boxes) is True


def test_matches_any_bbox_returns_false_when_no_bbox_matches():
    site_info = amf_site_metadata(latitude=42.0, longitude=-121.0)
    bounding_boxes = [
        (-110.0, 35.0, -100.0, 40.0),
        (-90.0, 45.0, -80.0, 50.0),
    ]

    assert ameriflux._matches_any_bbox(site_info, bounding_boxes) is False


def test_matches_any_bbox_returns_false_for_empty_bbox_list():
    site_info = amf_site_metadata(latitude=42.0, longitude=-121.0)

    assert ameriflux._matches_any_bbox(site_info, []) is False


@pytest.mark.parametrize('latitude, longitude', [
    pytest.param(39.0, -121.0, id='south-boundary'),
    pytest.param(45.0, -121.0, id='north-boundary'),
    pytest.param(42.0, -122.0, id='west-boundary'),
    pytest.param(42.0, -120.0, id='east-boundary'),
])
def test_matches_any_bbox_includes_boundary_coordinates(latitude, longitude):
    site_info = amf_site_metadata(latitude=latitude, longitude=longitude)
    bounding_boxes = [(-122.0, 39.0, -120.0, 45.0)]

    assert ameriflux._matches_any_bbox(site_info, bounding_boxes) is True


def test_matches_any_bbox_handles_negative_coordinates():
    site_info = amf_site_metadata(latitude=-40.0, longitude=-105.0)
    bounding_boxes = [(-110.0, -45.0, -100.0, -35.0)]

    assert ameriflux._matches_any_bbox(site_info, bounding_boxes) is True


# ================================
# TESTS for _filter_sites


def test_filter_sites_includes_named_site_when_bbox_does_not_match():
    site_info = amf_site_metadata(site_id='Test-A', latitude=10.0, longitude=10.0)

    result = ameriflux._filter_sites(
        {'Test-A': site_info}, {'Test-A'}, [(-2.0, -2.0, 2.0, 2.0)], 2022, 2023)

    assert result == ['Test-A']


def test_filter_sites_includes_site_matching_bbox():
    site_info = amf_site_metadata(site_id='Test-A', latitude=42.0, longitude=-121.0)

    result = ameriflux._filter_sites(
        {'Test-A': site_info}, set(), [(-122.0, 39.0, -120.0, 45.0)], 2022, 2023)

    assert result == ['Test-A']


def test_filter_sites_excludes_site_without_spatial_match():
    site_info = amf_site_metadata(site_id='Test-A', latitude=10.0, longitude=10.0)

    result = ameriflux._filter_sites(
        {'Test-A': site_info}, {'Test-B'}, [(-2.0, -2.0, 2.0, 2.0)], 2022, 2023)

    assert result == []


def test_filter_sites_excludes_sites_for_empty_spatial_filters():
    """This should not happen in practice b/c if there are no monitoring features, the code doesn't get this far"""
    site_info = amf_site_metadata(site_id='Test-A')

    result = ameriflux._filter_sites({'Test-A': site_info}, set(), [], 2022, 2023)

    assert result == []


def test_filter_sites_allows_query_without_end_year():
    site_info = amf_site_metadata(site_id='Test-A', start_year='2020', end_year='2024')

    result = ameriflux._filter_sites({'Test-A': site_info}, {'Test-A'}, [], 2024, None)

    assert result == ['Test-A']


def test_filter_sites_excludes_site_that_ended_before_query():
    site_info = amf_site_metadata(site_id='Test-A', start_year='2018', end_year='2020')

    result = ameriflux._filter_sites({'Test-A': site_info}, {'Test-A'}, [], 2021, None)

    assert result == []


def test_filter_sites_excludes_site_that_started_after_query():
    site_info = amf_site_metadata(site_id='Test-A', start_year='2024', end_year='2025')

    result = ameriflux._filter_sites({'Test-A': site_info}, {'Test-A'}, [], 2020, 2023)

    assert result == []


def test_filter_sites_includes_overlapping_date_boundaries():
    site_info = amf_site_metadata(site_id='Test-A', start_year='2020', end_year='2024')

    result = ameriflux._filter_sites({'Test-A': site_info}, {'Test-A'}, [], 2024, 2024)

    assert result == ['Test-A']


def test_filter_sites_returns_qualifying_sites_in_lookup_order():
    sites = {
        'Test-A': amf_site_metadata(site_id='Test-A', start_year='2020', end_year='2024'),
        'Test-B': amf_site_metadata(site_id='Test-B', start_year='2010', end_year='2015'),
        'Test-C': amf_site_metadata(site_id='Test-C', latitude=42.0, longitude=-121.0),
        'Test-D': amf_site_metadata(site_id='Test-D', latitude=10.0, longitude=10.0),
    }

    result = ameriflux._filter_sites(
        sites, {'Test-A', 'Test-B'}, [(-122.0, 39.0, -120.0, 45.0)], 2020, 2023)

    assert result == ['Test-A', 'Test-C']


# ================================
# TESTS for _get_variable_names


def test_get_variable_names_returns_available_standard_variable():
    result = ameriflux._get_variable_names('TA_F', {'TA_F', 'SWC_F_MDS_1'})

    assert result == ['TA_F']


def test_get_variable_names_returns_empty_for_unavailable_standard_variable():
    result = ameriflux._get_variable_names('TA_F', {'SWC_F_MDS_1'})

    assert result == []


def test_get_variable_names_returns_indexed_temperature_variables_in_numeric_order():
    available_variables = {'TS_F_MDS_10', 'TS_F_MDS_2', 'TS_F_MDS_1'}

    result = ameriflux._get_variable_names('TS_F_MDS', available_variables)

    assert result == ['TS_F_MDS_1', 'TS_F_MDS_2', 'TS_F_MDS_10']


def test_get_variable_names_returns_indexed_soil_water_variables():
    available_variables = {'SWC_F_MDS_3', 'SWC_F_MDS_1', 'SWC_F_MDS_2'}

    result = ameriflux._get_variable_names('SWC_F_MDS', available_variables)

    assert result == ['SWC_F_MDS_1', 'SWC_F_MDS_2', 'SWC_F_MDS_3']


def test_get_variable_names_ignores_non_numeric_index_suffixes():
    available_variables = {
        'TS_F_MDS', 'TS_F_MDS_1', 'TS_F_MDS_QC', 'TS_F_MDS_A'}

    result = ameriflux._get_variable_names('TS_F_MDS', available_variables)

    assert result == ['TS_F_MDS_1']


def test_get_variable_names_returns_empty_without_indexed_instances():
    available_variables = {'TS_F_MDS', 'TS_F_MDS_QC', 'TA_F'}

    result = ameriflux._get_variable_names('TS_F_MDS', available_variables)

    assert result == []


def test_get_variable_names_returns_empty_for_unknown_variable():
    result = ameriflux._get_variable_names('UNKNOWN', {'TA_F', 'SWC_F_MDS_1'})

    assert result == []


# ================================
# TESTS for _get_height_depth_changes


def test_get_height_depth_changes_records_initial_height():
    var_info = [{'VAR_INFO_HEIGHT': '2.5'}]

    result = ameriflux._get_height_depth_changes(var_info)

    assert result == [(2.5, None)]


def test_get_height_depth_changes_records_subsequent_height_changes():
    var_info = [
        {'VAR_INFO_HEIGHT': '2.5'},
        {'VAR_INFO_HEIGHT': '5.0', 'VAR_INFO_DATE': '2023-01-01'},
        {'VAR_INFO_HEIGHT': '7.5', 'VAR_INFO_DATE': '2024-01-01'},
    ]

    result = ameriflux._get_height_depth_changes(var_info)

    assert result == [
        (2.5, None),
        (5.0, '2023-01-01'),
        (7.5, '2024-01-01'),
    ]


def test_get_height_depth_changes_ignores_repeated_heights():
    var_info = [
        {'VAR_INFO_HEIGHT': '2.5'},
        {'VAR_INFO_HEIGHT': '2.5', 'VAR_INFO_DATE': '2023-01-01'},
        {'VAR_INFO_HEIGHT': '5.0', 'VAR_INFO_DATE': '2024-01-01'},
        {'VAR_INFO_HEIGHT': '5.0', 'VAR_INFO_DATE': '2025-01-01'},
        {'VAR_INFO_HEIGHT': '2.5', 'VAR_INFO_DATE': '2026-01-01'},
    ]

    result = ameriflux._get_height_depth_changes(var_info)

    assert result == [
        (2.5, None),
        (5.0, '2024-01-01'),
        (2.5, '2026-01-01'),
    ]


def test_get_height_depth_changes_skips_entries_without_height():
    var_info = [
        {'VAR_INFO_DATE': '2022-01-01'},
        {'VAR_INFO_HEIGHT': '2.5'},
        {'VAR_INFO_DATE': '2023-01-01', 'OTHER': 'value'},
        {'VAR_INFO_HEIGHT': '5.0', 'VAR_INFO_DATE': '2024-01-01'},
    ]

    result = ameriflux._get_height_depth_changes(var_info)

    assert result == [(2.5, None), (5.0, '2024-01-01')]


def test_get_height_depth_changes_allows_missing_change_date():
    """This should never happen. Date is required field."""
    var_info = [
        {'VAR_INFO_HEIGHT': '2.5'},
        {'VAR_INFO_HEIGHT': '5.0'},
    ]

    result = ameriflux._get_height_depth_changes(var_info)

    assert result == [(2.5, None), (5.0, None)]


def test_get_height_depth_changes_converts_height_values_to_float():
    var_info = [
        {'VAR_INFO_HEIGHT': '2'},
        {'VAR_INFO_HEIGHT': '-1.0', 'VAR_INFO_DATE': '2023-01-01'},
    ]

    result = ameriflux._get_height_depth_changes(var_info)

    assert result == [(2.0, None), (-1.0, '2023-01-01')]


@pytest.mark.parametrize('var_info', [
    pytest.param([], id='empty-list'),
    pytest.param([{}], id='empty-entry'),
    pytest.param(
        [{'VAR_INFO_DATE': '2023-01-01'}], id='date-without-height'),
])
def test_get_height_depth_changes_returns_empty_without_heights(var_info):
    assert ameriflux._get_height_depth_changes(var_info) == []


# ================================
# TESTS for _format_tvp_timestamp


def test_format_tvp_timestamp_preserves_hourly_resolution():
    result = ameriflux._format_tvp_timestamp(
        '2023-12-25T14:30', 'HH')

    assert result == '2023-12-25T14:30'


@pytest.mark.parametrize('file_resolution, expected', [
    pytest.param('DD', '2023-12-25', id='daily'),
    pytest.param('MM', '2023-12', id='monthly'),
    pytest.param('YY', '2023', id='yearly'),
])
def test_format_tvp_timestamp_formats_non_hourly_resolutions(
        file_resolution, expected):
    result = ameriflux._format_tvp_timestamp(
        '2023-12-25T14:30', file_resolution)

    assert result == expected


def test_format_tvp_timestamp_formats_negative_utc_offset():
    result = ameriflux._format_tvp_timestamp(
        '2023-12-25T14:30', 'HH', '-7.0')

    assert result == '2023-12-25T14:30-07:00'


def test_format_tvp_timestamp_formats_fractional_utc_offset():
    result = ameriflux._format_tvp_timestamp(
        '2023-12-25T14:30', 'HH', '5.5')

    assert result == '2023-12-25T14:30+05:30'


@pytest.mark.parametrize('utc_offset', [
    pytest.param(None, id='none'), pytest.param('', id='empty-string')])
def test_format_tvp_timestamp_omits_missing_hourly_utc_offset(utc_offset):
    result = ameriflux._format_tvp_timestamp(
        '2023-12-25T14:30', 'HH', utc_offset)

    assert result == '2023-12-25T14:30'


@pytest.mark.parametrize('file_resolution, expected', [
    pytest.param('DD', '2023-12-25', id='daily'),
    pytest.param('MM', '2023-12', id='monthly'),
    pytest.param('YY', '2023', id='yearly'),
])
def test_format_tvp_timestamp_ignores_utc_offset_for_non_hourly_resolutions(
        file_resolution, expected):
    result = ameriflux._format_tvp_timestamp(
        '2023-12-25T14:30', file_resolution, '-7.0')

    assert result == expected


def test_format_tvp_timestamp_reduces_unexpected_finer_precision():
    # This should not occur because minute resolution is the finest expected.
    result = ameriflux._format_tvp_timestamp(
        '2023-12-25T14:30:45.123456789', 'HH')

    assert result == '2023-12-25T14:30'


# ================================
# TESTS for _build_tvp_results


@pytest.mark.parametrize(
    'time_values, data_values, unit_conv, expected_values',
    [
        pytest.param([], [], 1, [], id='empty-inputs'),
        pytest.param(['t0', 't1'], [10.0, 20.0], 1, [10.0, 20.0], id='no-conversion'),
        pytest.param(['t0', 't1'], [10.0, 2.5], 0.4, [4.0, 1.0], id='conversion'),
        pytest.param(['t0', 't1'], [10.0, -9999], 0.4, [4.0, -9999], id='missing-sentinel'),
    ])
def test_build_tvp_results_handles_value_conversion(
        monkeypatch, time_values, data_values, unit_conv, expected_values):
    # Empty inputs should not occur during normal processing.
    monkeypatch.setattr(ameriflux, '_format_tvp_timestamp',
                        lambda timestamp, resolution, offset: str(timestamp))

    result_tvp, result_quality, filtered_count = ameriflux._build_tvp_results(
        time_values, data_values, unit_conv, 'DD')

    assert [result.value for result in result_tvp] == expected_values
    assert result_quality == []
    assert filtered_count == 0


def test_build_tvp_results_converts_numpy_values_to_native_types(monkeypatch):
    monkeypatch.setattr(ameriflux, '_format_tvp_timestamp',
                        lambda timestamp, resolution, offset: str(timestamp))

    result_tvp, result_quality, filtered_count = ameriflux._build_tvp_results(
        ['t0'], np.array([10]), 0.5, 'DD', quality_values=np.array([1]))

    assert result_tvp[0].value == 5.0
    assert type(result_tvp[0].value) is float
    assert result_quality == [1]
    assert type(result_quality[0]) is int
    assert filtered_count == 0


@pytest.mark.parametrize(
    'quality_values, requested_qualities, expected_values, expected_quality, '
    'expected_filtered', [
        pytest.param([0, 1, 2], ['0', '2'], [10.0, 30.0], [0, 2], 1,
                     id='filters-and-aligns-quality'),
        pytest.param([0, 1], [], [10.0, 20.0], [0, 1], 0,
                     id='empty-quality-filter-keeps-all'),
        pytest.param(None, ['0'], [10.0, 20.0], [], 0,
                     id='quality-filter-without-quality-data'),
        pytest.param([0, 1], ['1'], [20.0], [1], 1,
                     id='string-quality-values'),
    ])
def test_build_tvp_results_handles_quality_filtering(
        monkeypatch, quality_values, requested_qualities, expected_values,
        expected_quality, expected_filtered):
    monkeypatch.setattr(ameriflux, '_format_tvp_timestamp',
                        lambda timestamp, resolution, offset: str(timestamp))

    result_tvp, result_quality, filtered_count = ameriflux._build_tvp_results(
        ['t0', 't1', 't2'][:len(expected_values) if quality_values is None else
                           len(quality_values)],
        [10.0, 20.0, 30.0][:len(expected_values) if quality_values is None else
                           len(quality_values)],
        1, 'DD', quality_values=quality_values,
        requested_qualities=requested_qualities)

    assert [result.value for result in result_tvp] == expected_values
    assert result_quality == expected_quality
    assert filtered_count == expected_filtered


def test_build_tvp_results_passes_resolution_and_offset_to_formatter(monkeypatch):
    formatter_calls = []

    def format_timestamp(timestamp, resolution, offset):
        formatter_calls.append((timestamp, resolution, offset))
        return f'formatted-{timestamp}'

    monkeypatch.setattr(ameriflux, '_format_tvp_timestamp', format_timestamp)

    result_tvp, _, _ = ameriflux._build_tvp_results(
        ['t0', 't1'], [10.0, 20.0], 1, 'HH', '-7.0',
        quality_values=[0, 1], requested_qualities=['1'])

    assert [result.timestamp for result in result_tvp] == ['formatted-t1']
    assert formatter_calls == [('t1', 'HH', '-7.0')]


# ================================
# TESTS for AMFMeasurementTimeseriesTVPObservationAccess._get_unit_conv


def test_get_unit_conv_returns_default_when_amf_unit_is_none(monkeypatch):
    datasource = amf_measurement_timeseries_access()
    synthesis_messages = []

    def fail_mapping(*args):
        pytest.fail('Unit mapping should not be requested for a missing unit.')

    monkeypatch.setattr(datasource, 'get_datasource_attribute_mapping', fail_mapping)

    result = datasource._get_unit_conv(None, 'TA_F', synthesis_messages)

    assert result == (1, None)
    assert synthesis_messages == []


@pytest.mark.parametrize('amf_unit, b3d_unit, expected', [
    pytest.param('W m-2', 'W/m2', (1, 'W/m2'), id='energy-flux'),
    pytest.param('m s-1', 'm/s', (1, 'm/s'), id='wind-speed'),
    pytest.param('Decimal degrees', 'degrees', (1, 'degrees'), id='degrees'),
    pytest.param('deg C', 'C', (1, 'C'), id='temperature'),
])
def test_get_unit_conv_returns_known_lookup_conversion(
        monkeypatch, amf_unit, b3d_unit, expected):
    datasource = amf_measurement_timeseries_access()
    synthesis_messages = []
    mapping_calls = []

    def get_mapping(*args):
        mapping_calls.append(args)
        return SimpleNamespace(
            basin3d_desc=[SimpleNamespace(units=b3d_unit)])

    monkeypatch.setattr(datasource, 'get_datasource_attribute_mapping', get_mapping)

    result = datasource._get_unit_conv(amf_unit, 'TA_F', synthesis_messages)

    assert result == expected
    assert mapping_calls == [('OBSERVED_PROPERTY', 'TA_F')]
    assert synthesis_messages == []


def test_get_unit_conv_returns_amf_unit_when_units_match_without_lookup(
        monkeypatch):
    datasource = amf_measurement_timeseries_access()
    synthesis_messages = ['existing message']
    mapping_calls = []

    def get_mapping(*args):
        mapping_calls.append(args)
        return SimpleNamespace(
            basin3d_desc=[SimpleNamespace(units='unknown')])

    monkeypatch.setattr(datasource, 'get_datasource_attribute_mapping', get_mapping)
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)

    result = datasource._get_unit_conv('unknown', 'TA_F', synthesis_messages)

    assert result == (1, 'unknown')
    assert mapping_calls == [('OBSERVED_PROPERTY', 'TA_F')]
    assert warning_messages == []
    assert synthesis_messages == ['existing message']


def test_get_unit_conv_warns_for_unexpected_mismatched_units(monkeypatch):
    datasource = amf_measurement_timeseries_access()
    synthesis_messages = []
    amf_unit = 'unknown_amf_unit'
    variable = 'TA_F'
    expected_message = (
        f'Unit for {variable} was unexpected and unit conversion to BASIN-3D '
        f'unit could not be assessed. Returning values in AMF native unit '
        f'{amf_unit}.')

    monkeypatch.setattr(
        datasource, 'get_datasource_attribute_mapping',
        lambda *args: SimpleNamespace(
            basin3d_desc=[SimpleNamespace(units='unknown_b3d_unit')]))
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)

    result = datasource._get_unit_conv(amf_unit, variable, synthesis_messages)

    assert result == (1, amf_unit)
    assert warning_messages == [expected_message]
    assert synthesis_messages == [expected_message]


# ================================
# TESTS for _get_download_info


def test_get_download_info_returns_data_urls_and_builds_payload(monkeypatch):
    response_data = [{'site_id': 'Test-A', 'url': 'https://amf.amf/data.zip'}]
    request = {}

    def post_request(url, payload, synthesis_messages):
        request.update(url=url, payload=payload, synthesis_messages=synthesis_messages)
        return {'data_urls': response_data}

    monkeypatch.setattr(ameriflux, '_post_json', post_request)
    monkeypatch.setattr(ameriflux, 'AMF_USER_NAME', 'test-user')
    monkeypatch.setattr(ameriflux, 'AMF_USER_EMAIL', 'test@amf.amf')
    monkeypatch.setattr(ameriflux, 'AMF_DATA_INTENDED_USE_ENUM', 'synthesis')
    monkeypatch.setattr(ameriflux, 'AMF_DATA_USE_DESC', 'test description')
    synthesis_messages = []

    result = ameriflux._get_download_info(
        'https://amf.amf/api', ['Test-A'], synthesis_messages)

    assert result == response_data
    assert request == {
        'url': 'https://amf.amf/api/data_download',
        'payload': {
            'user_id': 'test-user',
            'user_email': 'test@amf.amf',
            'site_ids': ['Test-A'],
            'intended_use': 'synthesis',
            'description': 'test description (data accessed via BASIN-3D)',
            'data_policy': 'CCBY4.0',
            'data_product': 'FLUXNET',
            'data_variant': 'FULLSET',
        },
        'synthesis_messages': synthesis_messages,
    }
    assert synthesis_messages == []


@pytest.mark.parametrize('intended_use', [
    pytest.param('unsupported', id='unsupported-string'),
    pytest.param(None, id='non-string'),
])
def test_get_download_info_uses_defaults_for_invalid_metadata(
        monkeypatch, intended_use):
    request = {}

    def post_request(url, payload, synthesis_messages):
        request.update(payload)
        return {'data_urls': []}

    monkeypatch.setattr(ameriflux, '_post_json', post_request)
    monkeypatch.setattr(ameriflux, 'AMF_DATA_INTENDED_USE_ENUM', intended_use)
    monkeypatch.setattr(ameriflux, 'AMF_DATA_USE_DESC', None)

    result = ameriflux._get_download_info('https://amf.amf/api', [], [])

    assert result == []
    assert request['intended_use'] == ameriflux.INTENDED_USE_ENUM[-1]
    assert request['description'] == (
        f'{ameriflux.DEFAULT_USE_DESC} (data accessed via BASIN-3D)')


@pytest.mark.parametrize('response_json, expected_message', [
    pytest.param(
        ['invalid'],
        'AMF download response for https://amf.amf/api/data_download was not '
        'in expected dictionary format. It was list class.',
        id='non-dictionary-response'),
    pytest.param(
        {'data_urls': 'invalid'},
        'AMF download response for https://amf.amf/api/data_download did not '
        'contain data_urls as a list. It was str class.',
        id='non-list-data-urls'),
])
def test_get_download_info_records_invalid_response_data(
        monkeypatch, response_json, expected_message):
    monkeypatch.setattr(ameriflux, '_post_json', lambda *args, **kwargs: response_json)
    synthesis_messages = []

    result = ameriflux._get_download_info(
        'https://amf.amf/api', ['Test-A'], synthesis_messages)

    assert result == []
    assert synthesis_messages == [expected_message]


def test_get_download_info_returns_empty_for_missing_data_urls(monkeypatch):
    monkeypatch.setattr(ameriflux, '_post_json', lambda *args, **kwargs: {'status': 'ok'})
    synthesis_messages = []

    result = ameriflux._get_download_info(
        'https://amf.amf/api', ['Test-A'], synthesis_messages)

    assert result == []
    assert synthesis_messages == []


# ================================
# TESTS for _make_download_lookup


def test_make_download_lookup_returns_empty_for_empty_input(monkeypatch):
    # An empty download list should not occur during normal processing.
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []

    result = ameriflux._make_download_lookup([], synthesis_messages)

    assert result == {}
    assert warning_messages == []
    assert synthesis_messages == []


def test_make_download_lookup_builds_single_entry(monkeypatch):
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []
    download_info = [{
        'site_id': 'Test-A',
        'url': 'https://amf.amf/Test-A.zip',
        'download_checksum': 'checksum-a',
    }]

    result = ameriflux._make_download_lookup(download_info, synthesis_messages)

    assert result == {
        'Test-A': {
            'url': 'https://amf.amf/Test-A.zip',
            'checksum': 'checksum-a',
        }
    }
    assert warning_messages == []
    assert synthesis_messages == []


def test_make_download_lookup_builds_multiple_entries(monkeypatch):
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []
    download_info = [
        {
            'site_id': 'Test-A',
            'url': 'https://amf.amf/Test-A.zip',
            'download_checksum': 'checksum-a',
        },
        {
            'site_id': 'Test-B',
            'url': 'https://amf.amf/Test-B.zip',
            'download_checksum': 'checksum-b',
        },
    ]

    result = ameriflux._make_download_lookup(download_info, synthesis_messages)

    assert result == {
        'Test-A': {'url': 'https://amf.amf/Test-A.zip', 'checksum': 'checksum-a'},
        'Test-B': {'url': 'https://amf.amf/Test-B.zip', 'checksum': 'checksum-b'},
    }
    assert warning_messages == []
    assert synthesis_messages == []


@pytest.mark.parametrize('missing_field', [
    pytest.param('site_id', id='missing-site-id'),
    pytest.param('url', id='missing-url'),
    pytest.param('download_checksum', id='missing-checksum'),
])
def test_make_download_lookup_skips_entries_with_missing_fields(
        monkeypatch, missing_field):
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []
    download_entry = {
        'site_id': 'Test-A',
        'url': 'https://amf.amf/Test-A.zip',
        'download_checksum': 'checksum-a',
    }
    del download_entry[missing_field]

    result = ameriflux._make_download_lookup([download_entry], synthesis_messages)

    expected_message = (
        'AMF download information from data_download request is missing '
        f"required fields ['{missing_field}']. Skipping entry.")
    assert result == {}
    assert warning_messages == [expected_message]
    assert synthesis_messages == [expected_message]


@pytest.mark.parametrize('empty_field', [
    pytest.param('site_id', id='empty-site-id'),
    pytest.param('url', id='empty-url'),
    pytest.param('download_checksum', id='empty-checksum'),
])
def test_make_download_lookup_skips_entries_with_empty_fields(
        monkeypatch, empty_field):
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []
    download_entry = {
        'site_id': 'Test-A',
        'url': 'https://amf.amf/Test-A.zip',
        'download_checksum': 'checksum-a',
    }
    download_entry[empty_field] = ''

    result = ameriflux._make_download_lookup([download_entry], synthesis_messages)

    expected_message = (
        'AMF download information from data_download request is missing '
        f"required fields ['{empty_field}']. Skipping entry.")
    assert result == {}
    assert warning_messages == [expected_message]
    assert synthesis_messages == [expected_message]


def test_make_download_lookup_skips_non_dictionary_entries(monkeypatch):
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []

    result = ameriflux._make_download_lookup(['invalid'], synthesis_messages)

    expected_message = (
        'AMF download information element from data_download request was not a '
        "dictionary. Skipping entry.")
    assert result == {}
    assert warning_messages == [expected_message]
    assert synthesis_messages == [expected_message]


def test_make_download_lookup_continues_after_invalid_entry(monkeypatch):
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []
    invalid_entry = {'site_id': 'Test-A', 'url': 'https://amf.amf/Test-A.zip'}
    valid_entry = {
        'site_id': 'Test-B',
        'url': 'https://amf.amf/Test-B.zip',
        'download_checksum': 'checksum-b',
    }

    result = ameriflux._make_download_lookup(
        [invalid_entry, valid_entry], synthesis_messages)

    expected_message = (
        'AMF download information from data_download request is missing '
        f"required fields ['download_checksum']. Skipping entry.")
    assert result == {
        'Test-B': {
            'url': 'https://amf.amf/Test-B.zip',
            'checksum': 'checksum-b',
        }
    }
    assert warning_messages == [expected_message]
    assert synthesis_messages == [expected_message]


def test_make_download_lookup_preserves_existing_messages(monkeypatch):
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = ['existing message']
    download_info = [{
        'site_id': 'Test-A',
        'url': 'https://amf.amf/Test-A.zip',
        'download_checksum': 'checksum-a',
    }]

    ameriflux._make_download_lookup(download_info, synthesis_messages)

    assert warning_messages == []
    assert synthesis_messages == ['existing message']


def test_make_download_lookup_later_duplicate_overwrites_earlier(monkeypatch):
    # Duplicate site IDs should not occur in a valid download response.
    warning_messages = []
    monkeypatch.setattr(ameriflux.logger, 'warning', warning_messages.append)
    synthesis_messages = []
    download_info = [
        {
            'site_id': 'Test-A',
            'url': 'https://amf.amf/old.zip',
            'download_checksum': 'old-checksum',
        },
        {
            'site_id': 'Test-A',
            'url': 'https://amf.amf/new.zip',
            'download_checksum': 'new-checksum',
        },
    ]

    result = ameriflux._make_download_lookup(download_info, synthesis_messages)

    assert result == {
        'Test-A': {
            'url': 'https://amf.amf/new.zip',
            'checksum': 'new-checksum',
        }
    }
    assert warning_messages == []
    assert synthesis_messages == []


# ================================
# TESTS for _validate_zarr_path


def test_validate_zarr_path_returns_true_for_existing_writable_directory(
        tmp_path, monkeypatch):
    error_messages = []
    monkeypatch.setattr(ameriflux.logger, 'error', error_messages.append)
    synthesis_messages = []

    result = ameriflux._validate_zarr_path(tmp_path, synthesis_messages)

    assert result is True
    assert error_messages == []
    assert synthesis_messages == []


@pytest.mark.parametrize('path_type', [
    pytest.param('missing', id='missing-path'),
    pytest.param('file', id='file-path'),
])
def test_validate_zarr_path_rejects_invalid_paths(tmp_path, monkeypatch, path_type):
    if path_type == 'missing':
        zarr_path = tmp_path / 'missing'
        expected_message = (
            f'AMF zarr path does not exist: {zarr_path}. The local directory '
            'for temporary files should be set as the environmental variable: '
            'BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
    else:
        zarr_path = tmp_path / 'not-a-directory'
        zarr_path.touch()
        expected_message = (
            f'AMF zarr path is not a directory: {zarr_path}. The local directory '
            'for temporary files should be set as the environmental variable: '
            'BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')

    error_messages = []
    monkeypatch.setattr(ameriflux.logger, 'error', error_messages.append)
    synthesis_messages = []

    result = ameriflux._validate_zarr_path(zarr_path, synthesis_messages)

    assert result is False
    assert error_messages == [expected_message]
    assert synthesis_messages == [expected_message]


def test_validate_zarr_path_rejects_unwritable_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(ameriflux.os, 'access', lambda path, mode: False)
    error_messages = []
    monkeypatch.setattr(ameriflux.logger, 'error', error_messages.append)
    synthesis_messages = []

    result = ameriflux._validate_zarr_path(tmp_path, synthesis_messages)

    expected_message = (
        f'AMF zarr path is not writable: {tmp_path}. The local directory for '
        'temporary files can be set as the environmental variable: '
        'BASIN3D_LOCAL_TEMP_DIR, otherwise the default working directory is '
        'used. Please double check your configuration.')
    assert result is False
    assert error_messages == [expected_message]
    assert synthesis_messages == [expected_message]


# ================================
# TESTS for _create_zarr_temp_dir


def test_create_zarr_temp_dir_creates_amf_child_and_preserves_parent_contents(
        tmp_path):
    parent_file = tmp_path / 'keep.txt'
    parent_file.write_text('keep')
    synthesis_messages = []

    child_path = ameriflux._create_zarr_temp_dir(str(tmp_path), synthesis_messages)

    assert child_path.parent == tmp_path
    assert child_path.is_dir()
    assert child_path.name.startswith('basin3d-amf-')
    assert parent_file.read_text() == 'keep'
    assert synthesis_messages == []

    child_path.rmdir()


def test_create_zarr_temp_dir_returns_none_for_invalid_path(tmp_path, monkeypatch):
    zarr_path = tmp_path / 'missing'
    error_messages = []
    monkeypatch.setattr(ameriflux.logger, 'error', error_messages.append)
    mkdtemp_calls = []
    monkeypatch.setattr(
        ameriflux.tempfile, 'mkdtemp',
        lambda **kwargs: mkdtemp_calls.append(kwargs))
    synthesis_messages = []

    result = ameriflux._create_zarr_temp_dir(str(zarr_path), synthesis_messages)

    expected_message = (
        f'AMF zarr path does not exist: {zarr_path}. The local directory for '
        'temporary files should be set as the environmental variable: '
        'BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
    assert result is None
    assert error_messages == [expected_message]
    assert synthesis_messages == [expected_message]
    assert mkdtemp_calls == []


def test_create_zarr_temp_dir_returns_none_when_creation_fails(
        tmp_path, monkeypatch):
    exception = OSError(28, 'No space left on device')
    monkeypatch.setattr(
        ameriflux.tempfile, 'mkdtemp',
        lambda **kwargs: (_ for _ in ()).throw(exception))
    error_messages = []
    monkeypatch.setattr(ameriflux.logger, 'error', error_messages.append)
    synthesis_messages = []

    result = ameriflux._create_zarr_temp_dir(str(tmp_path), synthesis_messages)

    expected_message = (
        f'Failed to create AMF zarr temporary directory in {tmp_path}: '
        f'{exception}')
    assert result is None
    assert error_messages == [expected_message]
    assert synthesis_messages == [expected_message]

# ================================
# TESTS for _download_zip_to_temp


@pytest.mark.parametrize('checksum', [
    pytest.param(None, id='none'),
    pytest.param(12345, id='non-string'),
    pytest.param('', id='empty-string'),
])
def test_download_zip_to_temp_rejects_invalid_checksum(monkeypatch, checksum):
    get_url_calls = []
    monkeypatch.setattr(
        ameriflux, 'get_url',
        lambda *args, **kwargs: get_url_calls.append((args, kwargs)))
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', checksum, synthesis_messages)

    assert result is None
    assert get_url_calls == []
    assert synthesis_messages == [
        'AMF ZIP download for https://amf.amf/Test-A.zip did not include a valid checksum.'
    ]


@pytest.mark.parametrize('checksum', [
    pytest.param('crc32:abcd', id='unsupported-algorithm'),
    pytest.param('abc', id='unsupported-length'),
    pytest.param('md5:', id='empty-digest'),
])
def test_download_zip_to_temp_rejects_unsupported_checksum_format(
        monkeypatch, checksum):
    get_url_calls = []
    monkeypatch.setattr(
        ameriflux, 'get_url',
        lambda *args, **kwargs: get_url_calls.append((args, kwargs)))
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', checksum, synthesis_messages)

    assert result is None
    assert get_url_calls == []
    assert synthesis_messages == [
        'AMF ZIP download for https://amf.amf/Test-A.zip had an unsupported checksum format.'
    ]


def test_download_zip_to_temp_streams_and_verifies_explicit_checksum(monkeypatch):
    content = b'zip-content'
    digest = hashlib.md5(content).hexdigest()
    close_calls = []
    response = SimpleNamespace(
        status_code=200,
        iter_content=lambda chunk_size: [b'zip-', b'', b'content'],
        close=lambda: close_calls.append(True),
    )
    get_url_calls = []
    monkeypatch.setattr(
        ameriflux, 'get_url',
        lambda *args, **kwargs: (get_url_calls.append((args, kwargs)) or response))
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', f'md5:{digest}', synthesis_messages)

    assert result is not None
    assert result.read_bytes() == content
    assert result.suffix == '.zip'
    assert get_url_calls == [(('https://amf.amf/Test-A.zip',), {'stream': True})]
    assert close_calls == [True]
    assert synthesis_messages == []
    result.unlink()


@pytest.mark.parametrize('algorithm', ['md5', 'sha1', 'sha256'])
def test_download_zip_to_temp_infers_checksum_algorithm(monkeypatch, algorithm):
    content = b'zip-content'
    digest = hashlib.new(algorithm, content).hexdigest()
    response = SimpleNamespace(
        status_code=200,
        iter_content=lambda chunk_size: [content],
        close=lambda: None,
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', digest.upper(), synthesis_messages)

    assert result is not None
    assert result.read_bytes() == content
    assert synthesis_messages == []
    result.unlink()


@pytest.mark.parametrize('response, expected_status', [
    pytest.param(SimpleNamespace(status_code=404, close=lambda: None),
                 '404', id='http-error'),
    pytest.param(None, 'NO RESPONSE', id='no-response'),
])
def test_download_zip_to_temp_records_http_failure(
        monkeypatch, response, expected_status):
    content = b'zip-content'
    checksum = hashlib.md5(content).hexdigest()
    close_calls = []
    if response is not None:
        response.close = lambda: close_calls.append(True)
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', checksum, synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        f'AMF data request for https://amf.amf/Test-A.zip returned error code {expected_status}.'
    ]
    assert close_calls == ([True] if response is not None else [])


@pytest.mark.parametrize('response_json, response_text, expected_detail', [
    pytest.param({'message': 'invalid request'}, None, 'invalid request',
                 id='json-message'),
    pytest.param({'error': 'service unavailable'}, None, 'service unavailable',
                 id='json-error'),
    pytest.param({}, 'download service unavailable',
                 'download service unavailable', id='text-fallback'),
])
def test_download_zip_to_temp_includes_http_error_detail(
        monkeypatch, response_json, response_text, expected_detail):
    close_calls = []
    response = SimpleNamespace(
        status_code=404,
        json=lambda: response_json,
        text=response_text,
        close=lambda: close_calls.append(True),
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', hashlib.md5(b'content').hexdigest(),
        synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF data request for https://amf.amf/Test-A.zip returned error code '
        f'404: {expected_detail}.'
    ]
    assert close_calls == [True]


def test_download_zip_to_temp_prefers_json_message_over_error_and_text(
        monkeypatch):
    close_calls = []
    response = SimpleNamespace(
        status_code=404,
        json=lambda: {'message': 'invalid request', 'error': 'unavailable'},
        text='raw response detail',
        close=lambda: close_calls.append(True),
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', hashlib.md5(b'content').hexdigest(),
        synthesis_messages)

    assert synthesis_messages == [
        'AMF data request for https://amf.amf/Test-A.zip returned error code '
        '404: invalid request.'
    ]
    assert close_calls == [True]


def test_download_zip_to_temp_uses_text_when_json_extraction_fails(monkeypatch):
    def raise_json_error():
        raise ValueError('invalid JSON')

    close_calls = []
    response = SimpleNamespace(
        status_code=404,
        json=raise_json_error,
        text='raw response detail',
        close=lambda: close_calls.append(True),
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', hashlib.md5(b'content').hexdigest(),
        synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF data request for https://amf.amf/Test-A.zip returned error code '
        '404: raw response detail.'
    ]
    assert close_calls == [True]


def test_download_zip_to_temp_uses_status_only_without_http_error_detail(
        monkeypatch):
    close_calls = []
    response = SimpleNamespace(
        status_code=404,
        json=lambda: {},
        close=lambda: close_calls.append(True),
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', hashlib.md5(b'content').hexdigest(),
        synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF data request for https://amf.amf/Test-A.zip returned error code 404.'
    ]
    assert close_calls == [True]


def test_download_zip_to_temp_removes_file_on_checksum_mismatch(monkeypatch):
    content = b'zip-content'
    response = SimpleNamespace(
        status_code=200,
        iter_content=lambda chunk_size: [content],
        close=lambda: None,
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', hashlib.md5(b'other').hexdigest(),
        synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF ZIP download for https://amf.amf/Test-A.zip failed checksum verification.'
    ]


def test_download_zip_to_temp_removes_partial_file_on_stream_failure(monkeypatch):
    response = SimpleNamespace(
        status_code=200,
        iter_content=lambda chunk_size: (_ for _ in ()).throw(
            RuntimeError('stream failed')),
        close=lambda: None,
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', hashlib.md5(b'content').hexdigest(),
        synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF ZIP download for https://amf.amf/Test-A.zip failed: stream failed'
    ]


def test_download_zip_to_temp_records_temporary_file_error(monkeypatch):
    exception = OSError('cannot create temporary file')
    monkeypatch.setattr(
        ameriflux.tempfile, 'NamedTemporaryFile',
        lambda **kwargs: (_ for _ in ()).throw(exception))
    close_calls = []
    response = SimpleNamespace(
        status_code=200,
        iter_content=lambda chunk_size: [b'content'],
        close=lambda: close_calls.append(True),
    )
    monkeypatch.setattr(ameriflux, 'get_url', lambda *args, **kwargs: response)
    synthesis_messages = []

    result = ameriflux._download_zip_to_temp(
        'https://amf.amf/Test-A.zip', hashlib.md5(b'content').hexdigest(),
        synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF ZIP download for https://amf.amf/Test-A.zip failed: '
        'cannot create temporary file'
    ]
    assert close_calls == [True]


# ================================
# TESTS for _get_zip_members


def amf_zip_archive(member_names):
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, 'w') as archive:
        for member_name in member_names:
            archive.writestr(member_name, '')
    archive_buffer.seek(0)
    return archive_buffer, zipfile.ZipFile(archive_buffer)


AMF_DATA_PREFIX = 'AMF_US-Test_FLUXNET_FLUXMET_DD_'
AMF_LOOKUP_PREFIX = 'AMF_US-Test_FLUXNET_BIFVARINFO_DD_'
AMF_BIF_PREFIX = 'AMF_US-Test_FLUXNET_BIF_'


def test_get_zip_members_returns_requested_members():
    member_names = [
        f'{AMF_DATA_PREFIX}2022-2023_v1.3_r1.csv',
        f'{AMF_LOOKUP_PREFIX}2022-2023_v1.3_r1.csv',
        f'{AMF_BIF_PREFIX}2022-2023_v1.3_r1.csv',
        'unrelated.txt',
    ]
    archive_buffer, archive = amf_zip_archive(member_names)
    synthesis_messages = []

    try:
        result = ameriflux._get_zip_members(
            archive, AMF_DATA_PREFIX, AMF_LOOKUP_PREFIX, AMF_BIF_PREFIX,
            synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert [member.filename for member in result] == member_names[:3]
    assert synthesis_messages == []


@pytest.mark.parametrize(
    'missing_prefix',
    [pytest.param(AMF_DATA_PREFIX, id='data-member'),
     pytest.param(AMF_LOOKUP_PREFIX, id='lookup-member'),
     pytest.param(AMF_BIF_PREFIX, id='bif-member')],
)
def test_get_zip_members_reports_missing_member(missing_prefix):
    member_names = [
        f'{AMF_DATA_PREFIX}2022.csv',
        f'{AMF_LOOKUP_PREFIX}2022.csv',
        f'{AMF_BIF_PREFIX}2022.csv',
    ]
    member_names.remove(f'{missing_prefix}2022.csv')
    archive_buffer, archive = amf_zip_archive(member_names)
    synthesis_messages = []

    try:
        result = ameriflux._get_zip_members(
            archive, AMF_DATA_PREFIX, AMF_LOOKUP_PREFIX, AMF_BIF_PREFIX,
            synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result is None
    assert synthesis_messages == [
        f'AMF ZIP archive expected one member beginning with {missing_prefix}, found 0.'
    ]


@pytest.mark.parametrize(
    'duplicate_prefix',
    [pytest.param(AMF_DATA_PREFIX, id='data-member'),
     pytest.param(AMF_LOOKUP_PREFIX, id='lookup-member'),
     pytest.param(AMF_BIF_PREFIX, id='bif-member')],
)
def test_get_zip_members_reports_duplicate_member(duplicate_prefix):
    # This should never happen
    member_names = [
        f'{AMF_DATA_PREFIX}2022.csv',
        f'{AMF_LOOKUP_PREFIX}2022.csv',
        f'{AMF_BIF_PREFIX}2022.csv',
        f'{duplicate_prefix}2023.csv',
    ]
    archive_buffer, archive = amf_zip_archive(member_names)
    synthesis_messages = []

    try:
        result = ameriflux._get_zip_members(
            archive, AMF_DATA_PREFIX, AMF_LOOKUP_PREFIX, AMF_BIF_PREFIX,
            synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result is None
    assert synthesis_messages == [
        f'AMF ZIP archive expected one member beginning with {duplicate_prefix}, found 2.'
    ]


def test_get_zip_members_reports_all_invalid_prefixes():
    # This should never happen
    member_names = [
        f'{AMF_LOOKUP_PREFIX}2022.csv',
        f'{AMF_LOOKUP_PREFIX}2023.csv',
    ]
    archive_buffer, archive = amf_zip_archive(member_names)
    synthesis_messages = []

    try:
        result = ameriflux._get_zip_members(
            archive, AMF_DATA_PREFIX, AMF_LOOKUP_PREFIX, AMF_BIF_PREFIX,
            synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result is None
    assert synthesis_messages == [
        f'AMF ZIP archive expected one member beginning with {AMF_DATA_PREFIX}, found 0.',
        f'AMF ZIP archive expected one member beginning with {AMF_LOOKUP_PREFIX}, found 2.',
        f'AMF ZIP archive expected one member beginning with {AMF_BIF_PREFIX}, found 0.',
    ]


def test_get_zip_members_reports_empty_archive():
    # This should never happen
    archive_buffer, archive = amf_zip_archive([])
    synthesis_messages = []

    try:
        result = ameriflux._get_zip_members(
            archive, AMF_DATA_PREFIX, AMF_LOOKUP_PREFIX, AMF_BIF_PREFIX,
            synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result is None
    assert synthesis_messages == [
        f'AMF ZIP archive expected one member beginning with {AMF_DATA_PREFIX}, found 0.',
        f'AMF ZIP archive expected one member beginning with {AMF_LOOKUP_PREFIX}, found 0.',
        f'AMF ZIP archive expected one member beginning with {AMF_BIF_PREFIX}, found 0.',
    ]


# ================================
# TESTS for _read_lookup_csv


def amf_lookup_member():
    return SimpleNamespace(filename='AMF_US-Test_FLUXNET_BIFVARINFO_DD.csv')


def test_read_lookup_csv_builds_one_variable_group(monkeypatch):
    rows = [
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'TA_F'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_UNIT', 'DATAVALUE': 'deg C'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_HEIGHT', 'DATAVALUE': '10'},
    ]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result == {
        'TA_F': [{
            'SITE_ID': 'US-Test',
            'GROUP_ID': '1',
            'VARIABLE_GROUP': 'GRP_VAR_INFO',
            'VAR_INFO_VARNAME': 'TA_F',
            'VAR_INFO_UNIT': 'deg C',
            'VAR_INFO_HEIGHT': '10',
        }]
    }
    assert synthesis_messages == []


def test_read_lookup_csv_builds_multiple_variable_groups(monkeypatch):
    rows = [
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'TA_F'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_UNIT', 'DATAVALUE': 'deg C'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '2', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'SWC_F_MDS_1'},
    ]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result == {
        'TA_F': [{
            'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
            'VAR_INFO_VARNAME': 'TA_F', 'VAR_INFO_UNIT': 'deg C',
        }],
        'SWC_F_MDS_1': [{
            'SITE_ID': 'US-Test', 'GROUP_ID': '2', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
            'VAR_INFO_VARNAME': 'SWC_F_MDS_1',
        }],
    }
    assert synthesis_messages == []


def test_read_lookup_csv_preserves_repeated_variable_name_groups(monkeypatch):
    rows = [
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'TS_F_MDS_1'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_HEIGHT', 'DATAVALUE': '2'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '2', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'TS_F_MDS_1'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '2', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_HEIGHT', 'DATAVALUE': '10'},
    ]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert [group['GROUP_ID'] for group in result['TS_F_MDS_1']] == ['1', '2']
    assert [group['VAR_INFO_HEIGHT'] for group in result['TS_F_MDS_1']] == ['2', '10']
    assert synthesis_messages == []


def test_read_lookup_csv_ends_group_at_non_variable_row(monkeypatch):
    # This should never happen
    rows = [
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'TA_F'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_SITE',
         'VARIABLE': 'SITE_NAME', 'DATAVALUE': 'Test site'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '2', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'SWC_F_MDS_1'},
    ]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert set(result) == {'TA_F', 'SWC_F_MDS_1'}
    assert 'SITE_NAME' not in result['TA_F'][0]
    assert synthesis_messages == []


def test_read_lookup_csv_skips_group_without_variable_name(monkeypatch):
    # This should never happen
    rows = [
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_UNIT', 'DATAVALUE': 'deg C'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '2', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'TA_F'},
    ]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    logger_warning = Mock()
    monkeypatch.setattr(ameriflux.logger, 'warning', logger_warning)
    synthesis_messages = []

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert list(result) == ['TA_F']
    logger_warning.assert_called_once_with(
        'AMF BIFVARINFO group did not contain VAR_INFO_VARNAME. Skipping group.'
    )
    assert synthesis_messages == []


def test_read_lookup_csv_returns_empty_for_empty_rows(monkeypatch):
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=[]))
    synthesis_messages = []

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result == {}
    assert synthesis_messages == []


def test_read_lookup_csv_preserves_csv_failure(monkeypatch):
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=None))
    synthesis_messages = ['CSV processing failed']

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result == {}
    assert synthesis_messages == ['CSV processing failed']


def test_read_lookup_csv_preserves_empty_metadata_values(monkeypatch):
    rows = [{
        'SITE_ID': 'US-Test',
        'GROUP_ID': '1',
        'VARIABLE_GROUP': 'GRP_VAR_INFO',
        'VARIABLE': 'VAR_INFO_VARNAME',
        'DATAVALUE': 'TA_F',
    }, {
        'SITE_ID': 'US-Test',
        'GROUP_ID': '1',
        'VARIABLE_GROUP': 'GRP_VAR_INFO',
        'VARIABLE': 'VAR_INFO_UNIT',
        'DATAVALUE': '',
    }]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_lookup_csv(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result['TA_F'][0]['VAR_INFO_UNIT'] == ''
    assert synthesis_messages == []


# ================================
# TESTS for _read_bif_csv_rows


def amf_bif_csv_member(csv_bytes, filename='AMF_US-Test_BIF.csv'):
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, 'w') as archive:
        archive.writestr(filename, csv_bytes)
    archive_buffer.seek(0)
    archive = zipfile.ZipFile(archive_buffer)
    return archive_buffer, archive, archive.getinfo(filename)


AMF_BIF_HEADER = 'SITE_ID,GROUP_ID,VARIABLE_GROUP,VARIABLE,DATAVALUE'


def test_read_bif_csv_rows_returns_utf8_rows():
    csv_bytes = (f'{AMF_BIF_HEADER}\n'
                 'US-Test,1,GRP_VAR_INFO,VAR_INFO_VARNAME,TA_F\n'
                 'US-Test,1,GRP_VAR_INFO,VAR_INFO_UNIT,deg C\n').encode('utf-8')
    archive_buffer, archive, member = amf_bif_csv_member(csv_bytes)
    synthesis_messages = []

    try:
        result = ameriflux._read_bif_csv_rows(
            archive, member, 'BIFVARINFO', synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result == [
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_VARNAME', 'DATAVALUE': 'TA_F'},
        {'SITE_ID': 'US-Test', 'GROUP_ID': '1', 'VARIABLE_GROUP': 'GRP_VAR_INFO',
         'VARIABLE': 'VAR_INFO_UNIT', 'DATAVALUE': 'deg C'},
    ]
    assert synthesis_messages == []


def test_read_bif_csv_rows_uses_cp1252_fallback(monkeypatch):
    csv_bytes = (f'{AMF_BIF_HEADER}\n'
                 'US-Test,1,GRP_VAR_INFO,VAR_INFO_UNIT,\xb5mol mol-1\n').encode('cp1252')
    archive_buffer, archive, member = amf_bif_csv_member(csv_bytes)
    synthesis_messages = []
    logger_info = Mock()
    monkeypatch.setattr(ameriflux.logger, 'info', logger_info)

    try:
        result = ameriflux._read_bif_csv_rows(
            archive, member, 'BIFVARINFO', synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result[0]['DATAVALUE'] == 'µmol mol-1'
    assert synthesis_messages == []
    logger_info.assert_any_call(
        f'Using Windows-1252 encoding for AMF BIFVARINFO file {member.filename}.')


@pytest.mark.parametrize(
    'missing_field',
    [pytest.param(field, id=f'missing-{field.lower()}')
     for field in sorted(ameriflux.AMF_BIF_REQUIRED_FIELDS)],
)
def test_read_bif_csv_rows_reports_missing_required_field(missing_field):
    # This should never happen
    header = ','.join(field for field in ameriflux.AMF_BIF_REQUIRED_FIELDS if field != missing_field)
    csv_bytes = f'{header}\nUS-Test,1,GRP_VAR_INFO,TA_F,25\n'.encode('utf-8')
    archive_buffer, archive, member = amf_bif_csv_member(csv_bytes)
    synthesis_messages = []

    try:
        result = ameriflux._read_bif_csv_rows(
            archive, member, 'BIFVARINFO', synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result is None
    assert synthesis_messages == [
        f"AMF BIFVARINFO {member.filename} CSV is missing required columns: ['{missing_field}']."
    ]


def test_read_bif_csv_rows_reports_all_missing_fields():
    # This should never happen
    csv_bytes = b'UNRELATED\nvalue\n'
    archive_buffer, archive, member = amf_bif_csv_member(csv_bytes)
    synthesis_messages = []

    try:
        result = ameriflux._read_bif_csv_rows(
            archive, member, 'BIF', synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    missing_fields = sorted(ameriflux.AMF_BIF_REQUIRED_FIELDS)
    assert result is None
    assert synthesis_messages == [
        f'AMF BIF {member.filename} CSV is missing required columns: {missing_fields}.'
    ]


def test_read_bif_csv_rows_reports_empty_csv():
    # This should never happen
    archive_buffer, archive, member = amf_bif_csv_member(b'')
    synthesis_messages = []

    try:
        result = ameriflux._read_bif_csv_rows(
            archive, member, 'BIFVARINFO', synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    missing_fields = sorted(ameriflux.AMF_BIF_REQUIRED_FIELDS)
    assert result is None
    assert synthesis_messages == [
        f'AMF BIFVARINFO {member.filename} CSV is missing required columns: {missing_fields}.'
    ]


def test_read_bif_csv_rows_preserves_extra_columns():
    # This should never happen
    csv_bytes = (f'{AMF_BIF_HEADER},EXTRA_FIELD\n'
                 'US-Test,1,GRP_VAR_INFO,TA_F,25,extra\n').encode('utf-8')
    archive_buffer, archive, member = amf_bif_csv_member(csv_bytes)
    synthesis_messages = []

    try:
        result = ameriflux._read_bif_csv_rows(
            archive, member, 'BIF', synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result[0]['EXTRA_FIELD'] == 'extra'
    assert synthesis_messages == []


def test_read_bif_csv_rows_reports_archive_read_exception():
    archive = Mock()
    member = SimpleNamespace(filename='AMF_US-Test_BIF.csv')
    archive.open.side_effect = RuntimeError('read failed')
    synthesis_messages = []

    result = ameriflux._read_bif_csv_rows(
        archive, member, 'BIF', synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF BIF AMF_US-Test_BIF.csv CSV processing failed: read failed'
    ]


# ================================
# TESTS for _read_bif_utc_offset


def test_read_bif_utc_offset_returns_string(monkeypatch):
    rows = [
        {'VARIABLE': 'SITE_NAME', 'DATAVALUE': 'Test site'},
        {'VARIABLE': 'UTC_OFFSET', 'DATAVALUE': '-7.0'},
    ]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_bif_utc_offset(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result == '-7.0'
    assert isinstance(result, str)
    assert synthesis_messages == []


def test_read_bif_utc_offset_strips_surrounding_whitespace(monkeypatch):
    # This should not happen
    rows = [{'VARIABLE': 'UTC_OFFSET', 'DATAVALUE': '  -7.0  '}]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_bif_utc_offset(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result == '-7.0'
    assert synthesis_messages == []


def test_read_bif_utc_offset_returns_first_value(monkeypatch):
    # This should not happen
    rows = [
        {'VARIABLE': 'UTC_OFFSET', 'DATAVALUE': '-7.0'},
        {'VARIABLE': 'UTC_OFFSET', 'DATAVALUE': '-6.0'},
    ]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_bif_utc_offset(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result == '-7.0'
    assert synthesis_messages == []


@pytest.mark.parametrize(
    'rows',
    [pytest.param([], id='no-rows'),
     pytest.param([{'VARIABLE': 'SITE_NAME', 'DATAVALUE': 'Test site'}], id='no-utc-variable'),
     pytest.param([{'VARIABLE': 'UTC_OFFSET', 'DATAVALUE': ''}], id='empty-offset')],
)
def test_read_bif_utc_offset_reports_missing_value(monkeypatch, rows):
    # These should not happen
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []
    member = amf_lookup_member()

    result = ameriflux._read_bif_utc_offset(
        Mock(), member, synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        f'AMF BIF {member.filename} did not contain a UTC_OFFSET value.'
    ]


def test_read_bif_utc_offset_ignores_rows_without_expected_keys(monkeypatch):
    rows = [{}, {'VARIABLE': 'SITE_NAME'}, {'DATAVALUE': '-7.0'}]
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=rows))
    synthesis_messages = []

    result = ameriflux._read_bif_utc_offset(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result is None
    assert synthesis_messages == [
        'AMF BIF AMF_US-Test_FLUXNET_BIFVARINFO_DD.csv did not contain a UTC_OFFSET value.'
    ]


def test_read_bif_utc_offset_preserves_csv_failure(monkeypatch):
    monkeypatch.setattr(ameriflux, '_read_bif_csv_rows', Mock(return_value=None))
    synthesis_messages = ['CSV processing failed']

    result = ameriflux._read_bif_utc_offset(
        Mock(), amf_lookup_member(), synthesis_messages)

    assert result is None
    assert synthesis_messages == ['CSV processing failed']


# ================================
# TESTS for _dataframe_chunk_to_xarray


@pytest.mark.parametrize(
    'timestamp_value, expected_timestamp',
    [pytest.param('202401151230', pd.Timestamp('2024-01-15 12:30:00'), id='hourly'),
     pytest.param('20240115', pd.Timestamp('2024-01-15 00:00:00'), id='daily'),
     pytest.param(pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-01 00:00:00'), id='monthly'),
     pytest.param(pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-01 00:00:00'), id='yearly')],
)
def test_dataframe_chunk_to_xarray_preserves_timestamp_resolution(
        timestamp_value, expected_timestamp):
    dataframe = pd.DataFrame({
        'TIMESTAMP': [timestamp_value],
        'TA_F': [25.0],
    })

    result = ameriflux._dataframe_chunk_to_xarray(dataframe, 'TIMESTAMP')

    assert result.indexes['time'].tolist() == [expected_timestamp]
    assert result.attrs['timestamp_column'] == 'TIMESTAMP'
    assert result['TA_F'].values.tolist() == [25.0]


def test_dataframe_chunk_to_xarray_preserves_values_and_row_order():
    dataframe = pd.DataFrame({
        'TIMESTAMP': ['202401011200', '202401011230'],
        'TA_F': [25.0, 26.0],
        'SWC_F_MDS_1': [0.2, 0.3],
    })

    result = ameriflux._dataframe_chunk_to_xarray(dataframe, 'TIMESTAMP')

    assert result.indexes['time'].tolist() == [
        pd.Timestamp('2024-01-01 12:00:00'),
        pd.Timestamp('2024-01-01 12:30:00'),
    ]
    assert result['TA_F'].values.tolist() == [25.0, 26.0]
    assert result['SWC_F_MDS_1'].values.tolist() == [0.2, 0.3]


def test_dataframe_chunk_to_xarray_supports_timestamp_start():
    dataframe = pd.DataFrame({
        'TIMESTAMP_START': ['202401151230'],
        'TA_F': [25.0],
    })

    result = ameriflux._dataframe_chunk_to_xarray(dataframe, 'TIMESTAMP_START')

    assert result.indexes['time'].tolist() == [pd.Timestamp('2024-01-15 12:30:00')]
    assert result.attrs['timestamp_column'] == 'TIMESTAMP_START'


def test_dataframe_chunk_to_xarray_handles_empty_dataframe():
    dataframe = pd.DataFrame({
        'TIMESTAMP': pd.Series(dtype='object'),
        'TA_F': pd.Series(dtype='float64'),
    })

    result = ameriflux._dataframe_chunk_to_xarray(dataframe, 'TIMESTAMP')

    assert len(result.indexes['time']) == 0
    assert result['TA_F'].values.tolist() == []
    assert result.attrs['timestamp_column'] == 'TIMESTAMP'


def test_dataframe_chunk_to_xarray_preserves_missing_values():
    # The data csv file should never have missing values np.nan.
    # This should never happen.
    # Note the -9999 "missing value" gets passed thru to the xarray
    #    ie. it is not considered a missing value from xarray's / numpy's perspective.
    dataframe = pd.DataFrame({
        'TIMESTAMP': ['202401011200', '202401011230'],
        'TA_F': [25.0, np.nan],
    })

    result = ameriflux._dataframe_chunk_to_xarray(dataframe, 'TIMESTAMP')

    assert result['TA_F'].values[0] == 25.0
    assert np.isnan(result['TA_F'].values[1])


def test_dataframe_chunk_to_xarray_raises_for_missing_timestamp_column():
    dataframe = pd.DataFrame({'TA_F': [25.0]})

    with pytest.raises(KeyError):
        ameriflux._dataframe_chunk_to_xarray(dataframe, 'TIMESTAMP')


# ================================
# TESTS for _data_csv_to_zarr


def amf_data_csv_member(csv_text, filename='AMF_US-Test_FLUXMET_DD.csv'):
    return amf_bif_csv_member(csv_text.encode('utf-8'), filename)


def test_data_csv_to_zarr_filters_dates_and_preserves_missing_sentinel(tmp_path):
    csv_text = ('TIMESTAMP,TA_F\n'
                '20240101,1\n'
                '20240102,2\n'
                '20240103,-9999\n'
                '20240104,4\n'
                '20240105,5\n')
    archive_buffer, archive, member = amf_data_csv_member(csv_text)
    synthesis_messages = []
    zarr_path = tmp_path / 'data.zarr'

    try:
        result = ameriflux._data_csv_to_zarr(
            archive, member, zarr_path, 'TIMESTAMP', '%Y%m%d',
            '2024-01-02', '2024-01-04', 2, synthesis_messages)

        assert result.indexes['time'].tolist() == [
            pd.Timestamp('2024-01-02'),
            pd.Timestamp('2024-01-03'),
            pd.Timestamp('2024-01-04'),
        ]
        assert result['TA_F'].values.tolist() == [2, -9999, 4]
        assert synthesis_messages == []
        result.close()
    finally:
        archive.close()
        archive_buffer.close()


def test_data_csv_to_zarr_handles_hourly_timestamp_start(tmp_path):
    csv_text = ('TIMESTAMP_START,TA_F\n'
                '202401011200,25.0\n'
                '202401011230,26.0\n')
    archive_buffer, archive, member = amf_data_csv_member(csv_text)
    synthesis_messages = []
    zarr_path = tmp_path / 'data.zarr'

    try:
        result = ameriflux._data_csv_to_zarr(
            archive, member, zarr_path, 'TIMESTAMP_START', '%Y%m%d%H%M',
            '2024-01-01 12:00', '2024-01-01 12:30', 2, synthesis_messages)

        assert result.indexes['time'].tolist() == [
            pd.Timestamp('2024-01-01 12:00'),
            pd.Timestamp('2024-01-01 12:30'),
        ]
        assert result['TA_F'].values.tolist() == [25.0, 26.0]
        assert result.attrs['timestamp_column'] == 'TIMESTAMP_START'
        result.close()
    finally:
        archive.close()
        archive_buffer.close()


def test_data_csv_to_zarr_reports_no_matching_rows(tmp_path):
    csv_text = ('TIMESTAMP,TA_F\n'
                '20240101,1\n'
                '20240102,2\n')
    archive_buffer, archive, member = amf_data_csv_member(csv_text)
    synthesis_messages = []
    zarr_path = tmp_path / 'data.zarr'

    try:
        result = ameriflux._data_csv_to_zarr(
            archive, member, zarr_path, 'TIMESTAMP', '%Y%m%d',
            '2025-01-01', None, 2, synthesis_messages)
    finally:
        archive.close()
        archive_buffer.close()

    assert result is None
    assert synthesis_messages == [
        f'AMF data CSV {member.filename} did not contain rows matching the query dates.'
    ]


# ================================
# TESTS for _process_download_zip


def amf_download_zip_path(tmp_path, valid=True):
    zip_path = tmp_path / 'download.zip'
    if valid:
        with zipfile.ZipFile(zip_path, 'w') as archive:
            archive.writestr('data.csv', '')
    else:
        zip_path.write_bytes(b'not a zip file')
    return zip_path


def test_process_download_zip_returns_empty_when_download_fails(monkeypatch):
    download = Mock(return_value=None)
    get_members = Mock()
    monkeypatch.setattr(ameriflux, '_download_zip_to_temp', download)
    monkeypatch.setattr(ameriflux, '_get_zip_members', get_members)
    synthesis_messages = ['download failed']

    result = ameriflux._process_download_zip(
        'https://amf.amf/Test-A.zip', 'checksum', 'data_', 'lookup_',
        'TIMESTAMP', '%Y%m%d', '2024-01-01', None, Path('/tmp/data.zarr'),
        'bif_', synthesis_messages)

    assert result == ({}, None, None)
    download.assert_called_once_with(
        'https://amf.amf/Test-A.zip', 'checksum', synthesis_messages)
    get_members.assert_not_called()


def test_process_download_zip_returns_processed_outputs_and_cleans_zip(
        tmp_path, monkeypatch):
    zip_path = amf_download_zip_path(tmp_path)
    data_member = SimpleNamespace(filename='data.csv')
    lookup_member = SimpleNamespace(filename='lookup.csv')
    bif_member = SimpleNamespace(filename='bif.csv')
    members = (data_member, lookup_member, bif_member)
    lookup = {'TA_F': [{'VAR_INFO_UNIT': 'deg C'}]}
    dataset = Mock()
    get_members = Mock(return_value=members)
    read_lookup = Mock(return_value=lookup)
    read_utc_offset = Mock(return_value='-7.0')
    data_to_zarr = Mock(return_value=dataset)
    monkeypatch.setattr(ameriflux, '_download_zip_to_temp', Mock(return_value=zip_path))
    monkeypatch.setattr(ameriflux, '_get_zip_members', get_members)
    monkeypatch.setattr(ameriflux, '_read_lookup_csv', read_lookup)
    monkeypatch.setattr(ameriflux, '_read_bif_utc_offset', read_utc_offset)
    monkeypatch.setattr(ameriflux, '_data_csv_to_zarr', data_to_zarr)
    synthesis_messages = []
    zarr_path = tmp_path / 'data.zarr'

    result = ameriflux._process_download_zip(
        'https://amf.amf/Test-A.zip', 'checksum', 'data_', 'lookup_',
        'TIMESTAMP', '%Y%m%d', '2024-01-01', '2024-01-31', zarr_path,
        'bif_', synthesis_messages, chunk_size=123)

    assert result == (lookup, dataset, '-7.0')
    assert get_members.call_count == 1
    read_lookup.assert_called_once_with(get_members.call_args.args[0], lookup_member,
                                        synthesis_messages)
    read_utc_offset.assert_called_once_with(get_members.call_args.args[0], bif_member,
                                            synthesis_messages)
    assert data_to_zarr.call_args.args[1:] == (
        data_member, zarr_path, 'TIMESTAMP', '%Y%m%d',
        '2024-01-01', '2024-01-31', 123, synthesis_messages)
    assert not zip_path.exists()


def test_process_download_zip_returns_empty_when_members_are_missing(
        tmp_path, monkeypatch):
    zip_path = amf_download_zip_path(tmp_path)
    get_members = Mock(return_value=None)
    read_lookup = Mock()
    monkeypatch.setattr(ameriflux, '_download_zip_to_temp', Mock(return_value=zip_path))
    monkeypatch.setattr(ameriflux, '_get_zip_members', get_members)
    monkeypatch.setattr(ameriflux, '_read_lookup_csv', read_lookup)
    synthesis_messages = ['missing member']

    result = ameriflux._process_download_zip(
        'https://amf.amf/Test-A.zip', 'checksum', 'data_', 'lookup_',
        'TIMESTAMP', '%Y%m%d', '2024-01-01', None, tmp_path / 'data.zarr',
        'bif_', synthesis_messages)

    assert result == ({}, None, None)
    read_lookup.assert_not_called()
    assert not zip_path.exists()


def test_process_download_zip_returns_metadata_when_data_processing_fails(
        tmp_path, monkeypatch):
    zip_path = amf_download_zip_path(tmp_path)
    members = tuple(SimpleNamespace(filename=name) for name in ('data.csv', 'lookup.csv', 'bif.csv'))
    lookup = {'TA_F': []}
    monkeypatch.setattr(ameriflux, '_download_zip_to_temp', Mock(return_value=zip_path))
    monkeypatch.setattr(ameriflux, '_get_zip_members', Mock(return_value=members))
    monkeypatch.setattr(ameriflux, '_read_lookup_csv', Mock(return_value=lookup))
    monkeypatch.setattr(ameriflux, '_read_bif_utc_offset', Mock(return_value='-7.0'))
    monkeypatch.setattr(ameriflux, '_data_csv_to_zarr', Mock(return_value=None))
    synthesis_messages = []

    result = ameriflux._process_download_zip(
        'https://amf.amf/Test-A.zip', 'checksum', 'data_', 'lookup_',
        'TIMESTAMP', '%Y%m%d', '2024-01-01', None, tmp_path / 'data.zarr',
        'bif_', synthesis_messages)

    assert result == (lookup, None, '-7.0')
    assert not zip_path.exists()


def test_process_download_zip_handles_invalid_zip(tmp_path, monkeypatch):
    zip_path = amf_download_zip_path(tmp_path, valid=False)
    monkeypatch.setattr(ameriflux, '_download_zip_to_temp', Mock(return_value=zip_path))
    synthesis_messages = []

    result = ameriflux._process_download_zip(
        'https://amf.amf/Test-A.zip', 'checksum', 'data_', 'lookup_',
        'TIMESTAMP', '%Y%m%d', '2024-01-01', None, tmp_path / 'data.zarr',
        'bif_', synthesis_messages)

    assert result == ({}, None, None)
    assert synthesis_messages[0].startswith(
        'AMF download was not a valid ZIP archive:')
    assert not zip_path.exists()


def test_process_download_zip_cleans_zip_when_downstream_helper_raises(
        tmp_path, monkeypatch):
    zip_path = amf_download_zip_path(tmp_path)
    members = tuple(SimpleNamespace(filename=name) for name in ('data.csv', 'lookup.csv', 'bif.csv'))
    monkeypatch.setattr(ameriflux, '_download_zip_to_temp', Mock(return_value=zip_path))
    monkeypatch.setattr(ameriflux, '_get_zip_members', Mock(return_value=members))
    monkeypatch.setattr(ameriflux, '_read_lookup_csv', Mock(side_effect=RuntimeError('lookup failed')))
    synthesis_messages = []

    with pytest.raises(RuntimeError, match='lookup failed'):
        ameriflux._process_download_zip(
            'https://amf.amf/Test-A.zip', 'checksum', 'data_', 'lookup_',
            'TIMESTAMP', '%Y%m%d', '2024-01-01', None, tmp_path / 'data.zarr',
            'bif_', synthesis_messages)

    assert not zip_path.exists()


# ================================
# TESTS for _load_mf_object


def amf_monitoring_feature_access():
    catalog = CatalogSqlAlchemy()
    plugin = ameriflux.AMFDataSourcePlugin(catalog)
    catalog.initialize([plugin])
    return plugin.access_classes[MonitoringFeature]


def test_load_mf_object_builds_basic_monitoring_feature():
    result = ameriflux._load_mf_object(
        amf_monitoring_feature_access(),
        amf_site_metadata(),
        ['TA_F'])

    assert result.id == 'AMF-US-Test'
    assert result.name == 'Test site'
    assert result.feature_type == FeatureTypeEnum.POINT
    assert result.shape == SpatialSamplingShapes.SHAPE_POINT
    assert result.description == 'Test description.'
    assert not result.coordinates.absolute.vertical_extent
    assert result.coordinates.representative is None

    horizontal_position = result.coordinates.absolute.horizontal_position[0]
    assert horizontal_position.latitude == 44.0
    assert horizontal_position.longitude == -121.0
    assert horizontal_position.datum == ameriflux.HorizontalCoordinate.DATUM_WGS84
    assert horizontal_position.units == ameriflux.GeographicCoordinate.UNITS_DEC_DEGREES


def test_load_mf_object_adds_elevation_coordinate():
    result = ameriflux._load_mf_object(
        amf_monitoring_feature_access(),
        amf_site_metadata(elevation=1253.0),
        [])

    elevation = result.coordinates.absolute.vertical_extent[0]
    assert elevation.value == 1253.0
    assert elevation.distance_units == ameriflux.AltitudeCoordinate.DISTANCE_UNITS_METERS
    assert elevation.datum == ameriflux.DepthCoordinate.DATUM_MEAN_SEA_LEVEL


def test_load_mf_object_adds_single_representative_height():
    result = ameriflux._load_mf_object(
        amf_monitoring_feature_access(),
        amf_site_metadata(),
        [],
        [(2.5, None)])

    representative = result.coordinates.representative.vertical_position
    assert representative.value == 2.5
    assert representative.distance_units == ameriflux.DepthCoordinate.DISTANCE_UNITS_METERS
    assert representative.datum == ameriflux.DepthCoordinate.DATUM_LOCAL_SURFACE
    assert result.description == 'Test description.'


def test_load_mf_object_describes_multiple_height_changes():
    result = ameriflux._load_mf_object(
        amf_monitoring_feature_access(),
        amf_site_metadata(),
        [],
        [(2.5, None), (3.0, '2023-01-01')])

    assert result.coordinates.representative is None
    assert '2.50m beginning at start' in result.description
    assert '3.00m beginning at 2023-01-01' in result.description
    assert 'height/depth changes' in result.description


def test_load_mf_object_includes_optional_description_information():
    result = ameriflux._load_mf_object(
        amf_monitoring_feature_access(),
        amf_site_metadata(igbp='ENF', url='https://amf.amf/site'),
        [])

    assert 'IGBP Vegetation Type: ENF.' in result.description
    assert 'https://amf.amf/site' in result.description


@pytest.mark.parametrize(
    'observed_properties, expected_vocabularies',
    [
        (['TA_F', 'TS_F_MDS', 'SWC_F_MDS'], ['AT', 'STM', 'SMO']),
        (['unknown_variable'], [NO_MAPPING_TEXT]),
        ([], None),
    ])
def test_load_mf_object_maps_datasource_variables_to_basin3d_vocabularies(
        observed_properties, expected_vocabularies):
    result = ameriflux._load_mf_object(
        amf_monitoring_feature_access(),
        amf_site_metadata(),
        observed_properties)

    if expected_vocabularies is None:
        assert result.observed_properties is None
    else:
        actual_vocabularies = [
            mapping.get_basin3d_vocab()
            for mapping in result.observed_properties]
        assert actual_vocabularies == expected_vocabularies
