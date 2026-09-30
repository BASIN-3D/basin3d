"""

.. currentmodule:: basin3d.plugins.arm

:synopsis: Atmospheric Radiation Measurement (ARM) Plugin Definition

This module maps the Atmospheric Radiation Measurement (ARM) metadata and
data services to BASIN-3D monitoring features and measurement timeseries.

Monitoring features are built from ARM ``met.b1`` data-product metadata and
are identified as ``ARM-{site}-{facility}``. Measurement queries use the ARM
``armlive/query`` and ``armlive/mod`` endpoints, retrieve NetCDF files in
batches, and combine them in a temporary Zarr store before creating BASIN-3D
time-value-pair observations.

The plugin reads ``ARM_USER_NAME`` and ``ARM_USER_TOKEN`` environment variables
for ARM credentials. See https://adc.arm.gov/armlive/ for more information. It is
recommended to configure the environment variable ``BASIN3D_USE_FILE_DB=yes``.
``BASIN3D_LOCAL_TEMP_DIR`` may be used to select the writable parent directory
for per-request temporary Zarr stores; the current working directory is used
when it is not set. NetCDF and Zarr support must be installed using the
``basin3d[arm]`` dependencies.
"""

import os
import requests
import shutil
import tempfile
from html.parser import HTMLParser
from typing import Any

try:
    import xarray as _xr
    xr: Any = _xr
except ImportError:
    xr = None

from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Set, Tuple, TypedDict

from basin3d.core.access import get_url
from basin3d.core import monitor
from basin3d.core.models import (AbsoluteCoordinate, Coordinate, GeographicCoordinate, HorizontalCoordinate,
                                 MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature,
                                 TimeMetadataMixin, TimeValuePair, ResultListTVP)
from basin3d.core.plugin import (DataSourcePluginAccess, DataSourcePluginPoint, PluginIteratorResult,
                                 basin3d_plugin, separate_list_types)
from basin3d.core.schema.enum import FeatureTypeEnum, SpatialSamplingShapes
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature

logger = monitor.get_logger(__name__)


class _HTMLTextParser(HTMLParser):
    """Collect text content while ignoring HTML tags."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text = []

    def handle_data(self, data):
        self.text.append(data)


def _remove_html_tags(value: str) -> str:
    """Return the text content of an HTML-formatted string."""
    parser = _HTMLTextParser()
    parser.feed(value)
    parser.close()
    return ''.join(parser.text)


def _require_arm_dependencies():
    """Raise an actionable error when the optional ARM dependencies are absent."""
    if xr is None:
        raise ImportError('ARM plugin dependencies are not installed. Install them with: pip install "basin3d[arm]"')


NOT_PROVIDED = 'not provided'

DEFAULT_TEMP_DIR = os.getcwd()

ARM_NAME = os.environ.get('ARM_USER_NAME', None)
ARM_TOKEN = os.environ.get('ARM_USER_TOKEN', None)
LOCAL_TEMP_DIR = os.environ.get('BASIN3D_LOCAL_TEMP_DIR', DEFAULT_TEMP_DIR)


class UnitLookupInfo(TypedDict):
    """Describe one ARM-to-BASIN-3D unit conversion entry."""

    arm_unit: str
    target_unit: str
    conv: int | float


# Keys are written as ``{ARM unit}-{BASIN-3D unit}``; ``conv`` is the
# multiplicative factor applied to ARM values before they are returned.
UNIT_LOOKUP: Dict[str, UnitLookupInfo] = {
    'kPa-mm Hg': {'arm_unit': 'kPa', 'target_unit': 'mm Hg', 'conv': 0.4},
    'cm-m': {'arm_unit': 'cm', 'target_unit': 'm', 'conv': 100},
    'degC-C': {'arm_unit': 'degC', 'target_unit': 'C', 'conv': 1},
    'degree-degrees': {'arm_unit': 'degree', 'target_unit': 'degrees', 'conv': 1},
    'deg-degrees': {'arm_unit': 'degree', 'target_unit': 'degrees', 'conv': 1},
    '%-percent': {'arm_unit': '%', 'target_unit': 'percent', 'conv': 1},
}


def _fetch_timeseries(url: str, file_batch: List, variables: List, synthesis_messages: List):
    """Download one ARM NetCDF batch and return the requested variables.

    The response is streamed to a temporary NetCDF file, opened with xarray,
    and loaded into memory before the temporary file is removed. Variables
    that are absent from the response are reported; if none of the requested
    variables are present, the result is ``None``. The HTTP response is closed
    on every path.

    :param url: ARM ``armlive/mod`` URL.
    :param file_batch: ARM filenames to include in the JSON request body.
    :param variables: NetCDF variables to retain, normally including ``time``.
    :param synthesis_messages: Mutable list receiving request and data-format
        diagnostics.
    :return: Loaded xarray dataset containing available requested variables,
        or ``None`` when retrieval or processing fails.
    :raises ImportError: If the optional ARM dependencies are unavailable.
    """
    _require_arm_dependencies()
    response = None

    try:
        response = requests.get(url, json=file_batch, stream=True)
    except requests.RequestException as exc:
        message = f'ARM data request failed for {url}: {exc}'
        logger.warning(message)
        synthesis_messages.append(message)
        return None

    try:
        if response is None:
            message = f'ARM data request returned no response for {url}'
            logger.warning(message)
            synthesis_messages.append(message)
            return None

        if response.status_code != 200:
            detail = response.text.strip()
            if len(detail) > 500:
                detail = f'{detail[:500]}...'

            message = f'ARM data request returned HTTP {response.status_code} for {url}'
            if detail:
                message += f': {detail}'

            logger.warning(message)
            synthesis_messages.append(message)
            return None

        with tempfile.NamedTemporaryFile(suffix='.nc') as tmp:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)

            tmp.flush()

            with xr.open_dataset(tmp.name) as ds:
                available_variables = [variable for variable in variables if variable in ds.variables]
                missing_variables = [variable for variable in variables if variable not in ds.variables]

                if missing_variables:
                    message = (f'ARM data response for {url} did not contain requested variables: '
                               f'{", ".join(missing_variables)}')
                    logger.warning(message)
                    synthesis_messages.append(message)

                if not available_variables:
                    return None

                # Load before the temporary file is deleted.
                return ds[available_variables].load()
    except (requests.RequestException, RuntimeError, OSError, ValueError, TypeError, KeyError) as exc:
        message = f'ARM data request processing failed for {url}: {exc}'
        logger.error(message)
        synthesis_messages.append(message)
        return None
    finally:
        if response is not None:
            response.close()


def _validate_zarr_path(zarr_path: Path, synthesis_messages: List) -> bool:
    """Check that a temporary Zarr parent exists, is a directory, and is writable.

    Validation failures are logged and appended to ``synthesis_messages``.

    :param zarr_path: Configured parent directory for temporary ARM stores.
    :param synthesis_messages: Mutable list receiving validation diagnostics.
    :return: ``True`` when the path can be used, otherwise ``False``.
    """

    if not zarr_path.exists():
        message = (f'ARM zarr path does not exist: {zarr_path}. The local directory for temporary files should be '
                   'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
        logger.error(message)
        synthesis_messages.append(message)
        return False

    if not zarr_path.is_dir():
        message = (f'ARM zarr path is not a directory: {zarr_path}. The local directory for temporary files should be '
                   'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
        logger.error(message)
        synthesis_messages.append(message)
        return False

    if not os.access(zarr_path, os.W_OK):
        message = (f'ARM zarr path is not writable: {zarr_path}. The local directory for temporary files can be '
                   'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR, otherwise the default working directory '
                   'is used. Please double check your configuration.')
        logger.error(message)
        synthesis_messages.append(message)
        return False

    return True


def _create_zarr_temp_dir(zarr_path_str: str, synthesis_messages: List):
    """Create a per-request temporary Zarr directory.

    The configured parent directory is validated before a child directory is
    created with the ``basin3d-arm-`` prefix. Creation failures are recorded
    in ``synthesis_messages`` and return ``None``.

    :param zarr_path_str: Parent directory path, usually from
        ``BASIN3D_LOCAL_TEMP_DIR``.
    :param synthesis_messages: Mutable list receiving validation and creation
        diagnostics.
    :return: The newly created child path, or ``None`` on failure.
    """
    zarr_path = Path(zarr_path_str)
    if not _validate_zarr_path(zarr_path, synthesis_messages):
        return None

    try:
        return Path(tempfile.mkdtemp(prefix='basin3d-arm-', dir=zarr_path))
    except OSError as exc:
        message = f'Failed to create ARM zarr temporary directory in {zarr_path}: {exc}'
        logger.error(message)
        synthesis_messages.append(message)
        return None


def _collect_to_zarr(url: str, file_list: List, meas_variables: List, synthesis_messages: List,
                     zarr_path: Path):
    """Fetch ARM NetCDF batches and combine them in a temporary Zarr store.

    Each batch must expose the same data variables and non-time dimensions.
    Time lengths may differ, and valid batches are appended along ``time``.
    ARM ``missing_value`` encodings are copied to ``_FillValue`` before the
    data are written. A successfully opened xarray Zarr dataset is returned;
    failures, inconsistent batch structures, and an entirely empty retrieval
    are reported through ``synthesis_messages`` and return ``None``.

    :param url: ARM ``armlive/mod`` URL used for every batch.
    :param file_list: Batches of ARM filenames.
    :param meas_variables: Measurement variables to retrieve in addition to
        ``time``.
    :param synthesis_messages: Mutable list receiving retrieval and storage
        diagnostics.
    :param zarr_path: Validated directory in which to create the Zarr store.
    :return: An opened xarray Zarr dataset, or ``None`` if no usable dataset
        can be assembled.
    :raises ImportError: If the optional ARM dependencies are unavailable.
    """
    _require_arm_dependencies()

    # Validate the store path here so direct callers still receive synthesis messages for configuration errors.
    if not _validate_zarr_path(zarr_path, synthesis_messages):
        return None

    first_file = True
    variables = ['time'] + meas_variables
    expected_data_variables: set[str] = set()
    expected_non_time_dimensions = None

    for file_batch in file_list:
        part = _fetch_timeseries(url, file_batch, variables, synthesis_messages)

        if part is None:
            continue

        try:
            data_variables = set(part.data_vars)
            non_time_dimensions = {dimension: size for dimension, size in part.sizes.items()
                                   if dimension != 'time'}

            if first_file:
                expected_data_variables = data_variables
                expected_non_time_dimensions = non_time_dimensions
            elif (data_variables != expected_data_variables or
                  non_time_dimensions != expected_non_time_dimensions):
                variable_mismatch = data_variables != expected_data_variables
                dimension_mismatch = non_time_dimensions != expected_non_time_dimensions

                if variable_mismatch:
                    message = (f'ARM timeseries batch variable mismatch for {url}: expected variables '
                               f'{sorted(expected_data_variables)}, but received variables '
                               f'{sorted(data_variables)}.')
                    if dimension_mismatch:
                        message += (f' Non-time dimensions also differ: expected '
                                    f'{expected_non_time_dimensions}, but received {non_time_dimensions}.')
                else:
                    message = (f'ARM timeseries batch non-time dimension mismatch for {url}: expected '
                               f'{expected_non_time_dimensions}, but received {non_time_dimensions}.')

                logger.error(message)
                synthesis_messages.append(message)
                return None

            for variable in part.data_vars:
                missing_value = part[variable].encoding.get('missing_value')
                if missing_value is not None:
                    part[variable].encoding['_FillValue'] = missing_value

            try:
                if first_file:
                    # Creates the Zarr store.
                    part.to_zarr(zarr_path, mode='w', consolidated=False)
                    first_file = False
                else:
                    # Appends this dataset along the existing time dimension.
                    part.to_zarr(zarr_path, mode='a', append_dim='time', consolidated=False)
            except (OSError, ValueError, TypeError, KeyError) as exc:
                message = f'Failed to write ARM timeseries data to zarr path {zarr_path}: {exc}'
                logger.error(message)
                synthesis_messages.append(message)
                return None
        finally:
            part.close()

    if first_file:
        message = f'No ARM timeseries datasets were successfully retrieved for {url}.'
        logger.warning(message)
        synthesis_messages.append(message)
        return None

    try:
        return xr.open_zarr(zarr_path, consolidated=False)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        message = f'Failed to open ARM timeseries zarr path {zarr_path}: {exc}'
        logger.error(message)
        synthesis_messages.append(message)
        return None


def _batch_files(file_list: list, batch_size: int = 30) -> List:
    """Split sorted ARM filenames into batches of at most ``batch_size``.

    :param file_list: ARM data filenames, normally sorted by filename before
        batching.
    :param batch_size: Maximum number of files in each batch. Must be positive.
    :return: A list of filename batches. An empty input produces an empty list.
    :raises ValueError: If ``batch_size`` is not greater than zero.
    """
    batch_size = int(batch_size)
    if batch_size <= 0:
        raise ValueError('batch_size must be greater than zero')

    return [file_list[start:start + batch_size] for start in range(0, len(file_list), batch_size)]


def _get_mf_files(data_url: str, synthesis_messages: List) -> List:
    """Retrieve and batch ARM files for one monitoring feature.

    The ARM query response must contain a list under ``files``. Files are
    sorted before being divided into batches for the NetCDF endpoint. Invalid,
    empty, or missing responses are reported through ``synthesis_messages``
    and return an empty list.

    :param data_url: ARM ``armlive/query`` URL for one data product and date
        range.
    :param synthesis_messages: Mutable list receiving user-facing warnings or
        errors encountered while retrieving the files.
    :return: Batches of ARM filenames, or an empty list when no usable files
        are available.
    """

    results: List = []

    data_files = _get_arm_data_files(data_url, None, synthesis_messages)

    if not data_files:
        return results

    files: list = data_files.get('files', [])

    if not files:
        message = f'No ARM timeseries datasets were successfully retrieved for {data_url}.'
        logger.warning(message)
        synthesis_messages.append(message)
        return results

    if not isinstance(files, list):
        message = (f'ARM timeseries files for {data_url} were not in expected list format. '
                   f'It was {files.__class__.__name__}. Cannot batch.')
        logger.warning(message)
        synthesis_messages.append(message)
        return results

    files.sort()

    results = _batch_files(files)

    return results


def _get_arm_request(arm_url: str, bbox: tuple | None, synthesis_messages: List, empty_result: List | Dict):
    """Perform an ARM metadata or data-file request and decode its JSON body.

    BASIN-3D bounding boxes use ``(west, south, east, north)`` ordering. The
    ARM endpoint expects ``(west, north, east, south)``, so a supplied box is
    reordered when the request URL is constructed. HTTP failures, malformed
    error responses, and request exceptions are logged and recorded in
    ``synthesis_messages``; in those cases ``empty_result`` is returned.

    :param arm_url: ARM endpoint URL without the optional bounding-box query.
    :param bbox: Optional BASIN-3D bounding box.
    :param synthesis_messages: Mutable list receiving request diagnostics.
    :param empty_result: Empty list or dictionary matching the expected JSON
        response shape.
    :return: Decoded JSON response, or ``empty_result`` on failure.
    """
    results = empty_result
    request_url = arm_url

    # QueryMonitoringFeature uses west, south, east, north. ARM expects
    # top-left longitude, top-left latitude, bottom-right longitude,
    # bottom-right latitude, which is west, north, east, south.
    if bbox:
        west, south, east, north = bbox
        request_url = f'{arm_url}&bbox={west}%2C{north}%2C{east}%2C{south}'

    # ToDo: add mechanism to detect / try pagination (there is no next functionality in the ARM REST API)
    try:
        response = get_url(request_url)
        if not response or response.status_code != 200:
            status_code = response.status_code if response else 'NO RESPONSE'
            error_detail = None
            if response:
                try:
                    response_error = response.json()
                    if isinstance(response_error, dict):
                        error_detail = response_error.get('message') or response_error.get('error')
                except Exception:
                    pass

                if not error_detail:
                    error_detail = getattr(response, 'text', None)

            detail = f': {error_detail}' if error_detail else ''
            msg = f'ARM request for {request_url} returned error code {status_code}{detail}.'
            logger.error(msg)
            synthesis_messages.append(msg)
            return results

        return response.json()
    except Exception as e:
        msg = f'ARM request for {request_url} failed: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        return results


def _get_arm_metadata(arm_url: str, bbox: tuple | None, synthesis_messages: List) -> List:
    """Retrieve ARM metadata records and require a list-shaped response.

    :param arm_url: ARM metadata endpoint URL.
    :param bbox: Optional BASIN-3D bounding box to append to the request.
    :param synthesis_messages: Mutable list receiving request and validation
        diagnostics.
    :return: ARM metadata records, or an empty list for an invalid response.
    """
    metadata_results: List = []
    response_result = _get_arm_request(arm_url, bbox, synthesis_messages, metadata_results)

    if not isinstance(response_result, list):
        response_class = response_result.__class__.__name__
        msg = f'ARM metadata results for {arm_url} was not in expected list format. It was {response_class} class. Cannot parse.'
        logger.error(msg)
        synthesis_messages.append(msg)
        return metadata_results

    return response_result


def _get_arm_data_files(arm_url: str, bbox: tuple | None, synthesis_messages: List) -> Dict:
    """Retrieve an ARM file-query response and require a dictionary result.

    :param arm_url: ARM ``armlive/query`` endpoint URL.
    :param bbox: Optional BASIN-3D bounding box to append to the request.
    :param synthesis_messages: Mutable list receiving request and validation
        diagnostics.
    :return: ARM file-query dictionary, or an empty dictionary for an invalid
        response.
    """
    data_query_results: Dict = {}
    response_result = _get_arm_request(arm_url, bbox, synthesis_messages, data_query_results)

    if not isinstance(response_result, dict):
        response_class = response_result.__class__.__name__
        msg = f'ARM metadata results for {arm_url} was not in expected dictionary format. It was {response_class}. Cannot parse.'
        logger.error(msg)
        synthesis_messages.append(msg)
        return data_query_results

    return response_result


def _parse_arm_metadata(metadata_results: List, mf_lookup: Dict, synthesis_messages: List):
    """Parse ARM ``met.b1`` metadata into the monitoring-feature lookup.

    Each valid record is keyed by ``{site identifier}-{facility identifier}``
    and stores the feature name, WGS84 coordinates, parent site, and measured
    ARM variables. Duplicate records are ignored, which allows overlapping
    bounding-box queries and named-feature lookups to be combined safely.
    Records missing identifiers or spatial coverage are skipped and described
    in ``synthesis_messages``. Missing ``variableMeasured`` metadata is
    retained as a feature with an empty variable list.

    :param metadata_results: ARM metadata records, one for each data product.
    :param mf_lookup: Mutable destination mapping ARM feature IDs to parsed
        metadata dictionaries.
    :param synthesis_messages: Mutable list receiving validation diagnostics.
    :return: ``None``; ``mf_lookup`` is updated in place.
    """

    for metadata in metadata_results:
        try:
            spatial_coverage = metadata['spatialCoverage']
            contained_in_place = spatial_coverage['containedInPlace']
            site_identifier = contained_in_place['identifier']
            facility_identifier = spatial_coverage['identifier']
            mf_id = f'{site_identifier}-{facility_identifier}'
        except (KeyError, TypeError, ValueError) as e:
            msg = f'ARM metadata record missing required monitoring feature identifiers: {e}'
            logger.warning(msg)
            synthesis_messages.append(msg)
            continue

        if mf_id in mf_lookup:
            continue

        try:
            geo = spatial_coverage['geo']
            parent_name = contained_in_place['name']
            feature_name = spatial_coverage['name']
            latitude = geo['latitude']
            longitude = geo['longitude']
            citation = _remove_html_tags(metadata['citation'])

            required_values = {
                'containedInPlace.name': parent_name,
                'spatialCoverage.name': feature_name,
                'geo.latitude': latitude,
                'geo.longitude': longitude,
                'citation': citation,
            }
            missing_values = [field for field, value in required_values.items() if value is None or value == '']
            if missing_values:
                raise ValueError(f"missing {', '.join(missing_values)}")
        except (KeyError, TypeError, ValueError) as e:
            msg = f'ARM metadata record missing required spatial coverage information: {e}'
            logger.warning(msg)
            synthesis_messages.append(msg)
            continue

        variable_measured = metadata.get('variableMeasured') or []
        if not variable_measured:
            msg = f'ARM metadata record for {mf_id} has no variableMeasured metadata.'
            logger.info(msg)
            synthesis_messages.append(msg)

        var_list = [variable['name'] for variable in variable_measured if isinstance(variable, dict) and 'name' in variable]
        mf_lookup[mf_id] = {
            'id': mf_id,
            'facility_id': facility_identifier,
            'name': f'{parent_name} - {feature_name}',
            'site_id': site_identifier,
            'site_name': parent_name,
            'lat': latitude,
            'long': longitude,
            'citation': citation,
            'variables': var_list,
        }


def _load_mf_object(datasource: DataSourcePluginAccess, mf_id: str,
                    mf_info: Dict) -> MonitoringFeature | None:
    """Build a BASIN-3D point monitoring feature from parsed ARM metadata.

    The returned feature uses the ARM site as its related parent sampling
    feature, reports WGS84 decimal-degree coordinates, and exposes the ARM
    ``variableMeasured`` names as observed properties.

    :param datasource: ARM access object that owns the resulting model.
    :param mf_id: BASIN-3D ARM monitoring-feature identifier.
    :param mf_info: Parsed metadata dictionary produced by
        :func:`_parse_arm_metadata`.
    :return: A BASIN-3D point monitoring feature.
    """

    related_sampling_feature = RelatedSamplingFeature(
        datasource,
        id=mf_info.get('site_id'),
        related_sampling_feature=mf_info.get('site_name'),
        related_sampling_feature_type=FeatureTypeEnum.SITE,  # previously site
        role=RelatedSamplingFeature.ROLE_PARENT)

    monitoring_feature = MonitoringFeature(
        datasource,
        id=mf_id,
        name=mf_info.get('name'),
        feature_type=FeatureTypeEnum.POINT,
        shape=SpatialSamplingShapes.SHAPE_POINT,
        observed_properties=mf_info.get('variables'),
        related_sampling_feature_complex=[related_sampling_feature],
        coordinates=Coordinate(
            absolute=AbsoluteCoordinate(
                horizontal_position=GeographicCoordinate(
                    **{"latitude": mf_info.get('lat'),
                       "longitude": mf_info.get('long'),
                       # from api documentation: "Coordinates are published in EPSG:4326 / WGS84 / World Geodetic System 1984"
                       "datum": HorizontalCoordinate.DATUM_WGS84,
                       "units": GeographicCoordinate.UNITS_DEC_DEGREES}))
        )
    )

    return monitoring_feature


def _get_selected_arm_metadata(arm_metb1_url: str, query_monitoring_feature: List, synthesis_messages: List) -> Tuple[Set, Dict]:
    """Resolve named and bounding-box ARM feature selectors to metadata.

    Bounding-box selectors are queried first and deduplicated. Named selectors
    already found in those results reuse the existing lookup; missing names
    cause one full metadata request, after which unresolved names are reported
    and excluded.

    :param arm_metb1_url: ARM metadata endpoint URL.
    :param query_monitoring_feature: BASIN-3D monitoring-feature selectors,
        containing ARM IDs and/or bounding-box tuples.
    :param synthesis_messages: Mutable list receiving lookup diagnostics.
    :return: A set of selected ARM feature IDs and the complete parsed lookup
        used to construct them.
    """
    mf_lookup: Dict = {}

    # split up mf query types
    mf_query_types = separate_list_types(
        query_monitoring_feature, {'named': str, 'bbox': tuple})
    mf_named = mf_query_types.get('named', [])
    mf_bbox = mf_query_types.get('bbox', [])

    if mf_bbox:
        for bbox in mf_bbox:
            metadata_results = _get_arm_metadata(arm_metb1_url, bbox, synthesis_messages)
            if metadata_results:
                _parse_arm_metadata(metadata_results, mf_lookup, synthesis_messages)

    mf_set = set(mf_lookup.keys())

    if mf_named:
        have_full_lookup = False
        for mf_id in mf_named:
            # If mf_name was not captured in the bbox lookup and the full lookup is not yet acquired
            if mf_id not in mf_lookup and have_full_lookup is False:
                metadata_results = _get_arm_metadata(arm_metb1_url, None, synthesis_messages)
                _parse_arm_metadata(metadata_results, mf_lookup, synthesis_messages)
                # the mf_lookup has all the metadata now
                have_full_lookup = True
            if mf_id not in mf_lookup:
                msg = f'{mf_id} not found in ARM metadata for met.b1 data products'
                logger.warning(msg)
                synthesis_messages.append(msg)
                continue
            mf_set.add(mf_id)

    return mf_set, mf_lookup


def _build_tvp_results(time_values, data_values, missing_value, unit_conv: int | float,
                       quality_values=None, requested_qualities=None) -> Tuple[List, List, int]:
    """Build time-value pairs and aligned quality values for one ARM variable.

    When ``requested_qualities`` is non-empty, values whose corresponding QC
    value is not requested are omitted from both result lists. Conversion and
    missing-value handling are applied to every retained measurement.

    :param time_values: Ordered timestamps aligned with ``data_values``.
    :param data_values: ARM measurement values.
    :param missing_value: ARM fill value that must not be unit converted.
    :param unit_conv: Multiplicative conversion factor to the BASIN-3D unit.
    :param quality_values: Optional QC values aligned with the measurements.
    :param requested_qualities: Optional requested ARM QC values.
    :return: A tuple containing time-value pairs, retained QC values, and the
        number of measurements filtered by quality.
    """
    results_TVPs = []
    result_TVP_quality = []
    filtered_count = 0

    # Requested values are passed in from the translated query.
    #    Thus, the quality values are already in the ARM vocabulary but are strings.
    requested_qualities = {int(quality) for quality in (requested_qualities or [])}
    has_quality_values = quality_values is not None

    for index, (timestamp, value) in enumerate(zip(time_values, data_values)):
        quality = quality_values[index] if has_quality_values else None
        if quality is not None and hasattr(quality, 'item'):
            quality = quality.item()

        if requested_qualities and has_quality_values and quality not in requested_qualities:
            filtered_count += 1
            continue

        if unit_conv == 1 or value == missing_value:
            converted_value = value
        else:
            converted_value = value * unit_conv
        if hasattr(converted_value, 'item'):
            converted_value = converted_value.item()

        results_TVPs.append(TimeValuePair(timestamp=timestamp, value=converted_value))
        if has_quality_values:
            result_TVP_quality.append(quality)

    return results_TVPs, result_TVP_quality, filtered_count


class ARMMonitoringFeatureAccess(DataSourcePluginAccess):
    """Provide BASIN-3D monitoring features that have ARM ``met.b1`` data.

    ARM data products are exposed as point features with IDs in the
    ``ARM-{site}-{facility}`` form. The access supports listing all available
    products, selecting named ARM IDs, selecting WGS84 bounding boxes, and
    combining those selectors with duplicate removal.
    """

    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature):
        """Yield ARM monitoring features matching a BASIN-3D query.

        With no monitoring_feature query specification, all locations with ARM
         ``met.b1`` sites are retrieved. Querying by named facility + site IDs
        as well as bounding boxes are supported.
        Unsupported feature types and malformed or incomplete ARM records are
        reported through the access result's synthesis messages.

        :param query: BASIN-3D monitoring-feature query.
        :yields: :class:`~basin3d.core.models.MonitoringFeature` instances.
        :return: A ``StopIteration`` carrying synthesis messages after all
            matching features have been yielded.
        """

        synthesis_messages: List[str] = []
        synthesis_citations: List[str] = []

        # if parent feature is specified and not in the supported types, return nothing.
        if query.feature_type and query.feature_type not in ARMDataSourcePlugin.feature_types:
            msg = (f'{self.datasource.id_prefix} does not specified feature type: {query.feature_type}. '
                   f'Only feature types {ARMDataSourcePlugin.feature_types} are supported.')
            logger.warning(msg)
            synthesis_messages = [msg]
            return StopIteration(PluginIteratorResult(synthesis_messages, synthesis_citations))

        # the current number of data products is 65 (and has been for the past 1+ years).
        arm_metb1_url = f'{self.datasource.location}/metadata/data_product?data_product=met&page_from=0&page_size=100'

        mf_lookup: Dict = {}

        # if nothing specified, get all
        if not query.monitoring_feature:

            metadata_results = _get_arm_metadata(arm_metb1_url, None, synthesis_messages)
            _parse_arm_metadata(metadata_results, mf_lookup, synthesis_messages)
            mf_set = set(mf_lookup.keys())

        # else create a set, get bbox add to set, if named, see if already in the set, if not get all and step thru to add
        else:

            mf_set, mf_lookup = _get_selected_arm_metadata(arm_metb1_url, query.monitoring_feature, synthesis_messages)

        # yield the set list
        for mf_id in mf_set:
            mf_info = mf_lookup.get(mf_id, {})
            mf_obj = _load_mf_object(self, mf_id, mf_info)
            synthesis_citations.append(mf_info['citation'])
            yield mf_obj

        return StopIteration(PluginIteratorResult(synthesis_messages, synthesis_citations))


class ARMMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """Provide BASIN-3D observations from ARM ``met.b1`` time series.

    The access resolves ARM monitoring features, queries matching data-product
    files for the requested date range, combines NetCDF responses in a
    temporary Zarr store, applies configured unit conversions, and optionally
    filters measurements using ARM QC variables. Temporary stores are removed
    after each monitoring feature has been processed.
    """

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def _get_unit_conv(self, arm_unit: str, arm_variable: str,
                       synthesis_messages: List) -> Tuple[int | float, str]:
        """Determine the conversion from an ARM variable unit to BASIN-3D.

        Known mismatches use :data:`UNIT_LOOKUP`. Exact unit matches return a
        factor of ``1`` without a warning. Unknown mismatches also return the
        native ARM unit and factor ``1``, while recording a warning because the
        values cannot be assessed against the BASIN-3D unit.

        :param arm_unit: Unit reported by the ARM NetCDF variable.
        :param arm_variable: ARM variable name and BASIN-3D mapping key.
        :param synthesis_messages: Mutable list receiving unknown-unit
            warnings.
        :return: A ``(conversion factor, output unit)`` tuple.
        """

        # look up the BASIN3D variable unit
        b3d_mapping = self.get_datasource_attribute_mapping('OBSERVED_PROPERTY', arm_variable)
        b3d_op = b3d_mapping.basin3d_desc[0]
        b3d_unit = b3d_op.units
        unit_info = UNIT_LOOKUP.get(f'{arm_unit}-{b3d_unit}')

        # If all is expected, the units are in the lookup, and the mapping is known and returned
        if unit_info:
            unit_conv = unit_info['conv']
            unit_str = unit_info['target_unit']
            return unit_conv, unit_str

        # In the odd chance that the mapping is not pre-defined and the units are the exact same, all good and don't message.
        if b3d_unit != arm_unit:
            msg = f'Unit for {arm_variable} was unexpected and unit conversion to BASIN-3D unit could not be assessed. Returning values in ARM native unit {arm_unit}.'
            logger.warning(msg)
            synthesis_messages.append(msg)

        return 1, arm_unit

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """Yield ARM measurement timeseries TVP observations.

        The query must include ARM-prefixed monitoring features or bounding box
        coordinates. Requested observed properties are matched against each
        feature's ARM metadata; corresponding ``qc_`` variables are retrieved
        when present. ARM met.b1 are minute resolution and thus only the
        aggregation_duration='MINUTE' is supported.
        Results contain converted time-value pairs, optional QC values,
        the requested aggregation metadata, and ARM point monitoring features.
        Missing data, unavailable QC variables, invalid temporary storage, and retrieval
        errors are reported through synthesis messages.

        :param query: BASIN-3D measurement-timeseries query, including feature
            selectors, observed properties, dates, aggregation duration, and
            optional result-quality filters.
        :yields: :class:`~basin3d.core.models.MeasurementTimeseriesTVPObservation`
            instances for variables with usable ARM data.
        :return: A ``StopIteration`` carrying synthesis messages after all
            selected features have been processed.
        """
        synthesis_messages: list = []
        synthesis_citations: List[str] = []
        if not _validate_zarr_path(Path(LOCAL_TEMP_DIR), synthesis_messages):
            return StopIteration(PluginIteratorResult(synthesis_messages, synthesis_citations))

        if not query.monitoring_feature:
            msg = f'No monitoring features for ARM were specified or they were not specified with the {self.datasource.id_prefix} prefix.'
            logger.warning(msg)
            synthesis_messages.append(msg)
            return StopIteration(PluginIteratorResult(synthesis_messages, synthesis_citations))

        arm_metb1_url = f'{self.datasource.location}/metadata/data_product?data_product=met&page_from=0&page_size=100'

        mf_set, mf_lookup = _get_selected_arm_metadata(arm_metb1_url, query.monitoring_feature, synthesis_messages)

        query_date_str = f'&start={query.start_date}'
        if query.end_date:
            query_date_str += f'&end={query.end_date}'

        arm_data_query_url = (f'https://adc.arm.gov/armlive/query?user={ARM_NAME}:{ARM_TOKEN}'
                              '&ds={}'
                              f'{query_date_str}&wt=json')

        arm_data_url = (f'https://adc.arm.gov/armlive/mod?user={ARM_NAME}:{ARM_TOKEN}'
                        '&variables=time,{}&wt=cdf')

        query_observed_properties = deepcopy(query.observed_property)

        for obs_prop in query.observed_property:
            query_observed_properties.append(f'qc_{obs_prop}')

        for mf_id in mf_set:
            mf_info = mf_lookup.get(mf_id, {})
            mf_vars = mf_info.get('variables', {})
            if not mf_vars:
                msg = f'The metadata for {mf_id} does not specify any variables. Skipping'
                logger.warning(msg)
                synthesis_messages.append(msg)
                continue

            query_vars = [query_variable for query_variable in query_observed_properties if query_variable in mf_vars]

            if not query_vars:
                msg = f'{mf_id} does not have any of the queried observed properties.'
                logger.info(msg)
                synthesis_messages.append(msg)
                continue

            # get the dataset daily files
            mf_site = mf_info.get('site_id', '').lower()
            mf_facility = mf_info.get('facility_id', '')

            data_product_name = f'{mf_site}met{mf_facility}.b1'
            file_batches = _get_mf_files(arm_data_query_url.format(data_product_name),
                                         synthesis_messages)

            # Let the endpoint do the date filtering
            if not file_batches:
                msg = f'{mf_id} does not have any files that match the query parameters. Skipping'
                logger.info(msg)
                synthesis_messages.append(msg)
                continue

            query_var_str = ','.join(query_vars)
            arm_data_var_url = arm_data_url.format(query_var_str)

            zarr_path = None
            try:
                zarr_path = _create_zarr_temp_dir(LOCAL_TEMP_DIR, synthesis_messages)
                if zarr_path is None:
                    continue

                # loop thru the files, extracting the query variables
                data_zarr_io = _collect_to_zarr(arm_data_var_url, file_batches, query_vars, synthesis_messages, zarr_path)

                if not data_zarr_io:
                    continue

                monitoring_feature = _load_mf_object(self, mf_id, mf_info)

                with data_zarr_io as ds:

                    timestamps = [str(timestamp) for timestamp in ds['time'].values]

                    # For each var in the query_var list, ...
                    for var in query_vars:
                        if var not in ds.variables or var.startswith('qc_'):
                            continue

                        result_quality = set()

                        # Check if the var unit matches the BASIN-3D unit, if not get the lookup conversion
                        unit = ds.variables[var].attrs['units']
                        unit_conv, unit_str = self._get_unit_conv(unit, var, synthesis_messages)

                        var_missing_value = ds[var].encoding.get('missing_value')
                        var_data = ds[var].values
                        qc_var_name = f'qc_{var}'
                        has_qc_values = qc_var_name in ds.variables
                        qc_values = ds[qc_var_name].values if has_qc_values else None

                        # confirm the data value and quality array lengths are the same.
                        if qc_values is not None and len(qc_values) != len(var_data):
                            msg = (f'{mf_id} variable {var} is not the same length as its corresponding {qc_var_name}. '
                                   f'Skipping.')
                            logger.warning(msg)
                            synthesis_messages.append(msg)
                            del var_data
                            del qc_values
                            continue

                        if query.result_quality and not has_qc_values:
                            msg = (f'{mf_id} variable {var} did not have quality control information to filter on. '
                                   'Returning all values.')
                            logger.info(msg)
                            synthesis_messages.append(msg)

                        result_TVPs, result_TVP_quality, filtered_count = _build_tvp_results(
                            timestamps, var_data, var_missing_value, unit_conv, qc_values, query.result_quality)

                        # clear memory
                        del var_data
                        del qc_values

                        # If there is a corresponding qc variable
                        qc_var = ''
                        if has_qc_values:
                            result_quality = set(result_TVP_quality)
                            qc_var = f'::{qc_var_name}'

                        if filtered_count:
                            msg = (f'Filtered {filtered_count} values for {mf_id} variable {var} '
                                   'based on the result quality query.')
                            logger.info(msg)
                            synthesis_messages.append(msg)

                        # Create the MeasurementTVPObservation object
                        measurement_timeseries_tvp_observation = MeasurementTimeseriesTVPObservation(
                            self,
                            id=f'{data_product_name}--{var}{qc_var}',
                            unit_of_measurement=unit_str,
                            feature_of_interest_type=FeatureTypeEnum.POINT,
                            feature_of_interest=monitoring_feature,
                            result=ResultListTVP(plugin_access=self, value=result_TVPs, result_quality=result_TVP_quality),
                            observed_property=var,
                            result_quality=list(result_quality),
                            aggregation_duration=query.aggregation_duration[0],
                            time_reference_position=TimeMetadataMixin.TIME_REFERENCE_MIDDLE,
                        )

                        # clear memory
                        del result_TVP_quality

                        if mf_info['citation'] not in synthesis_citations:
                            synthesis_citations.append(mf_info['citation'])

                        yield measurement_timeseries_tvp_observation

            finally:
                # Remove the per-request zarr store while preserving the configured parent directory.
                if zarr_path is not None and zarr_path.exists():
                    shutil.rmtree(zarr_path)

        return StopIteration(PluginIteratorResult(synthesis_messages, synthesis_citations))


@basin3d_plugin
class ARMDataSourcePlugin(DataSourcePluginPoint):
    """Register the Atmospheric Radiation Measurement data source plugin.

    The plugin exposes ARM point monitoring features and ARM measurement
    timeseries through :class:`ARMMonitoringFeatureAccess` and
    :class:`ARMMeasurementTimeseriesTVPObservationAccess`.
    """

    title = 'Atmospheric Radiation Measurement Data Source Plugin'
    plugin_access_classes = (ARMMonitoringFeatureAccess, ARMMeasurementTimeseriesTVPObservationAccess)

    feature_types = ['POINT']

    class DataSourceMeta:
        """Static identity and endpoint metadata for the ARM data source."""

        id = 'ARM'
        location = 'https://metadata-api.svcs.arm.gov'
        id_prefix = 'ARM'
        name = 'Atmospheric Radiation Measurement'
