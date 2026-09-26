"""AmeriFlux data source plugin mockup.

This module defines the BASIN-3D plugin shape for AmeriFlux. Data retrieval
will be implemented in a subsequent change.
"""
import csv
import hashlib
import io
import os
import shutil
import tempfile
import zipfile

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, TypedDict

try:
    import xarray as _xr
    xr: Any = _xr
except ImportError:
    xr = None

from basin3d.core import monitor

from basin3d.core.access import get_url, post_url
from basin3d.core.models import (AbsoluteCoordinate, AltitudeCoordinate, Coordinate, GeographicCoordinate,
                                 HorizontalCoordinate, RepresentativeCoordinate,
                                 MeasurementTimeseriesTVPObservation, MonitoringFeature, DepthCoordinate,
                                 ResultListTVP, TimeMetadataMixin, TimeValuePair)
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin, separate_list_types
from basin3d.core.schema.enum import FeatureTypeEnum, StatisticEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.types import SpatialSamplingShapes


logger = monitor.get_logger(__name__)


def _require_amf_dependencies():
    if xr is None:
        raise ImportError('AmeriFlux plugin dependencies are not installed. Install them with: '
                          'pip install "basin3d[ameriflux]"')


DEFAULT_USE_DESC = 'Use not specified'
DEFAULT_TEMP_DIR = tempfile.gettempdir()

AMF_USER_NAME = os.environ.get('AMF_USER_NAME', None)
AMF_USER_EMAIL = os.environ.get('AMF_USER_EMAIL', None)
AMF_DATA_INTENDED_USE_ENUM = os.environ.get('AMF_DATA_INTENDED_USE', 'other')
AMF_DATA_USE_DESC = os.environ.get('AMF_DATA_USE_DESCRIPTION', DEFAULT_USE_DESC)
LOCAL_TEMP_DIR = os.environ.get('BASIN3D_LOCAL_TEMP_DIR', DEFAULT_TEMP_DIR)

# Docs https://amfcdn.lbl.gov/docs#post-/api/v2/data_download
INTENDED_USE_ENUM = ('synthesis', 'model', 'remote_sensing', 'other_research', 'education', 'other')


@dataclass
class _SiteMetadata:
    site_id: str
    site_name: str
    description: str
    latitude: float | None
    longitude: float | None
    elevation: float | None
    start_year: int
    end_year: int
    igbp: str
    url: str | None


class _UnitLookupInfo(TypedDict):
    amf_unit: str
    target_unit: str
    conv: int | float


AMF_UNIT_LOOKUP: Dict[str, _UnitLookupInfo] = {
    'kPa-mm Hg': {'amf_unit': 'kPa', 'target_unit': 'mm Hg', 'conv': 7.500616683},
    'W m-2-W/m2': {'amf_unit': 'W m-2', 'target_unit': 'W/m2', 'conv': 1},
    'm s-1-m/s': {'amf_unit': 'm s-1', 'target_unit': 'm/s', 'conv': 1},
    'Decimal degrees-degrees': {'amf_unit': 'Decimal degrees', 'target_unit': 'degrees', 'conv': 1},
    'deg C-C': {'amf_unit': 'deg C', 'target_unit': 'C', 'conv': 1},
}

INDEXED_VARIABLE_BASES = ('TS_F_MDS', 'SWC_F_MDS')
AMF_MISSING_VALUE = -9999
AMF_TIMESTAMP_FORMATS = {
    'HH': '%Y%m%d%H%M',
    'DD': '%Y%m%d',
    'MM': '%Y%m',
    'YY': '%Y',
}
AMF_BIF_REQUIRED_FIELDS = {'SITE_ID', 'GROUP_ID', 'VARIABLE_GROUP', 'VARIABLE', 'DATAVALUE'}


def _get_metadata(url: str, synthesis_messages: List[str]) -> Dict:
    """

    :param url:
    :param synthesis_messages:
    :return:
    """
    results: Dict = {}

    try:
        response = get_url(url)

        if not response or response.status_code != 200:
            status_code = response.status_code if response else 'NO RESPONSE'
            msg = f'AMF request for {url} returned error code {status_code}.'
            logger.error(msg)
            synthesis_messages.append(msg)
            return results

        response_json = response.json()

        if not isinstance(response_json, dict):
            response_class = response_json.__class__.__name__
            msg = (f'AMF metadata response for {url} was not in expected dictionary format. '
                   f'It was {response_class} class.')
            logger.error(msg)
            synthesis_messages.append(msg)
            return results

        return response_json

    except Exception as e:
        msg = f'AMF request for {url} failed: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        return results


def _parse_metadata(metadata: Dict, synthesis_messages: List[str]) -> Dict:
    """

    :param metadata:
    :param synthesis_messages:
    :return:
    """

    parsed_metadata: Dict = {}

    metadata_list = metadata.get('values', [])

    if not metadata_list or not isinstance(metadata_list, List):
        msg = 'AMF metadata information was not in expected format.'
        logger.error(msg)
        synthesis_messages.append(msg)
        return parsed_metadata

    for site_metadata in metadata_list:
        fluxnet_years = site_metadata.get('grp_publish_fluxnet', [])

        if not fluxnet_years or not isinstance(fluxnet_years, List):
            continue

        site_id = site_metadata.get('site_id', None)

        if site_id is None:
            continue

        location = site_metadata.get('grp_location', {})
        latitude = location.get('location_lat', None)
        longitude = location.get('location_long', None)
        elevation = location.get('location_elev', None)

        if latitude is None or longitude is None:
            continue

        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            msg = f'AMF {site_id} latitude and longitude values could not be converted to numeric values. Skipping site.'
            logger.error(msg)
            synthesis_messages.append(msg)
            continue

        if elevation is not None:
            try:
                elevation = float(elevation)
            except ValueError:
                msg = f'AMF {site_id} elevation values could not be converted to numeric values.'
                logger.warning(msg)
                synthesis_messages.append(msg)
                elevation = None

        start_year = fluxnet_years[0]
        end_year = fluxnet_years[-1]

        site_name = site_metadata.get('site_name', 'NO VALUE')
        description = site_metadata.get('site_desc', 'NO VALUE')

        igbp_info = site_metadata.get('grp_igbp', {})
        igbp = igbp_info.get('igbp', 'NO VALUE')
        url = site_metadata.get('url_ameriflux', None)

        parsed_metadata[site_id] = _SiteMetadata(
            site_id=site_id,
            site_name=site_name,
            description=description,
            latitude=latitude,
            longitude=longitude,
            elevation=elevation,
            start_year=start_year,
            end_year=end_year,
            igbp=igbp,
            url=url,
        )

    return parsed_metadata


def _inside_bbox(bbox: Tuple[float, float, float, float], lat: float, lon: float) -> bool:
    """

    :param bbox:
    :param lat:
    :param lon:
    :return:
    """
    west, south, east, north = bbox
    return west <= lon <= east and south <= lat <= north


def _matches_named(site_id: str, named_ids: Set[str]) -> bool:
    """Return whether a site ID matches a requested monitoring feature ID."""
    return site_id in named_ids


def _matches_any_bbox(site_info: _SiteMetadata, bounding_boxes: List[tuple]) -> bool:
    """Return whether a site falls inside any requested bounding box."""
    if site_info.latitude is None or site_info.longitude is None:
        return False
    return any(_inside_bbox(bbox, site_info.latitude, site_info.longitude)
               for bbox in bounding_boxes)


def _filter_sites(site_info_lookup: Dict, mf_named_ids: Set[str], mf_bbox_list: List[tuple],
                  query_start_year: int, query_end_year: Optional[int]) -> List[str]:

    filtered_sites = []

    for site_id, site_info in site_info_lookup.items():

        if not (_matches_named(site_id, mf_named_ids) or _matches_any_bbox(site_info, mf_bbox_list)):
            continue

        site_start_year = int(site_info.start_year)
        site_end_year = int(site_info.end_year)

        if not (site_end_year >= query_start_year and (
                query_end_year is None or site_start_year <= query_end_year)):
            continue

        filtered_sites.append(site_id)

    return filtered_sites


def _get_variable_names(observed_property: str, available_variables: Set[str]) -> List[str]:
    """Return matching data variables, including indexed soil variables."""
    if observed_property in INDEXED_VARIABLE_BASES:
        variable_prefix = f'{observed_property}_'
        variable_names = [
            variable_name for variable_name in available_variables if
            variable_name.startswith(variable_prefix) and
            variable_name[len(variable_prefix):].isdigit()
        ]
        return sorted(variable_names, key=lambda name: int(name[len(variable_prefix):]))

    return [observed_property] if observed_property in available_variables else []


def _get_height_depth_changes(var_info: List[Dict[str, str]]) -> List[Tuple[float, str | None]]:
    """Return the initial height and subsequent height changes for a variable."""
    height_depth_changes: List[Tuple[float, str | None]] = []
    previous_height: float | None = None

    for var_info_entry in var_info:
        if 'VAR_INFO_HEIGHT' not in var_info_entry:
            continue

        height = float(var_info_entry['VAR_INFO_HEIGHT'])
        if not height_depth_changes:
            height_depth_changes.append((height, None))
        elif height != previous_height:
            height_depth_changes.append((height, var_info_entry.get('VAR_INFO_DATE')))

        previous_height = height

    return height_depth_changes


def _format_tvp_timestamp(timestamp: Any, file_resolution: str,
                          utc_offset: str | None = None) -> str:
    """Format an xarray timestamp at its source data resolution as ISO text."""
    import numpy as np

    timestamp_units: Dict[str, Literal['Y', 'M', 'D', 'm']] = {
        'HH': 'm', 'DD': 'D', 'MM': 'M', 'YY': 'Y'}
    formatted_timestamp = str(np.datetime_as_string(
        np.datetime64(timestamp), unit=timestamp_units[file_resolution]))
    if file_resolution == 'HH':
        formatted_timestamp = f'{formatted_timestamp}{_format_utc_offset(utc_offset)}'
    return formatted_timestamp


def _format_utc_offset(utc_offset: str | None) -> str:
    """Format a string UTC hour offset as an ISO timezone suffix."""
    if not utc_offset:
        return ''

    try:
        offset = float(utc_offset)
    except (TypeError, ValueError):
        return ''

    sign = '-' if str(utc_offset).strip().startswith('-') else '+'
    absolute_offset = abs(offset)
    hours = int(absolute_offset)
    minutes = round((absolute_offset - hours) * 60)
    return f'{sign}{hours:02d}:{minutes:02d}'


def _build_tvp_results(time_values: List, data_values: Any, unit_conv: int | float,
                       file_resolution: str, utc_offset: str | None = None,
                       quality_values: Any = None,
                       requested_qualities: Optional[List] = None) -> Tuple[List, List, int]:
    """Build time-value pairs with optional quality filtering and unit conversion."""
    results_tvp: List[TimeValuePair] = []
    result_quality: List[Any] = []
    filtered_count = 0
    requested_quality_values: Set[int] = {
        int(quality) for quality in (requested_qualities or [])}
    has_quality_values = quality_values is not None

    for index, (timestamp, value) in enumerate(zip(time_values, data_values)):
        quality = quality_values[index] if quality_values is not None else None
        if quality is not None and hasattr(quality, 'item'):
            quality = quality.item()

        if (requested_quality_values and has_quality_values and
                quality not in requested_quality_values):
            filtered_count += 1
            continue

        # Preserve the AMF missing-value sentinel instead of converting -9999.
        if unit_conv != 1 and value != AMF_MISSING_VALUE:
            value = value * unit_conv
        # xarray values may be NumPy scalars; convert them to native Python values.
        if hasattr(value, 'item'):
            value = value.item()

        timestamp = _format_tvp_timestamp(timestamp, file_resolution, utc_offset)
        results_tvp.append(TimeValuePair(timestamp=timestamp, value=value))
        if has_quality_values:
            result_quality.append(quality)

    return results_tvp, result_quality, filtered_count


def _get_download_info(base_url: str, sites: List[str], synthesis_messages: List) -> List:
    """

    :param sites:
    :param synthesis_messages:
    :return:
    """
    download_info: List[Dict[str, Any]] = []

    intended_use = AMF_DATA_INTENDED_USE_ENUM
    if not isinstance(intended_use, str) or intended_use not in INTENDED_USE_ENUM:
        intended_use = INTENDED_USE_ENUM[-1]

    use_desc = DEFAULT_USE_DESC if not isinstance(AMF_DATA_USE_DESC, str) else AMF_DATA_USE_DESC
    use_desc = f'{use_desc} (data accessed via BASIN-3D)'

    # Documentation https://amfcdn.lbl.gov/docs#post-/api/v2/data_download
    url = f'{base_url}/data_download'

    payload = {"user_id": AMF_USER_NAME, "user_email": AMF_USER_EMAIL, "site_ids": sites,
               "intended_use": intended_use, "description": use_desc,
               "data_policy": "CCBY4.0", "data_product": "FLUXNET", "data_variant": "FULLSET"}

    try:
        response = post_url(url, json=payload, headers={'Content-Type': 'application/json'})

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
            msg = (f'AMF download request for {url} returned error code '
                   f'{status_code}{detail}.')
            logger.error(msg)
            synthesis_messages.append(msg)
            return download_info

        response_json = response.json()

        if not isinstance(response_json, Dict):
            response_class = response_json.__class__.__name__
            msg = (f'AMF download response for {url} was not in expected dictionary format. '
                   f'It was {response_class} class.')
            logger.error(msg)
            synthesis_messages.append(msg)
            return download_info

        data_urls = response_json.get('data_urls', [])

        if not isinstance(data_urls, List):
            response_class = data_urls.__class__.__name__
            msg = (f'AMF download response for {url} did not contain data_urls as a list. '
                   f'It was {response_class} class.')
            logger.error(msg)
            synthesis_messages.append(msg)
            return download_info

        return data_urls

    except Exception as e:
        msg = f'AMF download request for {url} failed: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)

    return download_info


def _make_download_lookup(download_info: List, synthesis_messages: List) -> Dict:
    """

    :param download_info:
    :param synthesis_messages:
    :return:
    """
    download_lookup = {}

    for data_info in download_info:
        required_fields = ('site_id', 'url', 'download_checksum')
        if not isinstance(data_info, dict):
            # This should never happen
            msg = ('AMF download information element from data_download request was not '
                   'a dictionary. Skipping entry.')
            logger.warning(msg)
            synthesis_messages.append(msg)
            continue

        missing_fields = [field for field in required_fields if not data_info.get(field)]
        if missing_fields:
            # This should never happen
            msg = ('AMF download information from data_download request is missing required fields '
                   f'{missing_fields}. Skipping entry.')
            logger.warning(msg)
            synthesis_messages.append(msg)
            continue

        site_id = data_info['site_id']
        data_url = data_info['url']
        data_checksum = data_info['download_checksum']
        download_lookup[site_id] = {'url': data_url, 'checksum': data_checksum}

    return download_lookup


def _validate_zarr_path(zarr_path: Path, synthesis_messages: List) -> bool:
    """Validate the parent directory used for temporary Zarr stores."""
    if not zarr_path.exists():
        msg = (f'AMF zarr path does not exist: {zarr_path}. The local directory for temporary files should be '
               'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
        logger.error(msg)
        synthesis_messages.append(msg)
        return False

    if not zarr_path.is_dir():
        msg = (f'AMF zarr path is not a directory: {zarr_path}. The local directory for temporary files should be '
               'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR. Please double check your configuration.')
        logger.error(msg)
        synthesis_messages.append(msg)
        return False

    if not os.access(zarr_path, os.W_OK):
        msg = (f'AMF zarr path is not writable: {zarr_path}. The local directory for temporary files can be '
               'set as the environmental variable: BASIN3D_LOCAL_TEMP_DIR, otherwise the default working directory '
               'is used. Please double check your configuration.')
        logger.error(msg)
        synthesis_messages.append(msg)
        return False

    return True


def _create_zarr_temp_dir(zarr_path_str: str, synthesis_messages: List) -> Path | None:
    """Create a per-request temporary directory beneath the configured Zarr path."""
    zarr_path = Path(zarr_path_str)
    if not _validate_zarr_path(zarr_path, synthesis_messages):
        return None

    try:
        return Path(tempfile.mkdtemp(prefix='basin3d-amf-', dir=zarr_path))
    except OSError as e:
        msg = f'Failed to create AMF zarr temporary directory in {zarr_path}: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        return None


def _download_zip_to_temp(url: str, checksum: str, synthesis_messages: List[str]) -> Path | None:
    """Stream a ZIP response to a temporary file and return its path."""
    response = None
    zip_path = None

    try:
        if not checksum or not isinstance(checksum, str):
            msg = f'AMF ZIP download for {url} did not include a valid checksum.'
            logger.error(msg)
            synthesis_messages.append(msg)
            return None

        checksum = checksum.strip().lower()
        checksum_parts = checksum.split(':', 1)
        algorithm: str | None = None
        if len(checksum_parts) == 2:
            algorithm, expected_checksum = checksum_parts
        else:
            expected_checksum = checksum
            algorithm = {32: 'md5', 40: 'sha1', 64: 'sha256'}.get(len(checksum))

        if algorithm not in {'md5', 'sha1', 'sha256'} or not expected_checksum:
            msg = f'AMF ZIP download for {url} had an unsupported checksum format.'
            logger.error(msg)
            synthesis_messages.append(msg)
            return None

        file_hash = hashlib.new(algorithm)
        response = get_url(url, stream=True)
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
            msg = (f'AMF data request for {url} returned error code '
                   f'{status_code}{detail}.')
            logger.error(msg)
            synthesis_messages.append(msg)
            return None

        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            zip_path = Path(temp_file.name)
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    temp_file.write(chunk)
                    file_hash.update(chunk)

        if file_hash.hexdigest().lower() != expected_checksum:
            msg = f'AMF ZIP download for {url} failed checksum verification.'
            logger.error(msg)
            synthesis_messages.append(msg)
            zip_path.unlink()
            return None

        return zip_path
    except Exception as e:
        msg = f'AMF ZIP download for {url} failed: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        if zip_path and zip_path.exists():
            zip_path.unlink()
        return None
    finally:
        if response is not None:
            response.close()


def _get_zip_members(archive: zipfile.ZipFile, data_member_prefix: str,
                     lookup_member_prefix: str, bif_member_prefix: str,
                     synthesis_messages: List[str]) -> Tuple[zipfile.ZipInfo, zipfile.ZipInfo,
                                                             zipfile.ZipInfo] | None:
    """Find the requested CSV members without extracting the archive."""
    def find_member(prefix: str) -> zipfile.ZipInfo | None:
        members = [member for member in archive.infolist() if member.filename.startswith(prefix)]
        if len(members) == 1:
            return members[0]

        msg = f'AMF ZIP archive expected one member beginning with {prefix}, found {len(members)}.'
        logger.error(msg)
        synthesis_messages.append(msg)
        return None

    data_member = find_member(data_member_prefix)
    lookup_member = find_member(lookup_member_prefix)
    bif_member = find_member(bif_member_prefix)
    if data_member is None or lookup_member is None or bif_member is None:
        return None

    return data_member, lookup_member, bif_member


def _read_lookup_csv(archive: zipfile.ZipFile, lookup_member: zipfile.ZipInfo,
                     synthesis_messages: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """Read adjacent BIFVARINFO rows into variable metadata groups."""
    lookup: Dict[str, List[Dict[str, str]]] = {}
    current_group: Dict[str, str] = {}
    current_group_id = None

    def save_group() -> None:
        if not current_group:
            return

        var_name = current_group.get('VAR_INFO_VARNAME')
        if not var_name:
            msg = 'AMF BIFVARINFO group did not contain VAR_INFO_VARNAME. Skipping group.'
            logger.warning(msg)
            return

        lookup.setdefault(var_name, []).append(current_group.copy())

    rows = _read_bif_csv_rows(archive, lookup_member, 'BIFVARINFO', synthesis_messages)
    if rows is None:
        return lookup

    for row in rows:
        if row.get('VARIABLE_GROUP') != 'GRP_VAR_INFO':
            save_group()
            current_group = {}
            current_group_id = None
            continue

        group_id = row.get('GROUP_ID')
        if current_group and group_id != current_group_id:
            save_group()
            current_group = {}

        if not current_group:
            current_group_id = group_id
            current_group = {
                'SITE_ID': row.get('SITE_ID', ''),
                'GROUP_ID': group_id or '',
                'VARIABLE_GROUP': row.get('VARIABLE_GROUP', '')
            }

        variable = row.get('VARIABLE')
        if variable:
            current_group[variable] = row.get('DATAVALUE', '')

    save_group()

    return lookup


def _read_bif_csv_rows(archive: zipfile.ZipFile, bif_member: zipfile.ZipInfo,
                       file_type: str, synthesis_messages: List[str]) -> List[Dict[str, str]] | None:
    """Read and validate rows from an AmeriFlux BIF CSV member."""
    logger.info(f'Processing AMF {file_type} file {bif_member.filename}.')

    try:
        with archive.open(bif_member) as member:
            csv_bytes = member.read()
            try:
                csv_text = csv_bytes.decode('utf-8')
            except UnicodeDecodeError:
                csv_text = csv_bytes.decode('cp1252')
                logger.info(f'Using Windows-1252 encoding for AMF {file_type} file {bif_member.filename}.')

            reader = csv.DictReader(io.StringIO(csv_text))
            if not reader.fieldnames or not AMF_BIF_REQUIRED_FIELDS.issubset(reader.fieldnames):
                missing_fields = AMF_BIF_REQUIRED_FIELDS - set(reader.fieldnames or [])
                msg = (f'AMF {file_type} {bif_member.filename} CSV is missing required columns: '
                       f'{sorted(missing_fields)}.')
                logger.error(msg)
                synthesis_messages.append(msg)
                return None

            return list(reader)
    except Exception as e:
        msg = f'AMF {file_type} {bif_member.filename} CSV processing failed: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        return None


def _read_bif_utc_offset(archive: zipfile.ZipFile, bif_member: zipfile.ZipInfo,
                         synthesis_messages: List[str]) -> str | None:
    """Read the string UTC offset from a site BIF CSV."""
    rows = _read_bif_csv_rows(archive, bif_member, 'BIF', synthesis_messages)
    if rows is None:
        return None

    for row in rows:
        if row.get('VARIABLE') == 'UTC_OFFSET':
            utc_offset = row.get('DATAVALUE')
            if utc_offset:
                return utc_offset.strip()

    msg = f'AMF BIF {bif_member.filename} did not contain a UTC_OFFSET value.'
    logger.warning(msg)
    synthesis_messages.append(msg)

    return None


def _dataframe_chunk_to_xarray(dataframe: Any, timestamp_column: str) -> Any:
    """Convert one data CSV chunk into an xarray Dataset."""
    import pandas as pd

    dataframe[timestamp_column] = pd.to_datetime(dataframe[timestamp_column])
    dataframe = dataframe.set_index(timestamp_column)
    dataframe.index.name = 'time'
    dataset = xr.Dataset.from_dataframe(dataframe)
    dataset.attrs['timestamp_column'] = timestamp_column
    return dataset


def _data_csv_to_zarr(archive: zipfile.ZipFile, data_member: zipfile.ZipInfo,
                      zarr_path: Path, timestamp_column: str,
                      timestamp_format: str, start_date: Any, end_date: Any,
                      chunk_size: int,
                      synthesis_messages: List[str]) -> Any | None:
    """Convert a data CSV to a disk-backed xarray Dataset in chunks."""
    _require_amf_dependencies()
    import pandas as pd

    first_chunk = True
    start_timestamp = pd.Timestamp(start_date)
    end_timestamp = pd.Timestamp(end_date) + pd.Timedelta(days=1) if end_date else None

    try:
        with archive.open(data_member) as member:
            with io.TextIOWrapper(member, encoding='utf-8', newline='') as text:
                for dataframe in pd.read_csv(text, chunksize=chunk_size):
                    timestamp_values = dataframe[timestamp_column].astype(str).str.strip()
                    dataframe[timestamp_column] = pd.to_datetime(
                        timestamp_values, format=timestamp_format)
                    timestamp_values = dataframe[timestamp_column]
                    chunk_min = timestamp_values.min()
                    chunk_max = timestamp_values.max()

                    if chunk_max < start_timestamp:
                        continue
                    if end_timestamp is not None and chunk_min >= end_timestamp:
                        break

                    data_filter = timestamp_values >= start_timestamp
                    if end_timestamp is not None:
                        data_filter &= timestamp_values < end_timestamp
                    dataframe = dataframe.loc[data_filter]
                    stop_after_chunk = end_timestamp is not None and chunk_max >= end_timestamp
                    if dataframe.empty:
                        if stop_after_chunk:
                            break
                        continue

                    dataset = _dataframe_chunk_to_xarray(dataframe, timestamp_column)
                    try:
                        if first_chunk:
                            dataset.to_zarr(zarr_path, mode='w', consolidated=False)
                            first_chunk = False
                        else:
                            dataset.to_zarr(zarr_path, mode='a', append_dim='time', consolidated=False)
                    finally:
                        dataset.close()

                    if stop_after_chunk:
                        break

        if first_chunk:
            msg = f'AMF data CSV {data_member.filename} did not contain rows matching the query dates.'
            logger.error(msg)
            synthesis_messages.append(msg)
            return None

        return xr.open_zarr(zarr_path, consolidated=False)
    except Exception as e:
        msg = f'AMF data CSV processing failed: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        return None


def _process_download_zip(url: str, checksum: str, data_member_prefix: str,
                          lookup_member_prefix: str, timestamp_column: str,
                          timestamp_format: str, start_date: Any,
                          end_date: Any, zarr_path: Path,
                          bif_member_prefix: str, synthesis_messages: List[str],
                          chunk_size: int = 10000) -> Tuple[Dict, Any | None, str | None]:
    """Download a ZIP and process its lookup and data CSV members."""
    lookup: Dict[str, List[Dict[str, str]]] = {}
    utc_offset = None
    zip_path = _download_zip_to_temp(url, checksum, synthesis_messages)
    if zip_path is None:
        return lookup, None, utc_offset

    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = _get_zip_members(archive, data_member_prefix, lookup_member_prefix,
                                       bif_member_prefix, synthesis_messages)
            if members is None:
                return lookup, None, utc_offset

            data_member, lookup_member, bif_member = members
            lookup = _read_lookup_csv(archive, lookup_member, synthesis_messages)
            utc_offset = _read_bif_utc_offset(archive, bif_member, synthesis_messages)
            dataset = _data_csv_to_zarr(archive, data_member, zarr_path, timestamp_column,
                                        timestamp_format, start_date, end_date,
                                        chunk_size, synthesis_messages)
            return lookup, dataset, utc_offset
    except zipfile.BadZipFile as e:
        msg = f'AMF download was not a valid ZIP archive: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        return lookup, None, utc_offset
    finally:
        if zip_path.exists():
            zip_path.unlink()


def _load_mf_object(datasource: DataSourcePluginAccess, site_info: _SiteMetadata,
                    observed_properties: List, height_depth: List | None = None) -> MonitoringFeature | None:
    """

    :param datasource:
    :param site_info:
    :param observed_properties:
    :param height_depth:
    :return:
    """

    coord = Coordinate(
            absolute=AbsoluteCoordinate(
                horizontal_position=[GeographicCoordinate(
                    **{"latitude": site_info.latitude,
                       "longitude": site_info.longitude,
                       "datum": HorizontalCoordinate.DATUM_WGS84,
                       "units": GeographicCoordinate.UNITS_DEC_DEGREES})]))

    if site_info.elevation is not None:
        coord.absolute.vertical_extent = [AltitudeCoordinate(
            **{"value": site_info.elevation,
               "distance_units": AltitudeCoordinate.DISTANCE_UNITS_METERS,
               "datum": DepthCoordinate.DATUM_MEAN_SEA_LEVEL})]

    extra_desc = ''
    if height_depth is not None and len(height_depth) == 1:
        coord.representative = RepresentativeCoordinate(
            vertical_position=DepthCoordinate(
                **{"value": height_depth[0][0],
                   "distance_units": DepthCoordinate.DISTANCE_UNITS_METERS,
                   "datum": DepthCoordinate.DATUM_LOCAL_SURFACE}))
    elif height_depth is not None:
        hd_str = ', '.join(
            f'{height:.2f}m beginning at {date}' if date is not None else f'{height:.2f}m beginning at start'
            for height, date in height_depth)
        extra_desc = (f'; NOTE: This variable has height/depth changes: {hd_str}. '
                      f'The height information is not contained in the coordinates.')

    igbp = f' IGBP Vegetation Type: {site_info.igbp}.' if site_info.igbp else ''
    url = f' {site_info.url}' if site_info.url else ''
    desc = f'{site_info.description}{extra_desc}{igbp}{url}{extra_desc}'

    mf = MonitoringFeature(
        datasource,
        id=site_info.site_id,
        name=site_info.site_name,
        feature_type=FeatureTypeEnum.POINT,
        shape=SpatialSamplingShapes.SHAPE_POINT,
        description=desc,
        observed_properties=observed_properties,
        coordinates=coord,
    )

    return mf


class AMFMonitoringFeatureAccess(DataSourcePluginAccess):
    """Access for AmeriFlux monitoring features."""

    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature):
        """Return an iterator of monitoring features available for query."""

        synthesis_messages: List[str] = []

        if query.parent_feature is not None:
            msg = 'AmeriFlux does not support filtering monitoring features by parent feature specification.'
            logger.warning(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        feature_type = isinstance(query.feature_type,
                                  FeatureTypeEnum) and query.feature_type.value or query.feature_type

        if feature_type in AMFDataSourcePlugin.feature_types or feature_type is None:

            metadata_url = f'{self.datasource.location}/site_info_display/AmeriFlux'

            metadata = _get_metadata(metadata_url, synthesis_messages)

            if not metadata:
                msg = f'No metadata found for {metadata_url}'
                logger.warning(msg)
                synthesis_messages.append(msg)
                return StopIteration(synthesis_messages)

            metadata_lookup = _parse_metadata(metadata, synthesis_messages)

            mf_named: List[str] = []
            mf_bbox: List[tuple] = []

            if query.monitoring_feature:
                # split up mf query types
                mf_types = separate_list_types(
                    query.monitoring_feature, {'named': str, 'bbox': tuple})
                mf_named = mf_types.get('named', [])
                mf_bbox = mf_types.get('bbox', [])

            named_ids = set(mf_named)
            has_monitoring_feature_filter = bool(named_ids or mf_bbox)
            observed_property_mappings = list(self.get_attribute_mappings(attr_type='OBSERVED_PROPERTY'))
            observed_props = [ma.datasource_vocab for ma in observed_property_mappings]

            # Loop through metadata once. Named and bbox filters are combined
            # with OR semantics, and bbox checks stop at the first match.
            for site_id, site_info in metadata_lookup.items():
                if has_monitoring_feature_filter and not (
                        _matches_named(site_id, named_ids) or _matches_any_bbox(site_info, mf_bbox)):
                    continue
                mf_obj = _load_mf_object(self, site_info, observed_props)
                if mf_obj:
                    yield mf_obj

        else:
            msg = (f'AmeriFlux does not support specified feature type: {feature_type}. '
                   f'Only feature types {AMFDataSourcePlugin.feature_types} are supported.')
            logger.warning(msg)
            synthesis_messages.append(msg)

        return StopIteration(synthesis_messages)


class AMFMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """Access for AmeriFlux measurement time series."""

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def _get_unit_conv(self, amf_unit: str | None, amf_variable: str,
                       synthesis_messages: List) -> Tuple[int | float, str | None]:
        """Return the AMF-to-BASIN-3D conversion factor and output unit."""
        if amf_unit is None:
            return 1, None

        b3d_mapping = self.get_datasource_attribute_mapping('OBSERVED_PROPERTY', amf_variable)
        b3d_unit = b3d_mapping.basin3d_desc[0].units
        unit_info = AMF_UNIT_LOOKUP.get(f'{amf_unit}-{b3d_unit}')

        if unit_info:
            return unit_info['conv'], unit_info['target_unit']

        if b3d_unit != amf_unit:
            msg = (f'Unit for {amf_variable} was unexpected and unit conversion to BASIN-3D unit could not be '
                   f'assessed. Returning values in AMF native unit {amf_unit}.')
            logger.warning(msg)
            synthesis_messages.append(msg)

        return 1, amf_unit

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """
        Return an iterator of measurement time series observations available for query.
        Every data product will have the observed properties with the exception of TS and SWC.
        TS and SWC have variable numbers of measurements with prefix indices. Will need to loop thru.
        The height and depth information will be in the VAR_INFO file.
        Need to filter on quality where the QC variables exist.
        :param query:
        :return:
        """

        synthesis_messages: list = []

        if AMF_USER_NAME is None or AMF_USER_EMAIL is None:
            msg = 'No AmeriFlux username or email configured in the environment variables. Cannot acquire AmeriFlux data.'
            logger.error(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        if not _validate_zarr_path(Path(LOCAL_TEMP_DIR), synthesis_messages):
            return StopIteration(synthesis_messages)

        if not query.monitoring_feature:
            msg = f'No monitoring features for AmeriFlux were specified or they were not specified with the {self.datasource.id_prefix} prefix.'
            logger.warning(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        if query.statistic and StatisticEnum.MEAN not in query.statistic:
            msg = f'AmeriFlux FLUXNET data product only supports statistic {StatisticEnum.MEAN}.'
            logger.warning(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        # Create the site_info lookup
        metadata_url = f'{self.datasource.location}/site_info_display/AmeriFlux'

        metadata = _get_metadata(metadata_url, synthesis_messages)

        if not metadata:
            msg = f'No metadata found for {metadata_url}'
            logger.warning(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        metadata_lookup = _parse_metadata(metadata, synthesis_messages)

        # separate the query monitoring feature information into named and bbox components
        mf_types = separate_list_types(
            query.monitoring_feature, {'named': str, 'bbox': tuple})
        mf_named = mf_types.get('named', [])
        mf_bbox = mf_types.get('bbox', [])

        # Loop thru the sites.
        named_ids = set(mf_named)
        query_start_year = query.start_date.year
        query_end_year = query.end_date.year if query.end_date else None

        # get the list of site with data that match the monitoring feature and date query parameters
        site_list = _filter_sites(metadata_lookup, named_ids, mf_bbox, query_start_year, query_end_year)

        if not site_list:
            msg = 'No data matches query specification.'
            logger.info(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        # Get the download information
        data_download_info = _get_download_info(self.datasource.location, site_list, synthesis_messages)

        if not data_download_info:
            sites_str = ', '.join(site_list)
            msg = f'Expected download information for sites {sites_str} was not retrieved.'
            logger.warning(msg)
            synthesis_messages.append(msg)

        download_site_lookup = _make_download_lookup(data_download_info, synthesis_messages)

        file_resolution = query.aggregation_duration[0]
        var_info_filename_part = f'_FLUXNET_BIFVARINFO_{file_resolution}_'
        data_filename_part = f'_FLUXNET_FLUXMET_{file_resolution}_'
        time_header = 'TIMESTAMP_START'
        if file_resolution != 'HH':
            time_header = 'TIMESTAMP'
        timestamp_format = AMF_TIMESTAMP_FORMATS[file_resolution]

        for site_id in site_list:

            if site_id not in download_site_lookup:
                msg = f'Site {site_id} not found in downloaded site information. Skipping.'
                logger.warning(msg)
                synthesis_messages.append(msg)
                continue

            site_url = download_site_lookup[site_id].get('url')
            zip_checksum = download_site_lookup[site_id].get('checksum')

            # Download the data file, extracting the VAR_INFO BIF and the aggregation_duration resolution data file.
            data_member_prefix = f'AMF_{site_id}{data_filename_part}'
            lookup_member_prefix = f'AMF_{site_id}{var_info_filename_part}'
            bif_member_prefix = f'AMF_{site_id}_FLUXNET_BIF_'
            zarr_path = None
            data_xarray = None
            var_info_lookup = None

            try:
                zarr_path = _create_zarr_temp_dir(LOCAL_TEMP_DIR, synthesis_messages)
                if zarr_path is None:
                    continue

                var_info_lookup, data_xarray, utc_offset = _process_download_zip(
                    site_url, zip_checksum, data_member_prefix, lookup_member_prefix,
                    time_header, timestamp_format, query.start_date, query.end_date,
                    zarr_path, bif_member_prefix, synthesis_messages)

                if data_xarray is None:
                    msg = f'No data was processed for site {site_id}. Skipping.'
                    logger.warning(msg)
                    synthesis_messages.append(msg)
                    continue

                utc_offset_num = None
                if utc_offset is not None:
                    try:
                        utc_offset_num = float(utc_offset)
                    except ValueError:
                        msg = f'{site_id}: BIF utc_offset value {utc_offset} was not converted to int.'
                        logger.warning(msg)
                        synthesis_messages.append(msg)

                available_variables = set(data_xarray.data_vars) & set(var_info_lookup)

                # Loop thru the observed properties.
                for data_var in query.observed_property:
                    variable_names = _get_variable_names(data_var, available_variables)
                    if not variable_names:
                        msg = f'No data variable found for {data_var} at site {site_id}. Skipping.'
                        logger.info(msg)
                        synthesis_messages.append(msg)
                        continue

                    for variable_name in variable_names:
                        var_info = var_info_lookup.get(variable_name, [])

                        heights_depths: List[Tuple[float, str | None]] | None = (
                            _get_height_depth_changes(var_info))
                        if not heights_depths:
                            heights_depths = None

                        monitoring_feature = _load_mf_object(
                            self, metadata_lookup[site_id], [data_var], heights_depths)
                        if monitoring_feature is None:
                            continue

                        # create the ResultTVP considering:
                        #    if the units need to be converted; if there is not conversion available, use the amf unit and log a warning message.
                        #    any filtering by result quality
                        #    deal with the timestamp, convert to iso -- for HH aggregation duration use timestamp start

                        amf_unit = var_info[0].get('VAR_INFO_UNIT', None)  # just use the last unit; they are the same among entries
                        unit_conv: int | float = 1
                        unit_str: str | None = None
                        if amf_unit is not None:
                            unit_conv, unit_str = self._get_unit_conv(amf_unit, data_var, synthesis_messages)

                        qc_variable_name = f'{variable_name}_QC'
                        # result quality is only supported for half-hourly resolution
                        has_qc_values = file_resolution == 'HH' and qc_variable_name in data_xarray.data_vars
                        quality_values = data_xarray[qc_variable_name].values if has_qc_values else None
                        data_values = data_xarray[variable_name].values

                        if (quality_values is not None and
                                len(quality_values) != len(data_values)):
                            msg = (f'{site_id} variable {variable_name} is not the same length as its corresponding '
                                   f'{qc_variable_name}. Skipping.')
                            logger.warning(msg)
                            synthesis_messages.append(msg)
                            continue

                        if query.result_quality and not has_qc_values:
                            msg = (f'{site_id} variable {variable_name} did not have quality control information '
                                   'to filter on. Returning all values.')
                            logger.info(msg)
                            synthesis_messages.append(msg)

                        timestamps = data_xarray['time'].values
                        result_tvp, result_quality, filtered_count = _build_tvp_results(
                            timestamps, data_values, unit_conv, file_resolution, utc_offset,
                            quality_values, query.result_quality)

                        if filtered_count:
                            msg = (f'Filtered {filtered_count} values for {site_id} variable {variable_name} '
                                   'based on the result quality query.')
                            logger.info(msg)
                            synthesis_messages.append(msg)

                        if not result_tvp:
                            continue

                        #    create the MeasurementTVPObservation and yield it.
                        observation = MeasurementTimeseriesTVPObservation(
                            self,
                            id=f'{self.datasource.id_prefix}--{site_id}--{variable_name}',
                            unit_of_measurement=unit_str,
                            feature_of_interest_type=FeatureTypeEnum.POINT,
                            feature_of_interest=monitoring_feature,
                            result=ResultListTVP(
                                plugin_access=self,
                                value=result_tvp,
                                result_quality=result_quality if has_qc_values else []),
                            observed_property=data_var,
                            result_quality=list(set(result_quality)) if has_qc_values else [],
                            aggregation_duration=query.aggregation_duration[0],
                            statistic='average',
                            utc_offset=utc_offset_num,
                        )

                        if file_resolution == 'HH':
                            observation.time_reference_position = TimeMetadataMixin.TIME_REFERENCE_START

                        yield observation

            finally:
                if data_xarray is not None:
                    data_xarray.close()
                    del data_xarray
                if var_info_lookup is not None:
                    del var_info_lookup
                if zarr_path is not None and zarr_path.exists():
                    shutil.rmtree(zarr_path)

        return StopIteration(synthesis_messages)


@basin3d_plugin
class AMFDataSourcePlugin(DataSourcePluginPoint):
    """AmeriFlux Data Source Plugin mockup."""

    title = 'AmeriFlux Data Source Plugin'
    plugin_access_classes = (AMFMonitoringFeatureAccess, AMFMeasurementTimeseriesTVPObservationAccess)
    feature_types = ['POINT']

    class DataSourceMeta:
        """Metadata used to construct the AmeriFlux BASIN-3D data source."""

        id = 'AMF'
        location = 'https://amfcdn.lbl.gov/api/v2'
        id_prefix = 'AMF'
        name = 'AmeriFlux'
