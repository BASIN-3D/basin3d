"""

.. currentmodule:: basin3d.plugins.arm

:synopsis: Atmospheric Radiation Measurement (ARM) Plugin Definition

This module provides the initial ARM Data Source plugin scaffold. ARM API and
NetCDF data access will be implemented in a subsequent change.
"""

# import netCDF4 as nc
import os
import requests
import tempfile
import xarray as xr

from pathlib import Path
from typing import Dict, List, Set, Tuple

from basin3d.core.access import get_url
from basin3d.core import monitor
from basin3d.core.models import (AbsoluteCoordinate, Coordinate, GeographicCoordinate, HorizontalCoordinate,
                                 MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature)
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin, separate_list_types
from basin3d.core.schema.enum import FeatureTypeEnum, SpatialSamplingShapes
from basin3d.core.schema.query import QueryBase, QueryMeasurementTimeseriesTVP, QueryMonitoringFeature

logger = monitor.get_logger(__name__)


NOT_PROVIDED = 'not provided'

ARM_NAME = os.environ.get('ARM_USER_NAME', None)
ARM_TOKEN = os.environ.get('ARM_USER_TOKEN', None)


def _fetch_timeseries(url, variables, synthesis_messages):
    """Download one NetCDF file and return the selected data as an xarray.Dataset.

    Failed requests are logged and added to ``synthesis_messages``.  ``None``
    is returned for a failed request so callers can continue with later URLs.
    """
    response = None

    try:
        response = requests.get(url, stream=True)
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
                # Load before the temporary file is deleted.
                return ds[variables].load()
    finally:
        if response is not None:
            response.close()


def _collect_to_zarr(urls, variables, zarr_path, synthesis_messages):
    """Fetch multiple NetCDF files and append them along the time dimension."""
    zarr_path = Path(zarr_path)
    first_file = True

    for url in urls:
        part = _fetch_timeseries(url, variables, synthesis_messages)

        if part is None:
            continue

        try:
            if first_file:
                # Creates the Zarr store.
                part.to_zarr(
                    zarr_path,
                    mode='w',
                    consolidated=False,
                )
                first_file = False
            else:
                # Appends this dataset along the existing time dimension.
                part.to_zarr(
                    zarr_path,
                    mode='a',
                    append_dim='time',
                    consolidated=False,
                )
        finally:
            part.close()

    if first_file:
        message = 'No ARM timeseries datasets were successfully retrieved.'
        logger.warning(message)
        synthesis_messages.append(message)
        return None

    return xr.open_zarr(
        zarr_path,
        consolidated=False,
    )


def _get_arm_metadata(arm_url: str, bbox: tuple | None, synthesis_messages: List):
    """

    :param arm_url:
    :param bbox:
    :param synthesis_messages:
    :return:
    """
    results = []
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
            msg = f'ARM metadata request for {request_url} returned error code {status_code}{detail}.'
            logger.error(msg)
            synthesis_messages.append(msg)
            return results

        return response.json()
    except Exception as e:
        msg = f'ARM metadata request for {request_url} failed: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        return results


def _parse_arm_metadata(metadata_results: List, mf_lookup: Dict, synthesis_messages: List):
    """

    :param metadata_results:
    :param mf_lookup:
    :param synthesis_messages:
    :return:
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

            required_values = {
                'containedInPlace.name': parent_name,
                'spatialCoverage.name': feature_name,
                'geo.latitude': latitude,
                'geo.longitude': longitude,
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
            'variables': var_list,
        }


def _load_mf_object(datasource: DataSourcePluginAccess, mf_id: str, mf_info: Dict) -> MonitoringFeature | None:
    """

    :param datasource:
    :param mf_id:
    :param mf_info:
    :return:
    """

    related_sampling_feature = RelatedSamplingFeature(
        datasource,
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
        related_sampling_feature_complex = [related_sampling_feature],
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
    """

    :param arm_metb1_url:
    :param query_monitoring_feature:
    :param synthesis_messages:
    :return:
    """
    mf_lookup = {}

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
            mf_set.add(mf_id)

    return mf_set, mf_lookup


class ARMMonitoringFeatureAccess(DataSourcePluginAccess):
    """Placeholder access for ARM monitoring features."""

    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature):
        """List ARM monitoring features.

        ARM monitoring-feature retrieval is not implemented yet.
        """

        synthesis_messages: List[str] = []

        # if parent feature is specified and not in the supported types, return nothing.
        if query.feature_type and query.feature_type not in ARMDataSourcePlugin.feature_types:
            msg = (f'{self.datasource.id_prefix} does not specified feature type: {query.feature_type}. '
                   f'Only feature types {ARMDataSourcePlugin.feature_types} are supported.')
            logger.warning(msg)
            synthesis_messages = [msg]
            return StopIteration(synthesis_messages)

        # the current number of data products is 65 (and has been for the past 1+ years).
        arm_metb1_url = f'{self.datasource.location}/metadata/data_product?data_product=met&page_from=0&page_size=100'

        mf_lookup = {}

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
            yield mf_obj

        return StopIteration(synthesis_messages)


class ARMMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """Placeholder access for ARM measurement observations."""

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """List ARM measurement timeseries TVP observations.

        ARM observation retrieval is not implemented yet.
        """
        synthesis_messages: list = []
        if not query.monitoring_feature:
            msg = f'No monitoring features for USGS were specified or they were not specified with the {self.datasource.id_prefix} prefix.'
            logger.warning(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        arm_metb1_url = f'{self.datasource.location}/metadata/data_product?data_product=met&page_from=0&page_size=100'

        mf_set, mf_lookup = _get_selected_arm_metadata(arm_metb1_url, query.monitoring_feature, synthesis_messages)

        query_date_str = f'&start={query.start_date}'
        if query.end_date:
            query_date_str += f'&end={query.end_date}'

        arm_data_url = (f'https://adc.arm.gov/armlive/query?user={ARM_NAME}:{ARM_TOKEN}'
                        '&ds={}'
                        f'{query_date_str}&wt=json')

        query_observed_properties = query.observed_property
        for observed_property in query.observed_property:
            query_observed_properties.append(f'qc_{observed_property}')

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

        return StopIteration(synthesis_messages)


@basin3d_plugin
class ARMDataSourcePlugin(DataSourcePluginPoint):
    """Atmospheric Radiation Measurement Data Source plugin scaffold."""

    title = 'Atmospheric Radiation Measurement Data Source Plugin'
    plugin_access_classes = (ARMMonitoringFeatureAccess, ARMMeasurementTimeseriesTVPObservationAccess)

    feature_types = ['POINT']

    class DataSourceMeta:
        """Metadata used to construct the ARM BASIN-3D data source."""

        id = 'ARM'
        location = 'https://metadata-api.svcs.arm.gov'
        id_prefix = 'ARM'
        name = 'Atmospheric Radiation Measurement'
