"""AmeriFlux data source plugin mockup.

This module defines the BASIN-3D plugin shape for AmeriFlux. Data retrieval
will be implemented in a subsequent change.
"""
from dataclasses import dataclass
from typing import Dict, List, Set

from basin3d.core import monitor

from basin3d.core.access import get_url
from basin3d.core.models import (AbsoluteCoordinate, AltitudeCoordinate, Coordinate, GeographicCoordinate, HorizontalCoordinate,
                                 MeasurementTimeseriesTVPObservation, MonitoringFeature)
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin, separate_list_types
from basin3d.core.schema.enum import FeatureTypeEnum, MappedAttributeEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.types import SpatialSamplingShapes


logger = monitor.get_logger(__name__)


@dataclass
class _SiteMetadata:
    site_id: str
    site_name: str
    description: str
    latitude: float
    longitude: float
    elevation: float
    start_year: str
    end_year: str
    igbp: str
    url: str


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
        msg = f'AMF metadata information was not in expected format.'
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

        if not latitude or not longitude:
            continue

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


def _inside_bbox(bbox: tuple, lat: float, lon: float) -> bool:
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
    return any(_inside_bbox(bbox, site_info.latitude, site_info.longitude)
               for bbox in bounding_boxes)


def _load_mf_object(datasource: DataSourcePluginAccess, site_info: _SiteMetadata, observed_properties: List) -> MonitoringFeature | None:
    """

    :param datasource:
    :param site_info:
    :param observed_properties:
    :return:
    """

    coord = Coordinate(
            absolute=AbsoluteCoordinate(
                horizontal_position=GeographicCoordinate(
                    **{"latitude": site_info.latitude,
                       "longitude": site_info.longitude,
                       "datum": HorizontalCoordinate.DATUM_WGS84,
                       "units": GeographicCoordinate.UNITS_DEC_DEGREES})))

    if site_info.elevation is not None:
        coord.absolute.vertical_extent = AltitudeCoordinate(**{"type": AltitudeCoordinate.TYPE_ALTITUDE,
                                                               "value": site_info.elevation})

    mf = MonitoringFeature(
        datasource,
        id=site_info.site_id,
        name=site_info.site_name,
        feature_type=FeatureTypeEnum.POINT,
        shape=SpatialSamplingShapes.SHAPE_POINT,
        description=site_info.description,
        observed_properties=observed_properties,
        coordinates=coord,
    )

    return mf


class AMFMonitoringFeatureAccess(DataSourcePluginAccess):
    """Placeholder access for AmeriFlux monitoring features."""

    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature):
        """Return no monitoring features until AmeriFlux retrieval is implemented."""

        synthesis_messages: List[str] = []

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
            observed_props = list(self.get_attribute_mappings(attr_type=MappedAttributeEnum.OBSERVED_PROPERTY))

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
            msg = f'{feature_type} is not supported.'
            logger.warning(msg)
            synthesis_messages.append(msg)

        return StopIteration(synthesis_messages)


class AMFMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """Placeholder access for AmeriFlux measurement time series."""

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """Return no observations until AmeriFlux retrieval is implemented."""

        synthesis_messages: List[str] = [
            'AmeriFlux measurement time series retrieval is not implemented.'
        ]
        logger.info(synthesis_messages[0])
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
