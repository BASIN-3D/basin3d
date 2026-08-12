"""

.. currentmodule:: basin3d.plugins.usgs

:platform: Unix, Mac
:synopsis: USGS Daily Values and Instantaneous Values Plugin Definition and supporting views.
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle S Christianson <dschristianson@lbl.gov>


* :class:`USGSDataSourcePlugin` - This Data Source plugin maps the USGS Daily Values and Instantaneous Values Service to BASIN-3D Models

USGS to BASIN-3D Mapping
++++++++++++++++++++++++
The table below describes how BASIN-3D synthesis models are mapped to the USGS Daily Values and Instantaneous Service models.

=================== === ==================================================================================================
USGS NWIS               BASIN-3D
=================== === ==================================================================================================
``nwis/dv``         >>  :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` with aggregation_duration == DAY
------------------- --- --------------------------------------------------------------------------------------------------
``nwis/iv``         >>  :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` with aggregation_duration == NONE
------------------- --- --------------------------------------------------------------------------------------------------
``nwis/sites``      >>  :class:`basin3d.synthesis.models.field.MonitoringFeature`
------------------- --- --------------------------------------------------------------------------------------------------
``nwis/huc``        >>  :class:`basin3d.core.models.MonitoringFeature`
------------------- --- --------------------------------------------------------------------------------------------------
``new_huc_rdb.txt`` >>  :class:`basin3d.synthesis.models.field.MonitoringFeature`
                         * Region (2-digit HUC code) to Region
                         * Subregion (4-digit HUC code) to Subregion
                         * Accounting (6-digit HUC code) Unit to Basin
                         * Watershed (8-digit HUC code) to Subbasin
=================== === ==================================================================================================


Access Classes
++++++++++++++

The following are the access classes that map *USGS Water Data* to the *BASIN-3D Models*.

* :class:`USGSMeasurementTimeseriesTVPObservationAccess` - Access for accessing a group of data points grouped by time, space, model, sample  etc.
* :class:`USGSMonitoringFeatureAccess` - Access for accessing monitoring features



---------------------
"""
import os
import requests

from datetime import date, timedelta
from datetime import datetime as dt
from math import ceil
from typing import Dict, List, Optional, Set, cast

from basin3d.core import monitor

from basin3d.core.connection import HTTPConnectionApiKey
from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, GeographicCoordinate, \
    MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature, \
    TimeMetadataMixin, TimeValuePair, ResultListTVP, HorizontalCoordinate
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin, separate_list_types
from basin3d.core.schema.enum import FeatureTypeEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.types import SpatialSamplingShapes

logger = monitor.get_logger(__name__)


# https://api.waterdata.usgs.gov/ogcapi/v0/openapi#/hydrologic-unit-codes/getHydrologic-unit-codesFeatures
# Dictionary of the supported USGS feature types, their huc classification code, and the total digits
HUC_CLASS_CODE = {
    'REGION': ('R', 2, FeatureTypeEnum.REGION),
    'SUBREGION': ('S', 4, FeatureTypeEnum.SUBREGION),
    'BASIN': ('B', 6, FeatureTypeEnum.BASIN),
    'SUBBASIN': ('U', 8, FeatureTypeEnum.SUBBASIN),
    'POINT': ('X', 12, FeatureTypeEnum.POINT)
}

NOT_PROVIDED = 'not provided'

PAGE_REQUEST_LIMIT_DEFAULT = 10

API_KEY = os.environ.get('USGS_API_KEY', None)
PAGE_REQUEST_LIMIT_STR = os.environ.get('USGS_PAGE_REQUEST_LIMIT', str(PAGE_REQUEST_LIMIT_DEFAULT))


try:
    PAGE_REQUEST_LIMIT = int(PAGE_REQUEST_LIMIT_STR)
except ValueError:
    PAGE_REQUEST_LIMIT = PAGE_REQUEST_LIMIT_DEFAULT


# Functions for Monitoring Feature
def _get_huc_lookup(datasource_location, http_connection: HTTPConnectionApiKey,
                    huc_filter: str, synthesis_messages: list) -> Dict:
    """
    Get the huc information and create a lookup

    :param http_connection:
    :param huc_filter:
    :param synthesis_messages:
    :return:
    """

    # Make subbasins = ~2500. Set limit at 10000 (max is 50000)
    huc_url = (f'{datasource_location}'
               '/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000'
               '&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0'
               f'&{huc_filter}')

    huc_info_lookup = {}  # type: ignore[var-annotated]

    huc_info_response, _ = _get_usgs_results(http_connection, huc_url, synthesis_messages)

    if huc_info_response:

        for huc in huc_info_response:
            huc_id = huc.get('id')
            huc_properties = huc.get('properties', {})
            huc_name = huc_properties.get('hydrologic_unit_name', 'not provided')
            huc_info_lookup.update({huc_id: huc_name})

    return huc_info_lookup


def _get_monitoring_location_observed_properties(
        datasource_location: str, http_connection: HTTPConnectionApiKey,
        tsm_filters: list, synthesis_messages: list,
        request_page_limit: int = PAGE_REQUEST_LIMIT) -> Dict:
    """

    :param tsm_filters:
    :param http_connection:
    :param synthesis_messages:
    :return:
    """
    tsm_url_base = (f'{datasource_location}'
                    '/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000'
                    '&properties=id,parameter_code,monitoring_location_id'
                    '&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&{}')

    observed_property_lookup = {}  # type: ignore[var-annotated]

    for tsm_filter in tsm_filters:

        tsm_url = tsm_url_base.format(tsm_filter)

        tsm_result, has_pagination_error = _get_usgs_results(http_connection, tsm_url, synthesis_messages,
                                                             request_page_limit=request_page_limit, ignore_error_msg=True)

        if has_pagination_error:
            msg = (f'A pagination error occurred while attempting to retrieve the timeseries information for {tsm_url}. '
                   f'Observed properties for the returned monitoring features will be missing. See instructions for how to increase the request page limit.')
            logger.warning(msg)
            synthesis_messages.append(msg)
            return {}

        # make lookup
        for tsm in tsm_result:
            tsm_properties = tsm.get('properties', {})
            ml_id = tsm_properties.get('monitoring_location_id')
            parameter_code = tsm_properties.get('parameter_code')

            observed_property_lookup.setdefault(ml_id, set()).add(parameter_code)

    return observed_property_lookup


def _get_unique_sites(site_responses: list, unique_usgs_sites: Dict):
    """

    :param site_responses:
    :param unique_usgs_sites:
    :return:
    """
    for m_loc in site_responses:
        site = m_loc.get('id')

        if site not in unique_usgs_sites:
            unique_usgs_sites[site] = m_loc


def _load_huc_obj(datasource: DataSourcePluginAccess, huc_id: str,
                  huc_name: str, feature_type: FeatureTypeEnum | None | str, synthesis_messages: list):
    """
    Transform USGS huc information to a :class:`~basin3d.core.models.MonitoringFeature` object

    :param huc_id: huc identifier
    :param huc_name: name of the huc
    :param feature_type: :class: `~basin3d.core.schema.enum.FeatureTypeEnum` feature type to be returned
    :return: a serialized :class:`~basin3d.core.models.MonitoringFeature` object
    """
    if not feature_type:
        for huc_class_code in HUC_CLASS_CODE.values():
            if huc_class_code[1] == len(huc_id):
                feature_type = huc_class_code[2]
                break

    if not feature_type:
        # Should not happen
        msg = f'Feature type could not be determined for huc id {huc_id}: {huc_name}'
        logger.warning(msg)
        synthesis_messages.append(msg)
        return None

    description = f'{feature_type}: {huc_name}'

    result = MonitoringFeature(
        datasource, id=huc_id,
        name=huc_name,
        description=description,
        feature_type=feature_type,
        shape=SpatialSamplingShapes.SHAPE_SURFACE,
        coordinates=None,
        observed_properties=None)

    if feature_type is not FeatureTypeEnum.REGION:

        parent_huc_code = None
        for huc_class_code in HUC_CLASS_CODE.values():
            if huc_class_code[1] == len(huc_id) - 2:
                parent_huc_code = huc_class_code[2]
                break

        if parent_huc_code is not None:
            result.related_sampling_feature_complex = [RelatedSamplingFeature(
                datasource,
                related_sampling_feature=huc_id[:-2],
                related_sampling_feature_type=parent_huc_code,
                role=RelatedSamplingFeature.ROLE_PARENT
            )]

    return result


# Functions for both Monitoring Feature and Measurement TVP Observation classes
def _convert_tuple_to_str(a_tuple: tuple, synthesis_msg=[]) -> Optional[str]:
    """Helper function to convert a tuple of float to a str
    :param a_tuple: tuple
    """
    output: Optional[str] = None
    msg: Optional[str] = None
    try:
        tuple_str_list = [str(v) for v in a_tuple]
        output = ','.join(tuple_str_list)
    except TypeError:
        msg = f'Could not convert {a_tuple} to str'
    except Exception as e:
        msg = f'Some other error {e} while trying to convert {a_tuple} to str'

    if msg:
        logger.warning(msg)
        synthesis_msg.append(msg)

    return output


def _get_usgs_results(http_connection: HTTPConnectionApiKey, initial_url: str, synthesis_messages: list,
                      results: list | None = None, request_page_limit: int = PAGE_REQUEST_LIMIT, ignore_error_msg: bool = False) -> tuple[list, bool]:
    """
    Function to send requests to USGS endpoints and deal with pagination using the next link in the response.
    Pagination is capped

    :param http_connection: connection class
    :param initial_url: initial url to request
    :param results: results list to which additional results will be added;
                    **USE with CAUTION since errors clear the list**
    :param request_page_limit: limit to for the pagination
    :param ignore_error_msg: whether to ignore the errors re: user configurable page request limit.
    :return: tuple containing the results and whether the pagination failed
    """

    if results is None:
        results = []

    if not ignore_error_msg and request_page_limit < 0:
        msg = ('Page request limit cannot be negative. No results are returned. '
               'See instructions for how to set a custom request page limit.')
        logger.error(msg)
        synthesis_messages.append(msg)
        results.clear()
        return results, True

    url: str | None = initial_url

    count = 0
    while url is not None and count < request_page_limit:

        usgs_response = http_connection.get(url)

        if usgs_response is not None and usgs_response.status_code == 200:
            url = _parse_usgs_response(usgs_response, results, synthesis_messages, initial_url)
        else:
            partial_result_msg = ''
            is_pagination_error = False
            if results:
                partial_result_msg = f' This url is part of the pagination for the initial request {initial_url}. No results are returned.'
                is_pagination_error = True
                # Ignore any results that were returned if the full pagination fails.
                results.clear()
            msg = f'Problem with request to {url}.{partial_result_msg}'
            if usgs_response is not None:
                msg = f'{msg}; {usgs_response.status_code}: {usgs_response.json}'
            logger.error(msg)
            synthesis_messages.append(msg)
            return results, is_pagination_error

        count += 1

    # the case where the count expired before pagination completed
    if count == request_page_limit and url:
        if not ignore_error_msg:
            msg = ('Pagination exceeded the default basin3d request limit: '
                   f'{str(request_page_limit)} for initial request {initial_url}. '
                   'No results will be returned. See instructions for increasing the limit.')
            logger.error(msg)
            synthesis_messages.append(msg)
        results.clear()
        return results, True

    return results, False


def _parse_usgs_response(usgs_response: requests.Response, response_result: list,
                         synthesis_messages: list, initial_url: str) -> str | None:
    """

    :param usgs_response:
    :param response_result:
    :return: the next url if the response requires pagination, otherwise None
    """

    result_json = usgs_response.json()
    result_features = result_json.get('features', [])

    response_result.extend(result_features)

    response_links = result_json.get('links', [])

    next_links = [link for link in response_links if link.get('rel') == 'next']

    if len(next_links) > 1:
        msg = f'Request response contains multiple next links; Cannot continue pagination for initial request {initial_url}'
        logger.error(msg)
        synthesis_messages.append(msg)
        response_result.clear()  # clear out any results
        return None

    if not next_links:
        return None

    next_link = next_links[0]
    next_url = cast(str | None, next_link.get('href'))

    if not next_url:
        msg = ('Request response contains a next link without a valid href; '
               f'Cannot continue pagination for initial request {initial_url}')
        logger.error(msg)
        synthesis_messages.append(msg)
        response_result.clear()  # clear out any results
        return None

    return next_url


def _load_point_obj(datasource: DataSourcePluginAccess, json_obj: Dict, observed_property_variables: dict, synthesis_messages: list):
    """
    Instantiate the object

    Example monitoring locations response - feature element
    {
        "type":"Feature",
        "properties":
            {
                "id":"USGS-09110000",
                "agency_code":"USGS",
                "monitoring_location_name":"TAYLOR RIVER AT ALMONT, CO.",
                "site_type":"Stream",
                "hydrologic_unit_code":"140200010113",
                "altitude":8016.2,
                "altitude_accuracy":0.13,
                "vertical_datum":"NAVD88",
                "time_zone_abbreviation":"MST",
                "uses_daylight_savings":"Y",
                "revision_note":"WSP 1213: 1911. WDR-US-2011: Drainage area."},
        "id":"USGS-09110000",
        "geometry":{
            "type":"Point",
            "coordinates":[-106.844722, 38.664444]
        }
    }

    :param json_obj:
    :param observed_property_variables: dict of locations with a list of their available variables
    :param synthesis_messages
    :return:
    """

    usgs_ml_id = json_obj['id']
    usgs_ml_code = usgs_ml_id.removeprefix(f'{datasource.datasource.id_prefix}-')

    lat, lon = None, None
    try:
        ml_geometry = json_obj.get('geometry', {})
        ml_coord = ml_geometry.get('coordinates')
        lat, lon = float(ml_coord[1]), float(ml_coord[0])
    except Exception as e:
        synthesis_messages.append(f"Error getting latlon: {str(e)}")
        logger.error(str(e))

    mf_opv = list(observed_property_variables.get(usgs_ml_id, []))
    if not mf_opv:
        msg = f"Could not find time series observed property variables for this monitoring feature: {usgs_ml_id}"
        synthesis_messages.append(msg)
        logger.warning({msg})

    ml_properties = json_obj.get('properties', {})
    hydrologic_unit_code = ml_properties.get('hydrologic_unit_code', NOT_PROVIDED)

    desc = (f'site type: {ml_properties.get("site_type")}; '
            f'hydrologic_unit_code: {hydrologic_unit_code}')

    related_sampling_feature = []
    if hydrologic_unit_code != NOT_PROVIDED:
        parent_huc = hydrologic_unit_code[0:8]
        related_sampling_feature = [
            RelatedSamplingFeature(
                datasource,
                related_sampling_feature=parent_huc,
                related_sampling_feature_type=FeatureTypeEnum.SUBBASIN,  # previously site
                role=RelatedSamplingFeature.ROLE_PARENT)]

    monitoring_feature = MonitoringFeature(
        datasource,
        id=usgs_ml_code,
        name=ml_properties.get('monitoring_location_name'),
        feature_type=FeatureTypeEnum.POINT,
        shape=SpatialSamplingShapes.SHAPE_POINT,
        description=desc,
        related_sampling_feature_complex=related_sampling_feature,
        observed_properties=mf_opv,
        coordinates=Coordinate(
            absolute=AbsoluteCoordinate(
                horizontal_position=GeographicCoordinate(
                    **{"latitude": lat,
                       "longitude": lon,
                       # from api documentation: "Coordinates are published in EPSG:4326 / WGS84 / World Geodetic System 1984"
                       # https://api.waterdata.usgs.gov/ogcapi/v0/openapi#/monitoring-locations/getMonitoring-locationsFeatures
                       "datum": HorizontalCoordinate.DATUM_WGS84,
                       "units": GeographicCoordinate.UNITS_DEC_DEGREES}))
        )
    )
    # if json_obj['alt_va'] and json_obj['alt_acy_va'] and json_obj['alt_datum_cd']:
    if ml_properties.get('altitude') and ml_properties.get('vertical_datum'):
        alt_info = {'value': float(ml_properties.get('altitude')),
                    'datum': ml_properties.get('vertical_datum')}
        alt_res = ml_properties.get('altitude_accuracy')
        if alt_res:
            alt_info.update({'resolution': float(alt_res)})
        monitoring_feature.coordinates.absolute.vertical_extent = [AltitudeCoordinate(**alt_info)]

    return monitoring_feature


# Functions for Measurement TVP Observation classes
def _convert_discharge(data, data_str, parameter, units):
    """
    Convert the River Discharge to m^3
    :param data:
    :param data_str:
    :param parameter:
    :param units:
    :return:
    """
    if parameter in ['00060', '00061']:
        # Hardcode conversion from ft^3 to m^3
        # for River discharge
        if data_str == '-999999':
            data = int(data_str)
        else:
            data *= 0.028316847
        units = "m^3/s"
    return data, units


def _tsm_query_filter(tsm_properties: Dict, query: QueryMeasurementTimeseriesTVP, ignore_stats: bool) -> bool:
    """
    Return True when the TSM satisfies the query criteria.

    Date ranges are inclusive. A TSM matches when at least some portion
    of its date range overlaps the requested query date range.

    :param tsm_properties:
    :param query:
    :return:
    """

    # required query arguments
    if tsm_properties['parameter_code'] not in query.observed_property:
        return False

    if not tsm_properties['end']:
        return False

    try:
        tsm_end_dt = dt.fromisoformat(tsm_properties['end']).date()
    except ValueError:
        return False

    if tsm_end_dt < query.start_date:
        return False

    # optional query arguments
    if query.end_date:
        if not tsm_properties['begin']:
            return False

        try:
            tsm_begin_dt = dt.fromisoformat(tsm_properties['begin']).date()
        except ValueError:
            return False
        if query.end_date < tsm_begin_dt:
            return False

    if not ignore_stats and query.statistic and tsm_properties['statistic_id'] not in query.statistic:
        return False

    return True


def _filter_timeseries_metadata(tsm_results: List, query: QueryMeasurementTimeseriesTVP,
                                observed_properties: Dict, unique_timeseries: Dict, unique_sites: Set,
                                ignore_stats: bool):
    """

    :param tsm_results:
    :param query:
    :param observed_properties:
    :param unique_timeseries:
    :param unique_sites:
    :return:
    """
    for tsm_obj in tsm_results:
        tsm = tsm_obj['properties']

        tsm_parameter_code = tsm['parameter_code']
        tsm_monitoring_location_id = tsm['monitoring_location_id']
        observed_properties.setdefault(tsm_monitoring_location_id, set()).add(tsm_parameter_code)

        if not _tsm_query_filter(tsm, query, ignore_stats):
            continue

        unique_timeseries.update({tsm['id']: tsm})
        unique_sites.add(tsm_monitoring_location_id)


def _calculate_date_ranges(start_date: date, end_date: date,
                           mode: str, total_data_object_limit: int = 49900) -> list[tuple[date, date, int]]:
    """
    Split an inclusive date range into the minimum number of sequential,
    non-overlapping ranges without exceeding the data object limit.

    Each tuple is: (start_date, end_date, expected_data_object_count)

    Object rates:
        daily       -> 1 object per day
        continuous  -> 96 objects per day

    Dates are kept intact; a day's data objects are never split between two ranges.

    The default limit is ~1 day less than the max USGS return limit to account for fuzziness
        in not specifying and end date in the basin3d query.
    ChatGPT helped write this function.
    """

    if end_date < start_date:
        raise ValueError('end_date must be on or after start_date')

    if total_data_object_limit <= 0:
        raise ValueError('total_data_object_limit must be greater than zero')

    objects_per_day = {'daily': 1, 'continuous': 96, }.get(mode)

    if objects_per_day is None:
        raise ValueError("mode must be 'daily' or 'continuous'")

    max_days_per_range = total_data_object_limit // objects_per_day

    if max_days_per_range == 0:
        raise ValueError(
            f'The limit of {total_data_object_limit} cannot hold one '
            f'{mode} day ({objects_per_day} objects required).'
        )

    total_days = (end_date - start_date).days + 1

    number_of_ranges = ceil(total_days / max_days_per_range)

    ranges = []
    current_start = start_date

    for _ in range(number_of_ranges):
        remaining_days = (end_date - current_start).days + 1
        days_in_range = min(max_days_per_range, remaining_days)

        current_end = current_start + timedelta(days=days_in_range - 1)
        expected_count = days_in_range * objects_per_day

        ranges.append((current_start, current_end, expected_count))

        current_start = current_end + timedelta(days=1)

    return ranges


class USGSMonitoringFeatureAccess(DataSourcePluginAccess):
    """
    Access for mapping USGS HUC Units to :class:`~basin3d.core.models.MonitoringFeature` objects.

    ============== === =======================================================
    USGS HUC code      BASIN-3D
    ============== === =======================================================
    2-digit        >>  :class:`basin3d.core.schema.enum.FeatureType` REGION
    -------------- --- -------------------------------------------------------
    4-digit        >>  :class:`basin3d.core.schema.enum.FeatureType` SUBREGION
    -------------- --- -------------------------------------------------------
    6-digit        >>  :class:`basin3d.core.schema.enum.FeatureType` BASIN
    -------------- --- -------------------------------------------------------
    8-digit        >>  :class:`basin3d.core.schema.enum.FeatureType` SUBBASIN
    ============== === =======================================================

    """
    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature):
        """
        List Monitoring Feature

        :param query: The query information object
        :return: a generator object that yields :class:`~basin3d.core.models.MonitoringFeature`
            objects
        """
        synthesis_messages: List[str] = []

        feature_type = isinstance(query.feature_type,
                                  FeatureTypeEnum) and query.feature_type.value or query.feature_type
        if feature_type in USGSDataSourcePlugin.feature_types or feature_type is None:

            # set up connection
            http_conn = HTTPConnectionApiKey(self.datasource, api_key=API_KEY, verify_ssl=True)

            # Convert parent_features
            parent_features = []
            parent_huc_codes = []
            if query.parent_feature:
                for value in query.parent_feature:
                    # if feature_type is specified, the parent features must be at least one hierarchical step above
                    if feature_type is not None:
                        feature_type_len = HUC_CLASS_CODE[feature_type][1]
                        if len(value) >= feature_type_len:
                            msg = (f'Specified parent feature {value} is not a huc that hierarchically above the '
                                   f'specified feature type {feature_type}. It will not be considered in the '
                                   f'query to USGS resources.')
                            logger.warning(msg)
                            synthesis_messages.append(msg)
                            continue
                    parent_features.append(value)
                parent_huc_codes = [huc_id.removeprefix(f'{self.datasource.id_prefix}-') for huc_id in parent_features]

            if query.parent_feature and not parent_features:
                msg = ('The specified parent features are not at least one step up in the huc hierarchy from the '
                       f'specified feature_type {feature_type}')
                logger.warning(msg)
                synthesis_messages.append(msg)
                return StopIteration(synthesis_messages)

            elif not feature_type or feature_type != FeatureTypeEnum.POINT:

                if query.monitoring_feature:
                    # split up mf query types
                    huc_types = separate_list_types(
                        query.monitoring_feature, {'named': str, 'bbox': tuple})
                    huc_named = huc_types.get('named', [])

                    if huc_types.get('bbox', []):
                        msg = ('USGS plugin only supports bounding box queries for feature_type = points. '
                               'Huc identifiers that match the specified monitoring_feature_ids will be returned.')
                        logger.warning(msg)
                        synthesis_messages.append(msg)

                    huc_ids = huc_named
                    mismatch_msg: str | None = None
                    if huc_named:
                        if feature_type:
                            feature_type_len = HUC_CLASS_CODE[feature_type][1]
                            huc_ids = [huc_identifier for huc_identifier in huc_named if len(huc_identifier) == feature_type_len]
                            if not huc_ids:
                                mismatch_msg = (f'Mismatch in specified huc name and feature type. Feature type {feature_type} '
                                                f'expects huc name with {feature_type_len} digits')

                    if huc_ids:
                        huc_list = ','.join(huc_ids)
                        huc_filter = f'id={huc_list}'
                        huc_info = _get_huc_lookup(self.datasource.location, http_conn, huc_filter, synthesis_messages)
                    else:
                        huc_info = {}
                        msg = ('No named monitoring features were specified; only bounding boxes. '
                               'Named monitoring features are required if no feature type is specified.')
                        if mismatch_msg:
                            msg = mismatch_msg
                        logger.warning(msg)
                        synthesis_messages.append(msg)

                elif feature_type:
                    huc_class = HUC_CLASS_CODE[feature_type][0]
                    huc_filter = f'hydrologic_unit_classification_code={huc_class}'
                    huc_info = _get_huc_lookup(self.datasource.location, http_conn, huc_filter, synthesis_messages)

                else:
                    huc_info = {}
                    min_length = 1

                    # if there is a parent feature specified, only get huc units that could be children
                    if parent_features:
                        parent_lengths = [len(parent_feat) for parent_feat in parent_huc_codes]
                        parent_length_min = min(parent_lengths)
                        min_length = max({min_length, parent_length_min})
                    huc_class_codes = [huc_code[0] for huc_code in HUC_CLASS_CODE.values() if huc_code[1] > min_length and huc_code[0] != 'X']
                    for huc_class in huc_class_codes:
                        huc_filter = f'hydrologic_unit_classification_code={huc_class}'
                        huc_info.update(_get_huc_lookup(self.datasource.location, http_conn, huc_filter, synthesis_messages))

                for huc_id, huc_name in huc_info.items():

                    if parent_huc_codes:
                        is_child_huc = False
                        # look thru each parent_huc_id to see if current huc_id is a child of one of them
                        for parent_huc_id in parent_huc_codes:
                            # for the case where feature_type is not specified and multiple parent features are,
                            #    there could be the case where one of specified parent huc ids is not hierarchically
                            #    one step above. If so, ignore this case and move onto the next.
                            if len(parent_huc_id) >= len(huc_id):
                                continue
                            if huc_id.startswith(parent_huc_id):
                                is_child_huc = True
                        if not is_child_huc:
                            continue

                    monitoring_feature = _load_huc_obj(
                        datasource=self,
                        huc_id=huc_id,
                        huc_name=huc_name,
                        feature_type=feature_type,
                        synthesis_messages=synthesis_messages)

                    yield monitoring_feature

            # no feature_type specified and/or feature_type == POINT
            elif feature_type == FeatureTypeEnum.POINT and (query.monitoring_feature or parent_features):
                # base_url = '{}site/?{}&seriesCatalogOutput=true&outputDataTypeCd=iv,dv&siteStatus=all&format=rdb'
                ml_url = (f'{self.datasource.location}'
                          '/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000'
                          '&properties=id,agency_code,monitoring_location_name,site_type,'
                          'hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,'
                          'uses_daylight_savings,revision_note'
                          '&skipGeometry=false&offset=0&agency_code=USGS&{}')

                # initiate variables for looping thru requests for multiple location filters
                unique_sites = {}  # type: ignore[var-annotated]
                loc_filters = []
                tsm_filters = []

                # Points by id: USGS calls these sites
                if query.monitoring_feature is not None:

                    # split up mf query types
                    mf_types = separate_list_types(query.monitoring_feature, {'named': str, 'bbox': tuple})
                    mf_named = mf_types.get('named', [])
                    mf_bbox = mf_types.get('bbox', [])

                    if mf_named:
                        usgs_sites = ','.join(f'{self.datasource.id_prefix}-{mf_usgs_id}' for mf_usgs_id in mf_named)
                        loc_filters.append(f'id={usgs_sites}')
                        tsm_filters.append(f'monitoring_location_id={usgs_sites}')

                    if mf_bbox:
                        bbox_coords = [_convert_tuple_to_str(bbox_tuple) for bbox_tuple in mf_bbox]
                        loc_filters.extend([f'bbox={bbox_cc}' for bbox_cc in bbox_coords])
                        tsm_filters.extend([f'bbox={bbox_cc}' for bbox_cc in bbox_coords])

                else:
                    # Point by parent feature:
                    parent_huc_ids = [huc_id.strip(f'{self.datasource.id_prefix}-') for huc_id in parent_features]
                    parent_huc_list = ','.join(parent_huc_ids)
                    loc_filters.append(f'hydrologic_unit_code={parent_huc_list}')
                    tsm_filters.append(f'hydrologic_unit_code={parent_huc_list}')

                # parent_hucs = None
                observed_property_ml_lookup = {}
                if loc_filters:
                    observed_property_ml_lookup = _get_monitoring_location_observed_properties(
                        self.datasource.location, http_conn, tsm_filters, synthesis_messages)

                has_pagination_error = False
                for a_filter in loc_filters:

                    url = ml_url.format(a_filter)
                    logger.debug(f"{self.__class__.__name__}.list url:{url}")

                    usgs_sites_response, has_pagination_error = _get_usgs_results(http_conn, url, synthesis_messages, request_page_limit=PAGE_REQUEST_LIMIT)

                    if has_pagination_error:
                        break

                    _get_unique_sites(usgs_sites_response, unique_sites)

                if not has_pagination_error and unique_sites:
                    for v in unique_sites.values():
                        yield _load_point_obj(datasource=self, json_obj=v,
                                              observed_property_variables=observed_property_ml_lookup,
                                              synthesis_messages=synthesis_messages)

            # point is specified but no monitoring feature or parent feature
            else:
                msg = 'Monitoring feature or parent feature must be specified for feature type POINT. Retry query.'
                logger.warning(msg)
                synthesis_messages.append(msg)

        else:
            synthesis_messages.append(f"Feature type {feature_type} not supported by {self.datasource.name}.")
            logger.warning(f"Feature type {feature_type} not supported by {self.datasource.name}.")

        return StopIteration(synthesis_messages)

    def get(self, query: QueryMonitoringFeature):
        """ Get a single Monitoring Feature object

        :param query: The query info object with id specified
        :return: a :class:`basin3d.core.models.MonitoringFeature` object
        """
        # query.id will always be a string at this point with validation upstream, thus ignoring the type checking

        monitoring_feature_len = len(query.id)  # type: ignore[arg-type]
        if not query.feature_type:
            if monitoring_feature_len == 2:
                query.feature_type = FeatureTypeEnum.REGION
            elif monitoring_feature_len == 4:
                query.feature_type = FeatureTypeEnum.SUBREGION
            elif monitoring_feature_len == 6:
                query.feature_type = FeatureTypeEnum.BASIN
            elif monitoring_feature_len == 8:
                query.feature_type = FeatureTypeEnum.SUBBASIN

        query.monitoring_feature = [query.id]  # type: ignore[list-item]

        for o in self.list(query=query):
            return o

        return None


class USGSMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """
    USGS Daily Values Service: https://waterservices.usgs.gov/docs/dv-service/

    USGS Instantaneous Values Service: https://waterservices.usgs.gov/docs/instantaneous-values/

    Access for mapping USGS water services daily or instantaneous value data to
    :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` objects.
    """

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """
        List of Measurement Timeseries TVP Observation objects for USGS Daily Values or Instantaneous Values

        :param query: :class:`basin3d.core.schema.query.QueryMeasurementTimeseriesTVP`
        :return: a generator object that yields :class:`~basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation` objects
        """
        synthesis_messages: list = []
        if not query.monitoring_feature:
            msg = f'No monitoring features for USGS were specified or they were not specified with the {self.datasource.id_prefix} prefix.'
            logger.warning(msg)
            synthesis_messages.append(msg)
            return StopIteration(synthesis_messages)

        tsm_computation_period_identifier = 'Daily'
        api_endpoint = 'daily'
        ignore_statistics = False
        if query.aggregation_duration[0] == 'NONE':
            tsm_computation_period_identifier = 'Points'
            api_endpoint = 'continuous'
            # if any statistic other than INSTANT is specified, ignore it and just return the continuous data
            if query.statistic and query.statistic != ['00011', ]:
                msg = ('USGS continuous data service only supports statistic INSTANT. The other statistics '
                       '(e.g., MEAN, MIN, MAX) cannot be specified when aggregation_duration = NONE and will be ignored.')
                logger.warning(msg)
                synthesis_messages.append(msg)
                ignore_statistics = True

        http_conn = HTTPConnectionApiKey(self.datasource, api_key=API_KEY, verify_ssl=True)

        tsm_filters = []

        # split up mf query types
        mf_types = separate_list_types(query.monitoring_feature, {'named': str, 'bbox': tuple})
        mf_named = mf_types.get('named', [])
        mf_bbox = mf_types.get('bbox', [])

        if mf_named:
            mf_named = sorted(mf_named)
            usgs_monitoring_locations = ','.join(f'{self.datasource.id_prefix}-{mf_usgs_id}'
                                                 for mf_usgs_id in mf_named if len(mf_usgs_id) > 7)
            if usgs_monitoring_locations:
                tsm_filter = f'monitoring_location_id={usgs_monitoring_locations}'
                tsm_filters.append(tsm_filter)

            huc_ids = [usgs_id for usgs_id in mf_named if len(usgs_id) < 7]
            if huc_ids:
                huc_ids_str = ','.join(huc_ids)
                tsm_filter = f'hydrologic_unit_code={huc_ids_str}'
                tsm_filters.append(tsm_filter)

        if mf_bbox:
            bbox_coords = [_convert_tuple_to_str(bbox_tuple) for bbox_tuple in mf_bbox]
            tsm_filters.extend([f'bbox={bbox_cc}' for bbox_cc in bbox_coords])

        tsm_url_base = (f'{self.datasource.location}'
                        '/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000'
                        '&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure'
                        '&skipGeometry=true&offset=0'
                        f'&computation_period_identifier={tsm_computation_period_identifier}'
                        '&{}')

        observed_properties = {}  # type: ignore[var-annotated]
        unique_sites = set()  # type: ignore[var-annotated]
        unique_timeseries = {}  # type: ignore[var-annotated]

        for a_filter in tsm_filters:

            url = tsm_url_base.format(a_filter)
            logger.debug(f"{self.__class__.__name__}.list url:{url}")

            usgs_site_response, has_pagination_error = _get_usgs_results(http_conn, url, synthesis_messages)

            if has_pagination_error:
                return StopIteration(synthesis_messages)

            if usgs_site_response:
                _filter_timeseries_metadata(usgs_site_response, query, observed_properties,
                                            unique_timeseries, unique_sites, ignore_statistics)

        if not unique_timeseries:
            msg = 'No timeseries could be found for the specified query arguments.'
            logger.warning(msg)
            synthesis_messages.append(msg)

        elif not unique_sites:
            # This should not happen
            msg = 'There were no monitoring features found in the time series metadata for the specified query parameters.'
            logger.warning(msg)
            synthesis_messages.append(msg)

        else:
            # ToDo: add utc_offset support once reconsider model attribute type
            # get time-zone-abbreviations
            """
            tz_lookup = {}
            tz_results = _get_usgs_results(
                http_conn,
                (f'/collections/time-zone-codes/items?f=json&lang=en-US&limit=1000'
                 '&properties=id,time_zone_utc_offset&skipGeometry=true&offset=0'),
                synthesis_messages)

            for tz_result in tz_results:
                tz_lookup[tz_result['id']] = tz_result['time_zone_utc_offset']
            """

            # get the monitoring feature info for all unique sites
            ml_ids = ','.join(sorted(unique_sites))
            ml_filter = f'id={ml_ids}'
            ml_url = (f'{self.datasource.location}'
                      '/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000'
                      '&properties=id,agency_code,monitoring_location_name,site_type,'
                      'hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,'
                      'uses_daylight_savings,revision_note'
                      f'&skipGeometry=false&offset=0&agency_code=USGS&{ml_filter}')

            ml_results, has_pagination_error = _get_usgs_results(http_conn, ml_url, synthesis_messages, request_page_limit=PAGE_REQUEST_LIMIT)

            if has_pagination_error:
                return StopIteration(synthesis_messages)

            ml_lookup = {}
            for ml_result in ml_results:
                ml_lookup.update({ml_result['id']: ml_result})

            approval_status_filter = ''
            if query.result_quality and len(query.result_quality) == 1:
                approval_status = query.result_quality[0]
                approval_status_filter = f'&approval_status={approval_status}'

            # Iterate over timeseries
            for ts_id, ts_metadata in unique_timeseries.items():

                # deal with time and splitting up the date range into acceptable chunks
                start_date = query.start_date
                ts_start = dt.fromisoformat(ts_metadata['begin']).date()
                ts_query_start = max(ts_start, start_date)

                end_date = date.today()
                if query.end_date:
                    end_date = query.end_date
                ts_end = dt.fromisoformat(ts_metadata['end']).date()
                ts_query_end = min(end_date, ts_end)

                return_limit = 50000  # 50000 is max limit for daily and continuous endpoints
                query_date_ranges = _calculate_date_ranges(ts_query_start, ts_query_end,
                                                           api_endpoint, return_limit - 100)

                # loop thru date ranges to get the full time series sorted by time
                #    This has to be done manually b/c sorting disables pagination
                ts_result: list = []

                has_pagination_error = False
                for idx, date_range in enumerate(query_date_ranges):
                    start_date_filter = date_range[0].strftime('%Y-%m-%d')
                    end_date_str = date_range[1].strftime('%Y-%m-%d')
                    end_date_filter = f'{end_date_str}T23%3A59%3A59Z'  # add time to last second of the day
                    if idx == len(query_date_ranges) - 1 and not query.end_date:
                        end_date_filter = '..'

                    url = (f'{self.datasource.location}'
                           f'/collections/{api_endpoint}/items?f=json&lang=en-US&limit={str(return_limit)}'
                           '&properties=time,value,unit_of_measure,approval_status'
                           '&skipGeometry=true&offset=0&agency_code=USGS'
                           '&sortby=time'  # sorting disables pagination
                           f'&time_series_id={ts_id}'
                           f'&time={start_date_filter}T00%3A00%3A00Z'
                           f'%2F{end_date_filter}'
                           f'{approval_status_filter}')

                    # an error in any of the calls for the given ts_id, will result in the entire timeseries
                    #    being removed from the output; this is handled in the _get_usgs_results function
                    #    Since ts_result is being passed in, it is modified in the function.
                    #    Because the request limit is hard coded here for basin-3d to handle the pagination,
                    #    the error messages about setting the pagination limit need to be ignored.
                    #    Instead, the frequency error message will be returned below.
                    _, has_pagination_error = _get_usgs_results(http_conn, url, synthesis_messages,
                                                                ts_result, request_page_limit=1, ignore_error_msg=True)

                    if has_pagination_error:
                        break

                if has_pagination_error:
                    # This should not happen as our understanding is that data collection for continuous is not more frequent than 15-minutes.
                    msg = f'Time series {ts_id} has data collection frequency higher than 15-min. Contact basin3d development team.'
                    logger.error(msg)
                    synthesis_messages.append(msg)
                    continue

                if ts_result:
                    # get ts metadata
                    unit_of_measurement = ts_metadata.get('unit_of_measure')
                    statistic_id = ts_metadata.get('statistic_id')
                    parameter_code = ts_metadata.get('parameter_code')

                    # get monitoring feature info
                    ml_id = ts_metadata.get('monitoring_location_id')
                    ml_info = ml_lookup[ml_id]
                    # timezone_offset = ml_info['properties']['time_zone_abbreviation']  # ToDo add later
                    monitoring_feature = _load_point_obj(
                        datasource=self, json_obj=ml_info,
                        observed_property_variables=observed_properties, synthesis_messages=synthesis_messages)

                    result_TVPs = []
                    result_TVP_quality = []
                    result_quality = set()

                    for data_obj in ts_result:
                        """
                        {
                            "type": "Feature",
                            "properties": {
                                "time": "2026-01-01",
                                "value": "127",
                                "unit_of_measure": "ft^3/s",
                                "approval_status": "Approved"
                            },
                            "id": "ec2b31fa-f303-43a9-93c8-0947c12d2ddc",
                            "geometry": null
                        },
                        """
                        data_properties = data_obj['properties']

                        # check for changing unit
                        if data_properties['unit_of_measure'] != unit_of_measurement:
                            if parameter_code not in ['00060', '00061']:
                                data_id = data_obj['id']
                                msg = f'Data value id {data_id} has unit different than its time series metadata. Skipping'
                                logger.warning(msg)
                                synthesis_messages.append(msg)
                                continue

                        try:
                            try:
                                data: Optional[float] = float(data_properties['value'])
                                data_str = data_properties['value']
                                # Hardcoded unit conversion for river discharge parameters
                                data, unit_of_measurement = _convert_discharge(data, data_str, parameter_code, unit_of_measurement)

                                if data is not None:
                                    result_quality.add(data_properties['approval_status'])
                                    result_TVPs.append(TimeValuePair(timestamp=data_properties['time'], value=data))
                                    result_TVP_quality.append(data_properties['approval_status'])

                            except Exception as e:
                                synthesis_messages.append(f"Unit Conversion Issue: {str(e)}")
                                logger.error(str(e))

                        except Exception as e:
                            synthesis_messages.append(f"TimeValuePair ERROR: {str(e)}")
                            logger.error(e)

                    measurement_timeseries_tvp_observation = MeasurementTimeseriesTVPObservation(
                        self,
                        id=ts_id,
                        unit_of_measurement=unit_of_measurement,
                        feature_of_interest_type=FeatureTypeEnum.POINT,
                        feature_of_interest=monitoring_feature,
                        # utc_offset=int(timezone_offset.split(":")[0]),
                        result=ResultListTVP(plugin_access=self, value=result_TVPs, result_quality=result_TVP_quality),
                        observed_property=parameter_code,
                        result_quality=list(result_quality),
                        aggregation_duration=query.aggregation_duration[0],
                        time_reference_position=TimeMetadataMixin.TIME_REFERENCE_MIDDLE,
                        statistic=statistic_id
                    )

                    yield measurement_timeseries_tvp_observation

        return StopIteration(synthesis_messages)


@basin3d_plugin
class USGSDataSourcePlugin(DataSourcePluginPoint):
    title = 'USGS Data Source Plugin'
    plugin_access_classes = (USGSMonitoringFeatureAccess, USGSMeasurementTimeseriesTVPObservationAccess)

    feature_types = ['POINT', 'REGION', 'BASIN', 'SUBREGION', 'SUBBASIN']

    class DataSourceMeta:
        """
        This is an internal metadata class for defining additional :class:`basin3d.core.models.DataSource`
        attributes.

        **Attributes:**
            - *id* - unique id short name
            - *name* - human friendly name (more descriptive)
            - *location* - resource location
            - *id_prefix* - id prefix to make model object ids unique across plugins
            - *credentials_format* - if the data source requires authentication, this is where the
                format of the stored credentials is defined.

        """
        # Data Source attributes
        id = 'USGS'  # unique id for the datasource
        location = 'https://api.waterdata.usgs.gov/ogcapi/v0'
        id_prefix = 'USGS'
        name = 'USGS'  # Human Friendly Data Source Name
