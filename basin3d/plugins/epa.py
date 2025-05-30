
"""

.. currentmodule:: basin3d.plugins.epa

:platform: Unix, Mac
:synopsis: EPA Water Quality eXchange Plugin Definition
:module author: Danielle Christianson <dschristianson@lbl.gov>


* :class:`EPADataSourcePlugin` - This Data Source plugin maps the EPA Water Quality Exchange Data Source to the BASIN-3D Models
"""
import csv
import json
import os
import requests
import urllib.parse

from copy import deepcopy
from typing import List, Optional, Union
from datetime import datetime as dt, date

from basin3d.core import monitor
from basin3d.core.schema.enum import FeatureTypeEnum, MappedAttributeEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.access import get_url, AccessIssueException
from basin3d.core.models import AbsoluteCoordinate, RepresentativeCoordinate, Coordinate, GeographicCoordinate, \
    DepthCoordinate, VerticalCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature, \
    TimeMetadataMixin, TimeValuePair, ResultListTVP, AttributeMapping
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin, separate_list_types
from basin3d.core.translate import get_datasource_mapped_attribute
from basin3d.core.types import SpatialSamplingShapes

logger = monitor.get_logger(__name__)


# https://www.epa.gov/waterdata/storage-and-retrieval-and-water-quality-exchange-domain-services-and-downloads
# Download link: https://cdx.epa.gov/wqx/download/DomainValues/TimeZone.csv
# Accessed 2023-10-03
TIMEZONE_MAP = {
    "ADT": {"utc_offset": "-03:00", "name": "Atlantic Daylight Time"},  # -3,8/11/2006 10:57:50 AM
    "AHST": {"utc_offset": "-10:00", "name": "Alaska-Hawaii Standard Time (*retired: >1983 use AKST)"},  # -10,8/11/2006 10:57:50 AM
    "AKDT": {"utc_offset": "-08:00", "name": "Alaska Daylight Time"},  # -8,8/11/2006 10:57:50 AM
    "AKST": {"utc_offset": "-09:00", "name": "Alaska Standard Time"},  # -9,8/11/2006 10:57:50 AM
    "AST": {"utc_offset": "-04:00", "name": "Atlantic Standard Time"},  # -4,8/11/2006 10:57:50 AM
    "BST": {"utc_offset": "-11:00", "name": "Bering Standard Time (*retired: >1983 use HAST)"},  # -11,8/11/2006 10:57:50 AM
    "CDT": {"utc_offset": "-05:00", "name": "Central Daylight Time"},  # -5,8/11/2006 10:57:50 AM
    "CEST": {"utc_offset": "+02:00", "name": "Sweden Daylight Time or Central European Standard Time"},  # 2,7/12/2016 9:39:33 AM
    "CET": {"utc_offset": "+01:00", "name": "Stockholm Sweden Time or Central European Time"},  # 1,7/12/2016 9:39:33 AM
    "CST": {"utc_offset": "-06:00", "name": "Central Standard Time"},  # -6,8/11/2006 10:57:50 AM
    "EDT": {"utc_offset": "-04:00", "name": "Eastern Daylight Time"},  # -4,8/11/2006 10:57:50 AM
    "EST": {"utc_offset": "-05:00", "name": "Eastern Standard Time"},  # -5,8/11/2006 10:57:50 AM
    "GMT": {"utc_offset": "+00:00", "name": "Greenwich Mean Time"},  # 0,6/19/2009 11:19:08 AM
    "GST": {"utc_offset": "+10:00", "name": "Guam Standard Time Zone (also Chamorro Standard Time)"},  # 10,8/11/2006 10:57:50 AM
    "HADT": {"utc_offset": "-09:00", "name": "Hawaii-Aleutian Daylight Time"},  # -9,8/11/2006 10:57:50 AM
    "HAST": {"utc_offset": "-10:00", "name": "Hawaii-Aleutian Standard Time"},  # -10,8/11/2006 10:57:50 AM
    "KST": {"utc_offset": "+09:00", "name": "Korea Standard Time"},  # 9,7/12/2016 9:39:33 AM
    "MDT": {"utc_offset": "-06:00", "name": "Mountain Daylight Time"},  # -6,8/11/2006 10:57:50 AM
    "MST": {"utc_offset": "-07:00", "name": "Mountain Standard Time"},  # -7,8/11/2006 10:57:50 AM
    "NDT": {"utc_offset": "-02:00", "name": "Newfoundland Daylight Time"},  # -2.5,8/11/2006 10:57:50 AM
    "NST": {"utc_offset": "-03:00", "name": "Newfoundland Standard Time"},  # -3.5,8/11/2006 10:57:50 AM
    "PDT": {"utc_offset": "-07:00", "name": "Pacific Daylight Time"},  # -7,8/11/2006 10:57:50 AM
    "PST": {"utc_offset": "-08:00", "name": "Pacific Standard Time"},  # -8,8/11/2006 10:57:50 AM
    "SST": {"utc_offset": "-11:00", "name": "American Samoa Standard Time"},  # -11,8/11/2006 10:57:50 AM
    "UTC": {"utc_offset": "+00:00", "name": "Coordinated Universal Time"},  # 0,1/15/2010 1:10:00 PM
    "YST": {"utc_offset": "-09:00", "name": "Yukon Standard Time: U.S.-Yukatat (*retired: >1983 use AKST)"}  # -9,8/11/2006 10:57:50 AM
}

EPA_WQP_API_VERSION = os.environ.get('EPA_WQP_API_VERSION', '2.2')

# mapping of EPA WQP API v2.2 to v3.0
# https://www.epa.gov/waterdata/water-quality-portal-quick-reference-guide
# accessed 2024-09-23
FIELD_NAMES = {
    'loc_id': {'2.2': 'MonitoringLocationIdentifier', '3.0': 'Location_Identifier'},
    'lat': {'2.2': 'LatitudeMeasure', '3.0': 'Location_Latitude'},
    'long': {'2.2': 'LongitudeMeasure', '3.0': 'Location_Longitude'},
    'org_name': {'2.2': 'OrganizationFormalName', '3.0': 'Org_FormalName'},
    'loc_name': {'2.2': 'MonitoringLocationName', '3.0': 'Location_Name'},
    'huc_8_code': {'2.2': 'HUCEightDigitCode', '3.0': 'Location_HUCEightDigitCode'},
    'provider': {'2.2': 'ProviderName', '3.0': 'ProviderName'},
    'phys_chem_results': {'2.2': 'resultPhysChem', '3.0': 'fullPhysChem'},
    'observed_property': {'2.2': 'CharacteristicName', '3.0': 'Result_Characteristic'},
    'sampling_media': {'2.2': 'ActivityMediaName', '3.0': 'Activity_Media'},
    'sample_fraction': {'2.2': 'ResultSampleFractionText', '3.0': 'Result_SampleFraction'},
    'depth_height': {'2.2': 'ResultDepthHeightMeasure/MeasureValue', '3.0': 'ResultDepthHeight_Measure'},
    'aggregation_duration': {'2.2': 'ResultTimeBasisText', '3.0': 'Result_TimeBasis'},
    'sampling_temp': {'2.2': 'ResultTemperatureBasisText', '3.0': 'Result_MeasureTemperatureBasis'},
    'statistic': {'2.2': 'StatisticalBaseCode', '3.0': 'Result_StatisticalBase'},
    'start_date': {'2.2': 'ActivityStartDate', '3.0': 'Activity_StartDate'},
    'start_time': {'2.2': 'ActivityStartTime/Time', '3.0': 'Activity_StartTime'},
    'start_time_zone': {'2.2': 'ActivityStartTime/TimeZoneCode', '3.0': 'Activity_StartTimeZone'},
    'end_date': {'2.2': 'ActivityEndDate', '3.0': 'Activity_EndDate'},
    'end_time': {'2.2': 'ActivityEndTime/Time', '3.0': 'Activity_EndTime'},
    'end_time_zone': {'2.2': 'ActivityEndTime/TimeZoneCode', '3.0': 'Activity_EndTimeZone'},
    'result': {'2.2': 'ResultMeasureValue', '3.0': 'Result_Measure'},
    'result_unit': {'2.2': 'ResultMeasure/MeasureUnitCode', '3.0': 'Result_MeasureUnit'},
    'result_status': {'2.2': 'ResultStatusIdentifier', '3.0': 'Result_MeasureStatusIdentifier'},
    'result_type': {'2.2': 'ResultValueTypeName', '3.0': 'Result_MeasureType'},
    'activity_type': {'2.2': 'ActivityTypeCode', '3.0': 'Activity_TypeCode'},
    'depth_height_unit': {'2.2': 'ResultDepthHeightMeasure/MeasureUnitCode', '3.0': 'ResultDepthHeight_MeasureUnit'},
    'depth_height_ref': {'2.2': 'ResultDepthAltitudeReferencePointText', '3.0': 'ResultDepthHeight_AltitudeReferencePoint'}
}

EPA_GEOSERVER_WFS_TIMEOUT_LIMIT = os.environ.get('EPA_GEOSERVER_WFS_TIMEOUT_LIMIT', 5)


def _post_wqp_search(search_type: str, query_data: dict,
                     api_version: str = EPA_WQP_API_VERSION) -> Optional[requests.Response]:
    """
    Request the locations information
    :param search_type: EPA WQP Data Profile type: Physical / Chemical (See FIELD_NAMES key 'phys_chem_results' for vocab, or Station (for locations)
    :param query_data: query in WQP vocabulary for the request
    :param api_version: EPA WQP API version: '2.2' and '3.0' currently supported; otherwise the url will be invalid.
    """
    url_api_version = 'INVALID_API_VERSION'

    if api_version == '2.2':
        url_api_version = 'data'
    elif api_version == '3.0':
        url_api_version = 'wqx3'

    url_with_params = f'https://www.waterqualitydata.us/{url_api_version}/{search_type}/search?mimeType=csv'
    headers = {'content-type': 'application/json'}
    data = {'providers': ['STORET']}
    data.update(query_data)
    logger.info(f'url: {url_with_params} and payload {str(data)}')

    try:
        data_json = json.dumps(data)
        resp = requests.post(url_with_params, data_json, headers=headers, verify=True, stream=True)
        logger.info(f'response: {resp.status_code}')
        return resp
    except Exception as e:
        logger.error(f'Unsuccessful attempt to query WQP; {e}')
        return None


def _get_location_info(loc_type: str, loc_list: list, synthesis_messages: list = []) -> list:
    """
    Get location information. First try the GeoServer WFS endpoint and if is not available
       within the timeout limit, fail over to a WQP Station request.
    :param loc_type: siteid or huc
    :param loc_list: list of location identifiers
    :param synthesis_messages: list of messages to report warning / error messages
    """
    loc_info_list = []
    fail_over = False
    msg: Optional[str] = None

    # try the ogc approach
    try:
        if loc_type == 'siteid':
            id_list_str = _make_mf_query_str(loc_list)
            loc_info_list = _get_location_info_ogc(id_list_str, synthesis_messages)
        else:
            for huc in loc_list:
                huc_query_str = f'huc%3A{huc}'
                loc_info_results = _get_location_info_ogc(huc_query_str, synthesis_messages)
                loc_info_list.extend(loc_info_results)
    # if fails, stream the csv file
    except TimeoutError as e:
        fail_over = True
        msg = f'WFS Geoserver timed out, fail over to WQP Station request\nError: {e}'
    except AccessIssueException as e:
        fail_over = True
        msg = f'WFS Geoserver did not respond appropriately, fail over to WQP Station request\nError: {e}'
    except Exception as e:
        fail_over = True
        msg = f'WFS Geoserver did not respond appropriately, fail over to WQP Station request\nError: {e}'

    if fail_over:
        logger.warning(msg)
        synthesis_messages.append(msg)
        loc_info_list.extend(_get_location_info_csv(loc_type, loc_list, synthesis_messages))

    return loc_info_list


def _get_location_info_csv(loc_type: str, loc_list: list, synthesis_messages: list) -> list:
    """
    Get location information from the EPA Data Source using the Station WQP request type
    :param loc_type: siteid or huc
    :param loc_list: list of location identifiers
    :param synthesis_messages: list of messages to report warning / error messages
    """

    api_version = EPA_WQP_API_VERSION
    loc_info: List[dict] = []
    wqp_response = _post_wqp_search('Station', {loc_type: loc_list}, api_version)

    if not wqp_response or (wqp_response.status_code and wqp_response.status_code != 200):
        msg = 'No or invalid response to WQP Station request'
        logger.error(msg)
        synthesis_messages.append(msg)
        return loc_info

    wqp_csv = _get_csv_dict_reader(wqp_response)

    # Build a dictionary object that looks like the OGC WFS structure so have one parsing code
    for idx, row_dict in enumerate(wqp_csv):
        properties_name = row_dict.get(FIELD_NAMES['loc_id'][api_version])

        if not properties_name:
            msg = f'Row {idx} of the response csv does not have an identifier, skipping'
            logger.warning(msg)
            synthesis_messages.append(msg)
            continue

        geometry_coord_lat = row_dict.get(FIELD_NAMES['lat'][api_version])
        geometry_coord_long = row_dict.get(FIELD_NAMES['long'][api_version])

        if not geometry_coord_lat or not geometry_coord_long:
            msg = f'Row {idx} with identifier {properties_name} does not have both lat / long coordinates. skipping'
            logger.warning(msg)
            synthesis_messages.append(msg)
            continue

        properties_org_name = row_dict.get(FIELD_NAMES['org_name'][api_version])
        properties_loc_name = row_dict.get(FIELD_NAMES['loc_name'][api_version])
        properties_huc8 = row_dict.get(FIELD_NAMES['huc_8_code'][api_version])
        properties_provider = row_dict.get(FIELD_NAMES['provider'][api_version])

        loc_info.append({
            'geometry': {'type': 'Point', 'coordinates': [geometry_coord_long, geometry_coord_lat]},
            'properties': {'name': properties_name, 'locName': properties_loc_name,
                           'huc8': properties_huc8, 'orgName': properties_org_name,
                           'provider': properties_provider}})

    return loc_info


def _get_location_info_ogc(loc_str: str, synthesis_messages: list) -> list:
    """
    Get location information from the EPA Data Source for specified locations from WFS in OGC format
    :param loc_str: query string for the GeoServer WFS request
    :param synthesis_messages: list of messages to report warning / error messages
    """
    loc_info_list = []
    url = ('https://www.waterqualitydata.us/ogcservices/wfs/?request=GetFeature&service=wfs&version=2.0.0&typeNames=wqp_sites'
           f'&SEARCHPARAMS={loc_str}%3Bproviders%3ASTORET&outputFormat=application%2Fjson')

    # if the timeout limit is set via an environmental variable it will be a str. Convert it to float.
    # if the conversion fails, then set value to str and get_url will throw an exception which will result
    #     in the get_loc_info method will fail over to the WQP Station request.
    try:
        geoserver_timeout_limit: Union[str, float, int] = float(EPA_GEOSERVER_WFS_TIMEOUT_LIMIT)
    except ValueError as e:
        msg = f'EPA_GEOSERVER_WFS_TIMEOUT_LIMIT: {e}'
        logger.warning(msg)
        synthesis_messages.append(msg)
        geoserver_timeout_limit = EPA_GEOSERVER_WFS_TIMEOUT_LIMIT
    except Exception as e:
        msg = f'EPA_GEOSERVER_WFS_TIMEOUT_LIMIT: Unknown exception while trying to convert value to float: {e}'
        logger.error(msg)
        synthesis_messages.append(msg)
        raise Exception(msg)

    # ToDo: Enhancements -- stream and chunk for expected large returns, chunk site_ids into multiple calls if large list
    result = get_url(url, timeout=geoserver_timeout_limit)

    if result and result.status_code and result.status_code == 200:
        try:
            result_data = json.loads(result.content)
            loc_info_list = result_data.get('features', [])
        except Exception as e:
            msg = f'EPA WQX {url} result could not be parsed {e}'
            logger.warning(msg)
            synthesis_messages.append(msg)
    else:
        error_code = 'NO RESPONSE'
        if result and result.status_code:
            error_code = result.status_code
        msg = f'EPA WQX {url} returned error code {error_code}. Trying fail over.'
        # Raise exception that will be caught in the _get_location_info try/except code block.
        #   The exception will be caught and the msg will be reported in the failover attempt.
        raise AccessIssueException(msg)

    return loc_info_list


def _make_monitoring_feature_object(plugin_access: DataSourcePluginAccess, loc_info_obj: dict) -> Optional[MonitoringFeature]:
    """
    Generate a monitoring feature object from the EPA location info
    :param loc_info_obj:
    :return:
    """
    loc_geometry_type = None
    loc_geometry = loc_info_obj.get('geometry')
    if loc_geometry:
        loc_geometry_type = loc_geometry.get('type')
    if not loc_geometry_type or loc_geometry_type.upper() != FeatureTypeEnum.POINT.value:
        return None

    loc_properties = loc_info_obj.get('properties')
    if not loc_properties:
        return None

    loc_id = loc_properties.get('name')
    loc_name = loc_properties.get('locName')
    huc8 = loc_properties.get('huc8')
    org = loc_properties.get('orgName')
    provider = loc_properties.get('provider')

    desc = f'Location is part of USGS huc {huc8}; organization {org}; provider {plugin_access.datasource.id} {provider}'

    monitoring_feature = MonitoringFeature(
        plugin_access,
        id=loc_id,
        name=loc_name,
        description=desc,
        feature_type=FeatureTypeEnum.POINT,
        shape=SpatialSamplingShapes.SHAPE_POINT)

    coord: list = loc_geometry.get('coordinates')  # type: ignore
    if coord:
        monitoring_feature.coordinates = Coordinate(
            absolute=AbsoluteCoordinate(horizontal_position=GeographicCoordinate(
                **{"latitude": float(coord[1]),
                   "longitude": float(coord[0]),
                   "units": GeographicCoordinate.UNITS_DEC_DEGREES})))

    return monitoring_feature


def _make_mf_query_str(query_mf: list) -> str:
    """
    Create a monitoring feature query string for the EPA locations REST API
    :param query_mf:
    :return:
    """
    id_list = []
    for idx, mf_id in enumerate(query_mf):
        url_encoded_id = urllib.parse.quote(mf_id)
        if idx == 0:
            id_list.append(f'siteid%3A{url_encoded_id}')
            continue
        id_list.append(f'%7C{url_encoded_id}')
    return ''.join(id_list)


def _reformat_date_for_epa_query(b3d_query_date: date) -> str:
    """
    Change BASIN-3D date object to EPA format: MM-DD-YYYY
    :param b3d_query_date:
    :return:
    """
    date_str = str(b3d_query_date)  # YYYY-MM-DD
    date_str_pieces = date_str.split('-')
    return f'{date_str_pieces[1]}-{date_str_pieces[2]}-{date_str_pieces[0]}'


class EPAMonitoringFeatureAccess(DataSourcePluginAccess):
    """
    Access for mapping EPA Water Quality Exchange sampling locations to
    :class:`~basin3d.core.models.MonitoringFeature` objects.

    Using WFS OGC Mapping service: '~/ogcservices/wfs/?request=GetFeature&service=wfs&version=2.0.0&typeNames=wqp_sites&SEARCHPARAMS=providers%3ASTORET&outputFormat=application%2Fjson'
    Backup: WQP Station endpoint: 'https://www.waterqualitydata.us/{url_api_version}/Station/search?mimeType=csv'

    ============== === ====================================================
    EPA WQX            BASIN-3D
    ============== === ====================================================
    siteid          >> :class:`basin3d.core.models.MonitoringFeature`
    ============== === ====================================================

    """
    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature):
        """
        Return measurement locations based on the query parameters
        Either a parent_feature list or monitoring_feature list should be specified.
        USGS hucs can be specified for parent_feature parameters (e.g., EPA-14020001)

        :param query: MonitoringFeature query
        :return:
        """
        synthesis_messages: List[str] = []

        feature_type = isinstance(query.feature_type, FeatureTypeEnum) and query.feature_type.value or query.feature_type

        if feature_type and feature_type not in EPADataSourcePlugin.feature_types:
            msg = f'Feature type {feature_type} not supported by {self.datasource.name}.'
            synthesis_messages.append(msg)
            logger.warning(msg)
        elif not query.parent_feature and not query.monitoring_feature:
            msg = f'{self.datasource.name} requires either a parent feature or monitoring feature be specified in the query.'
            synthesis_messages.append(msg)
            logger.warning(msg)
        elif query.parent_feature and query.monitoring_feature:
            msg = f'{self.datasource.name} does not support querying monitoring features by both parent_feature (huc) and monitoring_feature (list of ids).'
            synthesis_messages.append(msg)
            logger.warning(msg)
        else:
            loc_list = []
            loc_type = None
            if query.parent_feature:
                for parent_feature in query.parent_feature:
                    if len(parent_feature) not in [2, 4, 6, 8, 10, 12] or not parent_feature.isdigit():
                        msg = f'{self.datasource.name}: {parent_feature} does not appear to be a valid USGS huc: 2, 4, 6, 8, 10, or 12-digit code.'
                        synthesis_messages.append(msg)
                        logger.warning(msg)
                        continue
                    wildcard = '*'
                    if len(parent_feature) >= 8:
                        wildcard = ''
                    loc_list.append(f'{parent_feature}{wildcard}')
                if loc_list:
                    loc_type = 'huc'

            elif query.monitoring_feature:
                mf_separate_list = separate_list_types(query.monitoring_feature, {'named': str, 'bbox': tuple})
                named_mf_list = mf_separate_list.get('named', [])

                # ToDo: future, enable bbox query
                if not named_mf_list:
                    logger.info(f'Data source {self.datasource.id} requires specification of monitoring feature identifier.')
                    yield

                loc_list = deepcopy(named_mf_list)
                loc_type = 'siteid'

            if loc_type:
                loc_info_list = _get_location_info(loc_type, loc_list, synthesis_messages)  # type: ignore
                for loc_info in loc_info_list:
                    mf_obj = _make_monitoring_feature_object(self, loc_info)
                    if mf_obj:
                        yield mf_obj

        return StopIteration(synthesis_messages)  # type: ignore

    def get(self, query: QueryMonitoringFeature):
        """
        Get a EPA measurement location based on the id
        :param query: MonitoringFeature query, id must be specified
        :return:
        """
        # query.id will always be a string at this point with validation upstream, thus ignoring the type checking

        loc_info = _get_location_info('siteid', [query.id])

        if loc_info:
            if len(loc_info) > 1:
                msg = f'{self.datasource.id}: monitoring feature query by id {query.id} yielded too many site_id results from WQP. Cannot report single site.'
                logger.warning(msg)
                return None
            return _make_monitoring_feature_object(self, loc_info[0])

        return None


def _get_csv_dict_reader(epa_response: requests.Response) -> csv.DictReader:
    """
    Generate a DictReader from the EPA data response. Isolate the functionality so that it can easily be mocked for testing.
    :param epa_response:
    :return:
    """
    return csv.DictReader(epa_response.iter_lines(decode_unicode=True))


def _convert_distance_unit_to_meters(unit: str) -> Optional[float]:
    """
    Distance unit converter for depth / height values.
    :param unit: the measurement unit variable
    :return:
    """
    multiplier = None
    if unit == 'ft':
        multiplier = 0.3048
    elif unit == 'in':
        multiplier = 0.0254
    elif unit == 'cm':
        multiplier = 0.01
    elif unit == 'm':
        multiplier = 1
    return multiplier


def _parse_epa_results_phys_chem(wqp_response: requests.Response, query: QueryMeasurementTimeseriesTVP, op_map: dict,
                                 results: dict, synthesis_messages: list, api_version: str = EPA_WQP_API_VERSION) -> set:
    """
    Helper to parse the resultsPhysChem data.
    See https://www.waterqualitydata.us/portal_userguide/#table-7-sample-results-physicalchemical-result-retrieval-metadata

    Combo to find repeated measures in time (api_version = '2.2' | '3.0')
      MonitoringLocationIdentifier | Location_Identifier -- endpoint query
      CharacteristicName | Result_Characteristic -- endpoint query
      ActivityMediaName | Activity_Media -- endpoint query
      ResultSampleFractionText | Result_SampleFraction
      ResultDepthHeightMeasure/MeasureValue | ResultDepthHeight_Measure
      ResultTimeBasisText | Result_TimeBasis -- filter response | aggregation_duration, assume no value is instantaneous
      ResultTemperatureBasisText | Result_MeasureTemperatureBasis
      StatisticalBaseCode | Result_StatisticalBase -- filter response | statistic

    Time (api_version = '2.2' | '3.0')
      NOTE: combo these start time values to report in the TVP time. Set time position to start.
      ActivityStartDate | Activity_StartDate-- endpoint query
      ActivityStartTime/Time | Activity_StartTime
      ActivityStartTime/TimeZoneCode | Activity_StartTimeZone

      # Using start date only, but including here for completeness (api_version = '2.2' | '3.0')
      ActivityEndDate | Activity_EndDate -- endpoint query
      ActivityEndTime/Time | Activity_EndTime
      ActivityEndTime/TimeZoneCode | Activity_EndTimeZone

    Measurement results (api_version = '2.2' | '3.0')
      ResultMeasureValue | Result_Measure = value
      ResultMeasure/MeasureUnitCode | Result_MeasureUnit = unit
      ResultStatusIdentifier | Result_MeasureStatusIdentifier -- filter response | result_quality
      ResultValueTypeName | Result_MeasureType -- filter response for Estimated ONLY | result_quality

    Additional info (api_version = '2.2' | '3.0')
      ActivityTypeCode | Activity_TypeCode, e.g. Field Msr/Obs, Sample-Routine
      ResultDepthHeightMeasure/MeasureUnitCode | ResultDepthHeight_MeasureUnit
      ResultDepthAltitudeReferencePointText | ResultDepthHeight_AltitudeReferencePoint

    :param wqp_response:
    :param query:
    :param op_map: observed property map of datasource variable vocab to basin vocab
    :param results:
    :param synthesis_messages:
    :return:
    """

    # ToDo: get estimated value from attribute mappings directly.
    EPA_ESTIMATED_VOCAB = 'Estimated'

    def translate_empty_value(value):
        """
        Turn empty string values into None
        :param value:
        :return:
        """
        if value == '':
            return None
        return value

    def make_timestamp(row_dict: dict) -> Optional[str]:
        start_date = row_dict.get(FIELD_NAMES['start_date'][api_version])
        if not start_date:
            return None

        start_timestamp_str = f'{start_date}'
        timestamp_str_format = '%Y-%m-%d'

        start_time = row_dict.get(FIELD_NAMES['start_time'][api_version])
        if start_time:
            timestamp_str_format += ' %H:%M:%S'
            start_timestamp_str += f' {start_time}'

        start_time_zone = row_dict.get(FIELD_NAMES['start_time_zone'][api_version])
        if start_time_zone:
            mapped_time_zone = TIMEZONE_MAP.get(start_time_zone)
            if mapped_time_zone:
                start_time_zone_int = mapped_time_zone.get("utc_offset")
                if start_time_zone_int:
                    start_timestamp_str += start_time_zone_int
                    timestamp_str_format += '%z'

        try:
            return dt.strptime(start_timestamp_str, timestamp_str_format).isoformat(sep='T')
        except Exception as e:
            # ToDo: enhancement, include more info in the msg
            msg = f'Could not parse and convert start timestamp: {start_timestamp_str}; {e}'
            synthesis_messages.append(msg)
            logger.warning(msg)
            return None

    def str_to_numeric(value) -> Optional[Union[float, int]]:
        if not translate_empty_value(value):
            return None

        is_float = True
        if '.' not in value:
            is_float = False

        try:
            if is_float:
                return float(value)
            return int(value)
        except Exception:
            # ToDo: enhancement, include more info in the msg
            msg = f'Could not parse expected numerical measurement value {value}'
            synthesis_messages.append(msg)
            logger.warning(msg)
            return None

    def is_in_query(row_dict: dict, query: QueryMeasurementTimeseriesTVP) -> bool:
        """
        (api version = '2.2' | '3.0')
        ResultTimeBasisText | Result_TimeBasis -- filter response | aggregation_duration, assume empty value is instantaneous
        StatisticalBaseCode | Result_StatisticalBase -- filter response | statistic
        ResultStatusIdentifier | Result_MeasureStatusIdentifier -- filter response | result_quality
        ResultValueTypeName | Result_MeasureType -- filter response for Estimated ONLY | result_quality

        :param row_dict:
        :param query:
        :return:
        """

        if query.aggregation_duration and query.aggregation_duration[0] != 'NONE':
            ds_aggregation_duration = row_dict.get(FIELD_NAMES['aggregation_duration'][api_version])
            if not ds_aggregation_duration or ds_aggregation_duration and ds_aggregation_duration not in query.aggregation_duration:
                return False
        elif query.aggregation_duration[0] == 'NONE':
            if row_dict.get(FIELD_NAMES['aggregation_duration'][api_version]):
                return False

        if query.statistic:
            ds_statistic = row_dict.get(FIELD_NAMES['statistic'][api_version])
            if not ds_statistic or ds_statistic and ds_statistic not in query.statistic:
                return False

        # EPA estimated vocab is in vocabulary list separate from the rest of the result quality terms.
        # Revisit if BASIN-3D adds a ResultValueType
        if query.result_quality and EPA_ESTIMATED_VOCAB in query.result_quality:
            ds_result_value_type = row_dict.get(FIELD_NAMES['result_type'][api_version])
            if ds_result_value_type == EPA_ESTIMATED_VOCAB:
                return True

        if query.result_quality:
            ds_result_value_type = row_dict.get(FIELD_NAMES['result_type'][api_version])
            if ds_result_value_type == EPA_ESTIMATED_VOCAB:
                return False
            ds_result_quality = row_dict.get(FIELD_NAMES['result_status'][api_version])
            if not ds_result_quality or ds_result_quality and ds_result_quality not in query.result_quality:
                return False

        return True

    # --------------- End of internal helper functions

    mf_set = set()

    # wqp_csv = csv.DictReader(wqp_response.iter_lines(decode_unicode=True))
    wqp_csv = _get_csv_dict_reader(wqp_response)
    for row_dict in wqp_csv:
        # filter out any results that don't match the query fields that were not specified in the call to WQP
        if not is_in_query(row_dict, query):
            continue

        # make the dictionary key
        mf_id = translate_empty_value(row_dict.get(FIELD_NAMES['loc_id'][api_version]))
        # epa_observed_property = translate_empty_value(row_dict.get('CharacteristicName'))
        epa_observed_property = translate_empty_value(row_dict.get(FIELD_NAMES['observed_property'][api_version]))
        measurement = str_to_numeric(row_dict.get(FIELD_NAMES['result'][api_version]))

        # check first set of required info
        if any([x is None for x in [mf_id, epa_observed_property, measurement]]):
            continue

        # get the basin3d vocab for synthesis and skip if no match
        basin3d_vocab = op_map.get(epa_observed_property)
        if not basin3d_vocab:
            continue

        mf_set.add(mf_id)

        # time
        start_timestamp = make_timestamp(row_dict)

        # if the time fields cannot be parsed, move on.
        if not start_timestamp:
            continue

        tvp = TimeValuePair(timestamp=start_timestamp, value=measurement)

        epa_sampling_medium = row_dict.get(FIELD_NAMES['sampling_media'][api_version])
        epa_sample_fraction = row_dict.get(FIELD_NAMES['sample_fraction'][api_version])
        # Note there are also Activity DepthHeight fields
        epa_depth_height = row_dict.get(FIELD_NAMES['depth_height'][api_version])
        epa_depth_height_unit = row_dict.get(FIELD_NAMES['depth_height_unit'][api_version])
        epa_depth_height_ref = row_dict.get(FIELD_NAMES['depth_height_ref'][api_version])
        epa_aggregation_duration = row_dict.get(FIELD_NAMES['aggregation_duration'][api_version])
        epa_sampling_temp = row_dict.get(FIELD_NAMES['sampling_temp'][api_version])
        epa_statistic = row_dict.get(FIELD_NAMES['statistic'][api_version])
        epa_unit = translate_empty_value(row_dict.get(FIELD_NAMES['result_unit'][api_version]))

        # result_quality lives in 2 fields so first look in one field that has priority over the others.
        # If not, translate the the other field.
        epa_result_quality = translate_empty_value(row_dict.get(FIELD_NAMES['result_type'][api_version]))
        if epa_result_quality is None or epa_result_quality != EPA_ESTIMATED_VOCAB:
            epa_result_quality = translate_empty_value(row_dict.get(FIELD_NAMES['result_status'][api_version]))

        # The data return are a mix of siteids, variables, dates, etc because the data are not timeseries.
        # so make a unique key out of the fields that would define a single time series. Add timestamps / data to that
        # unique timeseries as the return is streamed.
        result_key = (f'{mf_id}-{basin3d_vocab}-{epa_sampling_medium}-{epa_unit}-{epa_depth_height}'
                      f'-{epa_depth_height_unit}-{epa_aggregation_duration}-{epa_statistic}-{epa_sampling_temp}')
        combo_dict = results.setdefault(result_key, {})
        if 'metadata' not in combo_dict.keys():
            id_info = f'{mf_id}-{epa_observed_property}'
            if translate_empty_value(epa_sample_fraction):
                id_info = f'{id_info}-sample_fraction:{epa_sample_fraction}'
            if translate_empty_value(epa_sampling_temp):
                id_info = f'{id_info}-activity_temp:{epa_sampling_temp}'
            if translate_empty_value(epa_depth_height_ref):
                id_info = f'{id_info}-depth_ref:{epa_depth_height_ref}'

            combo_dict['metadata'] = {
                'id': id_info,
                'mf_id': mf_id,
                'sampling_medium': translate_empty_value(epa_sampling_medium),
                'aggregation_duration': translate_empty_value(epa_aggregation_duration),
                'result_quality': set(),
                'statistic': translate_empty_value(epa_statistic),
                'unit_of_measurement': epa_unit,
                'height_depth': str_to_numeric(epa_depth_height),
                'height_depth_unit': translate_empty_value(epa_depth_height_unit),
                'observed_property': epa_observed_property
            }

        if epa_result_quality:
            combo_dict['metadata']['result_quality'].add(epa_result_quality)

        combo_dict.setdefault('result_tvp', []).append(tvp)
        combo_dict.setdefault('result_quality', []).append(epa_result_quality)

    return mf_set


class EPAMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """
    Water Quality Portal Service: https://www.waterqualitydata.us/

    Access for mapping EPA water quality data to
    :class:`~basin3d.core.models.MeasurementTimeseriesTVPObservation` objects.

    """

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """
        Generate EPA Measurement Timeseries TVP object. The data are not really time series. But we are treating them as such.

        :param query:
        :return:
        """
        api_version = EPA_WQP_API_VERSION

        synthesis_messages: List[str] = []

        mf_separated_lists = separate_list_types(query.monitoring_feature, {'named': str, 'bbox': tuple})
        named_monitoring_features = mf_separated_lists.get('named', [])

        # ToDo: future, expand to support bbox
        if not named_monitoring_features:
            logger.info(f'Data source {self.datasource.id} requires specification of monitoring feature identifier.')
            yield

        params = {'dataProfile': FIELD_NAMES['phys_chem_results'][api_version],
                  'siteid': named_monitoring_features,
                  'characteristicName': query.observed_property,
                  'startDateLo': _reformat_date_for_epa_query(query.start_date)}

        if query.sampling_medium:
            params.update({"sampleMedia": query.sampling_medium})

        if query.end_date:
            params.update({"startDateHi": _reformat_date_for_epa_query(query.end_date)})

        # get data from resultPhysChem; possible extension biological results (sample based data)
        epa_data = _post_wqp_search('Result', params, api_version)

        mf_list = []
        if epa_data and epa_data.status_code == 200:
            # Because the data are all mixed together and we support multiple mappings,
            #   group the variables by the BASIN-3D vocab. Create a look up store for the variables in the query.
            op_map = self._get_observed_property_map(query.observed_property)

            results = {}  # type: ignore[var-annotated]
            mf_set = _parse_epa_results_phys_chem(epa_data, query, op_map, results, synthesis_messages, api_version)
            mf_list = list(mf_set)

        if mf_list:
            # mf_query_str = _make_mf_query_str(mf_list)
            loc_info = _get_location_info('siteid', mf_list, synthesis_messages)
            loc_info_store = {}
            for loc_info_obj in loc_info:
                loc_properties = loc_info_obj.get('properties')
                if not loc_properties:
                    continue
                loc_id = loc_properties.get('name')
                if loc_id:
                    loc_info_store[loc_id] = loc_info_obj

            # loop thru the results
            for result in results.values():
                metadata = result.get('metadata')

                mf_id = metadata.get('mf_id')
                monitoring_feature_info = loc_info_store.get(mf_id)
                if not monitoring_feature_info:
                    msg = f'{self.datasource.id}: Location information for {mf_id} could not be found.'
                    logger.debug(msg)
                    continue

                mf_obj = _make_monitoring_feature_object(self, monitoring_feature_info)

                if not mf_obj:
                    continue

                if metadata.get('height_depth'):
                    depth_height_unit = metadata.get('height_depth_unit')
                    unit_multiplier = _convert_distance_unit_to_meters(depth_height_unit)
                    if depth_height_unit and unit_multiplier:
                        depth_height = metadata.get('height_depth')
                        depth_height *= unit_multiplier

                        if mf_obj.coordinates:
                            mf_obj.coordinates.representative = RepresentativeCoordinate(
                                vertical_position=DepthCoordinate(
                                    value=depth_height,
                                    distance_units=VerticalCoordinate.DISTANCE_UNITS_METERS))

                metadata_result_quality = None
                if metadata.get('result_quality'):
                    metadata_result_quality = list(metadata.get('result_quality'))

                # deal with result values
                measurement_result_quality = result.get('result_quality')
                if any(measurement_result_quality):
                    tvp_result = ResultListTVP(self, value=result.get('result_tvp'), result_quality=measurement_result_quality)
                else:
                    tvp_result = ResultListTVP(self, value=result.get('result_tvp'))

                meas_tvp_obj = MeasurementTimeseriesTVPObservation(
                    plugin_access=self,
                    id=metadata.get('id'),
                    observed_property=metadata.get('observed_property'),
                    feature_of_interest=mf_obj,
                    feature_of_interest_type=mf_obj.shape,
                    time_reference_position=TimeMetadataMixin.TIME_REFERENCE_START,
                    result_quality=metadata_result_quality,
                    result=tvp_result,
                    statistic=metadata.get('statistic'),
                    aggregation_duration=metadata.get('aggregation_duration'),
                    sampling_medium=metadata.get('sampling_medium'),
                    unit_of_measurement=metadata.get('unit_of_measurement')
                )

                yield meas_tvp_obj

        else:
            msg = f'{self.datasource.id}: No resultPhysChem results matched the query: {str(params)}'
            synthesis_messages.append(msg)

        return StopIteration(synthesis_messages)

    def _get_observed_property_map(self, observed_properties) -> dict:
        """
        Get a map of data source vocabulary to basin 3d vocabulary to group observations.
        :param observed_properties: list, observed properties
        :return:
        """
        op_map = {}
        for observed_property in observed_properties:
            attr_mapping: AttributeMapping = get_datasource_mapped_attribute(self, MappedAttributeEnum.OBSERVED_PROPERTY.value, observed_property)
            basin3d_vocab = attr_mapping.basin3d_vocab
            op_map[observed_property] = basin3d_vocab

        return op_map


@basin3d_plugin
class EPADataSourcePlugin(DataSourcePluginPoint):
    title = 'EPA Water Quality eXchange Data Source Plugin'
    plugin_access_classes = (EPAMonitoringFeatureAccess, EPAMeasurementTimeseriesTVPObservationAccess)

    # IF this list grows, EPAMonitoringFeature logic may need to be updated.
    feature_types = ['POINT']

    class DataSourceMeta:
        """
        This is an internal metadata class for defining additional :class:`~basin3d.models.DataSource`
        attributes.

        **Attributes:**
            - *id* - unique id short name
            - *name* - human friendly name (more descriptive)
            - *location* - resource location
            - *id_prefix* - id prefix to make model object ids unique across plugins
            - *credentials_format* - if the data source requires authentication, this is where the
                format of the stored credentials is defined.

        """
        # Citation info: National Water Quality Monitoring Council, YYYY, Water Quality Portal, accessed mm, dd, yyyy, hyperlink_for_query, https://doi.org/10.5066/P9QRKUVJ.

        # Data Source attributes
        id = 'EPA'  # unique id for the datasource
        location = 'https://www.waterqualitydata.us'
        id_prefix = 'EPA'
        name = 'EPA Water Quality eXchange'  # Human Friendly Data Source Name