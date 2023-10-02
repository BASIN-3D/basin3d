
"""

.. currentmodule:: basin3d.plugins.epa

:platform: Unix, Mac
:synopsis: EPA Water Quality eXchange Plugin Definition
:module author: Danielle Christianson <dschristianson@lbl.gov>


* :class:`EPADataSourcePlugin` - This Data Source plugin maps the EPA Water Quality Exchange Data Source to the BASIN-3D Models
"""
import csv
import json
import requests
import urllib.parse

from typing import Iterator, List, Optional, Union
from datetime import datetime as dt, date

from basin3d.core import monitor
from basin3d.core.schema.enum import FeatureTypeEnum, MappedAttributeEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.access import get_url
from basin3d.core.models import AbsoluteCoordinate, RepresentativeCoordinate, Coordinate, GeographicCoordinate, \
    DepthCoordinate, VerticalCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature, \
    TimeMetadataMixin, TimeValuePair, ResultListTVP, AttributeMapping
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin
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


def _post_wqp_data_search(query_data: dict) -> Optional[requests.Response]:
    """
    Request the locations information
    :param query_data:
    :return:
    """
    url_with_params = 'https://www.waterqualitydata.us/data/Result/search?mimeType=csv'
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


def _get_location_info(loc_str: str) -> list:
    """
    Get location information from the EPA Data Source for specified loctions
    :param loc_str:
    :return:
    """
    loc_info_list = []
    url = ('https://www.waterqualitydata.us/ogcservices/wfs/?request=GetFeature&service=wfs&version=2.0.0&typeNames=wqp_sites'
           f'&SEARCHPARAMS={loc_str}%3Bproviders%3ASTORET&outputFormat=application%2Fjson')

    # ToDo: Enhancements -- stream and chunk for expected large returns, chunk site_ids into multiple calls if large list
    result = get_url(url)

    if result and result.status_code and result.status_code == 200:
        try:
            result_data = json.loads(result.content)
            loc_info_list = result_data.get('features', [])
        except Exception as e:
            logger.warning(f'EPA WQX {url} result could not be parsed {e}')
    else:
        error_code = 'NO RESPONSE'
        if result and result.status_code:
            error_code = result.status_code
        logger.warning(f'EPA WQX {url} returned error code {error_code}')

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

    coord: list = loc_geometry.get('coordinates')
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

    ============== === ====================================================
    EPA WQX            BASIN-3D
    ============== === ====================================================
    siteid          >> :class:`basin3d.core.models.MonitoringFeature`
    ============== === ====================================================

    """
    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature) -> Iterator[Optional[MonitoringFeature]]:
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
            if query.parent_feature:
                for parent_feature in query.parent_feature:
                    if len(parent_feature) not in [2, 4, 6, 8] or not parent_feature.isdigit():
                        msg = f'{self.datasource.name}: {parent_feature} does not appear to be a valid USGS huc: 2, 4, 6, 8-digit code.'
                        synthesis_messages.append(msg)
                        logger.warning(msg)
                        continue
                    wildcard = '*'
                    if len(parent_feature) == 8:
                        wildcard = ''
                    loc_list.append(f'huc%3A{parent_feature}{wildcard}')

            elif query.monitoring_feature:
                id_list_str = _make_mf_query_str(query.monitoring_feature)
                loc_list.append(''.join(id_list_str))

            for loc_str in loc_list:
                loc_info_list = _get_location_info(loc_str)
                for loc_info in loc_info_list:
                    mf_obj = _make_monitoring_feature_object(self, loc_info)
                    if mf_obj:
                        yield mf_obj

        return StopIteration(synthesis_messages)

    def get(self, query: QueryMonitoringFeature):
        """
        Get a EPA measurement location based on the id
        :param query: MonitoringFeature query, id must be specified
        :return:
        """
        # query.id will always be a string at this point with validation upstream, thus ignoring the type checking

        site_id = f'siteid%3A{query.id}'
        loc_info = _get_location_info(site_id)

        if loc_info:
            if len(loc_info) > 1:
                logger.warning(f'{self.datasource.id}: monitoring feature query by id {query.id} yielded too many site_id results from WQP. Cannot report single site.')
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
                                 results: dict, synthesis_messages: list) -> set:
    """
    Helper to parse the resultsPhysChem data.
    See https://www.waterqualitydata.us/portal_userguide/#table-7-sample-results-physicalchemical-result-retrieval-metadata

    Combo to find repeated measures in time
      MonitoringLocationIdentifier -- endpoint query
      CharacteristicName -- endpoint query
      ActivityMediaName -- endpoint query
      ResultSampleFractionText
      ResultDepthHeightMeasure/MeasureValue
      ResultTimeBasisText -- filter response | aggregation_duration, assume no value is instantaneous
      ResultTemperatureBasisText
      StatisticalBaseCode -- filter response | statistic

    Time
      NOTE: combo these start time values to report in the TVP time. Set time position to start.
      ActivityStartDate -- endpoint query
      ActivityStartTime/Time
      ActivityStartTime/TimeZoneCode

      # Using start date only, but including here for completeness
      ActivityEndDate -- endpoint query
      ActivityEndTime/Time
      ActivityEndTime/TimeZoneCode

    Measurement results
      ResultMeasureValue = value
      ResultMeasure/MeasureUnitCode = unit
      MeasureQualifierCode -- filter response | result_quality
      ResultValueTypeName -- filter response for Estimated ONLY | result_quality

    Additional info
      ActivityTypeCode, e.g. Field Msr/Obs, Sample-Routine
      ResultDepthHeightMeasure/MeasureUnitCode
      ResultDepthAltitudeReferencePointText

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
        start_date = row_dict.get('ActivityStartDate')
        if not start_date:
            return None

        start_timestamp_str = f'{start_date}'
        timestamp_str_format = '%Y-%m-%d'

        start_time = row_dict.get('ActivityStartTime/Time')
        if start_time:
            timestamp_str_format += ' %H:%M:%S'
            start_timestamp_str += f' {start_time}'

        start_time_zone = row_dict.get('ActivityStartTime/TimeZoneCode')
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
        ResultTimeBasisText -- filter response | aggregation_duration, assume empty value is instantaneous
        StatisticalBaseCode -- filter response | statistic
        MeasureQualifierCode -- filter response | result_quality
        ResultValueType -- filter response for Estimated ONLY | result_quality

        :param row_dict:
        :param query:
        :return:
        """

        if query.aggregation_duration and query.aggregation_duration[0] != 'NONE':
            ds_aggregation_duration = row_dict.get('ResultTimeBasisText')
            if not ds_aggregation_duration or ds_aggregation_duration and ds_aggregation_duration not in query.aggregation_duration:
                return False
        elif query.aggregation_duration[0] == 'NONE':
            if row_dict.get('ResultTimeBasisText'):
                return False

        if query.statistic:
            ds_statistic = row_dict.get('StatisticalBaseCode')
            if not ds_statistic or ds_statistic and ds_statistic not in query.statistic:
                return False

        # EPA estimated vocab is in vocabulary list separate from the rest of the result quality terms.
        # Revisit if BASIN-3D adds a ResultValueType
        if query.result_quality and EPA_ESTIMATED_VOCAB in query.result_quality:
            ds_result_value_type = row_dict.get('ResultValueTypeName')
            if ds_result_value_type == EPA_ESTIMATED_VOCAB:
                return True

        if query.result_quality:
            ds_result_value_type = row_dict.get('ResultValueTypeName')
            if ds_result_value_type == EPA_ESTIMATED_VOCAB:
                return False
            ds_result_quality = row_dict.get('ResultStatusIdentifier')
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
        mf_id = translate_empty_value(row_dict.get('MonitoringLocationIdentifier'))
        epa_observed_property = translate_empty_value(row_dict.get('CharacteristicName'))
        measurement = str_to_numeric(row_dict.get('ResultMeasureValue'))

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

        epa_sampling_medium = row_dict.get('ActivityMediaName')
        epa_sample_fraction = row_dict.get('ResultSampleFractionText')
        # Note there are also Activity DepthHeight fields
        epa_depth_height = row_dict.get('ResultDepthHeightMeasure/MeasureValue')
        epa_depth_height_unit = row_dict.get('ResultDepthHeightMeasure/MeasureUnitCode')
        epa_depth_height_ref = row_dict.get('ResultDepthAltitudeReferencePointText')
        epa_aggregation_duration = row_dict.get('ResultTimeBasisText')
        epa_temp = row_dict.get('ResultTemperatureBasisText')
        epa_statistic = row_dict.get('StatisticalBaseCode')
        epa_unit = translate_empty_value(row_dict.get('ResultMeasure/MeasureUnitCode'))

        # result_quality lives in 2 fields so first look in one field that has priority over the others.
        # If not, translate the the other field.
        epa_result_quality = translate_empty_value(row_dict.get('ResultValueTypeName'))
        if epa_result_quality is None or epa_result_quality != EPA_ESTIMATED_VOCAB:
            epa_result_quality = translate_empty_value(row_dict.get('ResultStatusIdentifier'))

        # The data return are a mix of siteids, variables, dates, etc because the data are not timeseries.
        # so make a unique key out of the fields that would define a single time series. Add timestamps / data to that
        # unique timeseries as the return is streamed.
        result_key = (f'{mf_id}-{basin3d_vocab}-{epa_sampling_medium}-{epa_unit}-{epa_depth_height}'
                      f'-{epa_depth_height_unit}-{epa_aggregation_duration}-{epa_statistic}-{epa_temp}')
        combo_dict = results.setdefault(result_key, {})
        if 'metadata' not in combo_dict.keys():
            id_info = f'{mf_id}-{epa_observed_property}'
            if translate_empty_value(epa_sample_fraction):
                id_info = f'{id_info}-sample_fraction:{epa_sample_fraction}'
            if translate_empty_value(epa_temp):
                id_info = f'{id_info}-activity_temp:{epa_temp}'
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
        synthesis_messages: List[str] = []

        params = {'dataProfile': 'resultPhysChem',
                  'siteid': query.monitoring_feature,
                  'characteristicName': query.observed_property,
                  'startDateLo': _reformat_date_for_epa_query(query.start_date)}

        if query.sampling_medium:
            params.update({"sampleMedia": query.sampling_medium})

        if query.end_date:
            params.update({"startDateHi": _reformat_date_for_epa_query(query.end_date)})

        # get data from resultPhysChem; possible extension biological results (sample based data)
        epa_data = _post_wqp_data_search(params)

        if epa_data and epa_data.status_code == 200:
            # Because the data are all mixed together and we support multiple mappings,
            #   group the variables by the BASIN-3D vocab. Create a look up store for the variables in the query.
            op_map = self._get_observed_property_map(query.observed_property)

            results = {}  # type: ignore[var-annotated]
            mf_set = _parse_epa_results_phys_chem(epa_data, query, op_map, results, synthesis_messages)

            mf_query_str = _make_mf_query_str(list(mf_set))
            loc_info = _get_location_info(mf_query_str)
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
            attr_mapping: AttributeMapping = get_datasource_mapped_attribute(self, MappedAttributeEnum.OBSERVED_PROPERTY, observed_property)
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