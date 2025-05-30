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
import json

from basin3d.core import monitor
from typing import Any, List, Optional, Tuple

import requests

from basin3d.core.schema.enum import FeatureTypeEnum, AggregationDurationEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.access import get_url
from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, GeographicCoordinate, \
    MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature, \
    TimeMetadataMixin, TimeValuePair, ResultListTVP
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin, separate_list_types
from basin3d.core.types import SpatialSamplingShapes
from basin3d.plugins import usgs_huc_codes

logger = monitor.get_logger(__name__)

URL_USGS_HUC = "https://water.usgs.gov/GIS/new_huc_rdb.txt"


def convert_discharge(data, data_str, parameter, units):
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


def _translate_bbox(a_tuple: tuple) -> tuple:
    """
    Translate the basin3d bbox to USGS bbox
    -- eventually this should be done in the translate module but need more use cases
    -- ALSO: USGS is NAD83 and basin3d is WGS84 -- eventually need to convert. For now, they are close enough.

    basin3d order = west longitude, east longitude, south latitude, north latitude
    USGS order = west longitude, south latitude, east longitude, north latitude

    :param a_tuple:
    :return: tuple in correct order
    """
    reordered_tuple = (a_tuple[0], a_tuple[2], a_tuple[1], a_tuple[3])
    return reordered_tuple


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


def generator_usgs_measurement_timeseries_tvp_observation(view,
                                                          query: QueryMeasurementTimeseriesTVP,
                                                          synthesis_messages):
    """
    Get the data for USGS Daily Values or Instantaneous Values

    :param view: Access class
    :param query: Query information for this request
    :param synthesis_messages: list to hold any processing messages
    :returns: a generator object that yields data from the request in json form
    """

    # Temporal resolution is always daily.
    search_params: List[Tuple[str, Any]] = list()

    search_params.append(("startDT", query.start_date))

    if query.end_date:
        search_params.append(("endDT", query.end_date))

    search_params.append(("parameterCd", ",".join([str(o) for o in query.observed_property])))

    if query.statistic:
        # if aggregation duration is NONE (iv) and there is a query that has a stat param, clear the statistics list
        # add warning message to user, but there are no statistic values for IV call. need synthesis param to add message
        if query.aggregation_duration[0] == AggregationDurationEnum.NONE:
            synthesis_messages.append(
                f"USGS Instantaneous Values service does not support statistics and cannot be specified when aggregation_duration = {AggregationDurationEnum.NONE}. Specified statistic arguments will be ignored.")
            logger.info(
                f"USGS Instantaneous Values service does not support statistics and cannot be specified when aggregation_duration = {AggregationDurationEnum.NONE}. Specified statistic arguments will be ignored.")
        else:
            search_params.append(("statCd", ",".join([str(o) for o in query.statistic])))

    else:
        search_params.append(("siteStatus", "all"))

    # look for station locations only
    # THIS IS Stream tupe locations only
    search_params.append(("siteType", "ST"))

    # JSON format
    search_params.append(("format", "json"))

    # handle the monitoring features
    monitoring_feature_types = separate_list_types(query.monitoring_feature, {'named': str, 'bbox': tuple})
    monitoring_feature_named = monitoring_feature_types.get('named', [])
    monitoring_feature_filters = []  # overall list of monitoring feature query elements

    if monitoring_feature_named and len(monitoring_feature_named[0]) > 2:
        # search for stations
        monitoring_feature_filters.append(("sites", ",".join(monitoring_feature_named)))
    elif monitoring_feature_named:
        # search for stations by specifying the huc
        monitoring_feature_filters.append(("huc", ",".join(monitoring_feature_named)))

    # translate the bounding box coordinates to the usgs API format. Note the usgs parameter name is bBox.
    monitoring_features_bbox = monitoring_feature_types.get('bbox', [])
    monitoring_features_bbox = [_translate_bbox(bbox_value) for bbox_value in monitoring_features_bbox]
    monitoring_features_bbox = [_convert_tuple_to_str(bbox_coord) for bbox_coord in monitoring_features_bbox]
    monitoring_feature_filters.extend([('bBox', bbox_coord) for bbox_coord in monitoring_features_bbox if bbox_coord is not None])

    if not monitoring_feature_filters:
        msg = 'no monitoring features extracted from the query'
        logger.error(msg)
        synthesis_messages.append(msg)

    # Request the data points, calls IV or DV depending on aggregation duration passed in param
    # Default to DV service if aggregation duration is DAY or when nothing is specified
    # Calls IV service in aggregation duration is NONE
    endpoint = query.aggregation_duration[0] == AggregationDurationEnum.NONE and "iv" or "dv"
    url = f'{view.datasource.location}{endpoint}'

    for idx, mf_filter in enumerate(monitoring_feature_filters):
        # if not the first filter in the list, remove the last element in the search params.
        if idx > 0:
            search_params = search_params[:-1]

        search_params.append(mf_filter)

        response = get_url(url, params=search_params)

        if response.status_code == 200:
            try:

                json_obj = response.json()

                # There is a valid json response
                if json_obj:
                    timeseries_json = json_obj['value']['timeSeries']

                    # Iterate over monitoring_features
                    for data_json in timeseries_json:
                        yield data_json

            except json.decoder.JSONDecodeError:
                synthesis_messages.append("JSON Not Returned: {}".format(response.content))
                logger.error("JSON Not Returned: {}".format(response.content))
        else:
            import re
            p = re.compile(r'<.*?>')
            synthesis_messages.append("HTTP {}: {}".format(response.status_code, p.sub(' ', response.text)))
            logger.error("HTTP {}: {}".format(response.status_code, p.sub(' ', response.text)))


def iter_rdb_to_json(rdb_text):
    """
    Generator that iterates over an rdb file

    :param rdb_text: USGS rdb data
    :return:
    """
    header = None
    format = None  # not doing anything with this
    for i, line in enumerate(rdb_text.split("\n")):
        line = line.replace("\r", "")
        line = line.lstrip()
        if line and not line[0] == '#':
            if not header:
                header = line.split()
                continue
            elif not format:
                format = line.split()

            elif line and line[0] not in ['#', ' ', '<', ' ']:
                data = line.split("\t")

                json_object = dict(zip(header, data))
                yield json_object


def _load_point_obj(datasource, json_obj, observed_property_variables, synthesis_messages):
    """
    Instantiate the object

    #
    # The following selected fields are included in this output:
    #
    #  agency_cd       -- Agency
    #  site_no         -- Site identification number
    #  station_nm      -- Site name
    #  site_tp_cd      -- Site type
    #  dec_lat_va      -- Decimal latitude
    #  dec_long_va     -- Decimal longitude
    #  coord_acy_cd    -- Latitude-longitude accuracy
    #  dec_coord_datum_cd -- Decimal Latitude-longitude datum
    #  alt_va          -- Altitude of Gage/land surface
    #  alt_acy_va      -- Altitude accuracy
    #  alt_datum_cd    -- Altitude datum
    #  huc_cd          -- Hydrologic unit code

    :param json_obj:
    :param observed_property_variables: list of locations and their available variables
    :param synthesis_messages
    :return:
    """

    if 'site_no' in json_obj:
        id = json_obj['site_no']

        lat, lon = None, None
        try:
            lat, lon = float(json_obj['dec_lat_va']), float(json_obj['dec_long_va'])
        except Exception as e:
            synthesis_messages.append(f"Error getting latlon: {str(e)}")
            logger.error(str(e))
        if id not in observed_property_variables.keys():
            mf_opv = "Could not find observed property variables for this monitoring feature"
            msg = f"Could not find observed property variables for this monitoring feature: {id}"
            synthesis_messages.append(msg)
            logger.warning({msg})
        else:
            mf_opv = observed_property_variables[id]
        monitoring_feature = MonitoringFeature(
            datasource,
            id="{}".format(id),
            name=json_obj['station_nm'],
            feature_type=FeatureTypeEnum.POINT,
            shape=SpatialSamplingShapes.SHAPE_POINT,
            related_sampling_feature_complex=[RelatedSamplingFeature(
                datasource,
                related_sampling_feature=json_obj['huc_cd'],
                related_sampling_feature_type=FeatureTypeEnum.SUBBASIN,  # previously site
                role=RelatedSamplingFeature.ROLE_PARENT
            )],
            # geographical_group_id=huc_accounting_unit_id,
            # geographical_group_type=FeatureTypeEnum.REGION,
            observed_properties=mf_opv,
            coordinates=Coordinate(
                absolute=AbsoluteCoordinate(
                    horizontal_position=GeographicCoordinate(
                        **{"latitude": lat,
                           "longitude": lon,
                           "datum": json_obj['dec_coord_datum_cd'],
                           "units": GeographicCoordinate.UNITS_DEC_DEGREES}))
            )
        )

        # huc_accounting_unit_id = json_obj['huc_cd'][0:6]  #Subbasin
        if json_obj['alt_va'] and json_obj['alt_acy_va'] and json_obj['alt_datum_cd']:
            monitoring_feature.coordinates.absolute.vertical_extent = \
                [AltitudeCoordinate(
                    **{"value": float(json_obj['alt_va']),
                       "resolution": float(json_obj['alt_acy_va']),
                       "datum": json_obj['alt_datum_cd']})]

        return monitoring_feature


def _parse_sites_response(usgs_site_response, observed_properties_variables, unique_usgs_sites):
    """
    Get a dictionary of location variables for the given location results and
    get a dictionary of unique sites or subbasins

    :param usgs_site_response: datasource JSON object of the locations
    :return observed_properties_variables, unique_usgs_sites: a tuple of a dictionary of observed property variables for the given location results and a dictionary of unique sites or subbasins
    """

    for v in iter_rdb_to_json(usgs_site_response.text):
        param, site, stat = v['parm_cd'], v['site_no'], v['stat_cd']
        observed_properties_variables.setdefault(site, [])

        # FIX: stat -- need to change to B3D vocab
        if param not in observed_properties_variables[site] and stat != 'NOT_SUPPORTED':
            observed_properties_variables[site].append(param)
        if site not in unique_usgs_sites:
            unique_usgs_sites[site] = v
    logger.debug("Location DataTypes: {}".format(observed_properties_variables))

    return observed_properties_variables, unique_usgs_sites


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

            # Convert parent_features
            usgs_regions = []
            usgs_subbasins = []
            parent_features = []
            if query.parent_feature:
                for value in query.parent_feature:
                    parent_features.append(value)
                    if len(value) < 4:
                        usgs_regions.append(value)
                    elif len(value) == 8:
                        usgs_subbasins.append(value)

            if not feature_type or feature_type != FeatureTypeEnum.POINT:

                huc_text = self.get_hydrological_unit_codes(synthesis_messages=synthesis_messages)
                logger.debug(f"{self.__class__.__name__}.list url:{URL_USGS_HUC}")

                for json_obj in [o for o in iter_rdb_to_json(huc_text) if
                                 not parent_features or [p for p in parent_features if o["huc"].startswith(p)]]:

                    monitoring_feature = None
                    if (feature_type is None or feature_type == FeatureTypeEnum.REGION) and len(json_obj["huc"]) < 4:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypeEnum.REGION)

                    elif (feature_type is None or feature_type == FeatureTypeEnum.SUBREGION) and len(
                            json_obj["huc"]) == 4:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypeEnum.SUBREGION,
                                                                related_sampling_feature=json_obj["huc"][0:2],
                                                                related_sampling_feature_type=FeatureTypeEnum.REGION)

                    elif (feature_type is None or feature_type == FeatureTypeEnum.BASIN) and len(json_obj["huc"]) == 6:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypeEnum.BASIN,
                                                                related_sampling_feature=json_obj["huc"][0:4],
                                                                related_sampling_feature_type=FeatureTypeEnum.SUBREGION)

                    elif (feature_type is None or feature_type == FeatureTypeEnum.SUBBASIN) and len(
                            json_obj["huc"]) == 8:
                        hucs = {json_obj["huc"][0:i] for i in range(2, 8, 2)}

                        # Filter by regions if it is set
                        if not usgs_regions or not hucs.isdisjoint(usgs_regions):
                            # This is a Cataloging Unit (See https://water.usgs.gov/GIS/huc_name.html)
                            monitoring_feature = self._load_huc_obj(
                                json_obj=json_obj, feature_type=FeatureTypeEnum.SUBBASIN,
                                description="{} Watershed: Drainage basin code is defined by the USGS State "
                                            "Office where the site is located.".format(json_obj["basin"]),
                                related_sampling_feature=json_obj["huc"][0:6],
                                related_sampling_feature_type=FeatureTypeEnum.BASIN)

                    else:
                        logger.debug("Ignoring HUC {}".format(json_obj["huc"]))

                    # Determine whether to yield the monitoring feature object
                    if monitoring_feature:
                        if query.monitoring_feature and json_obj['huc'] in query.monitoring_feature:
                            yield monitoring_feature
                        elif not query.monitoring_feature:
                            yield monitoring_feature
            # no feature_type specified and/or feature_type == POINT
            else:
                base_url = '{}site/?{}&seriesCatalogOutput=true&outputDataTypeCd=iv,dv&siteStatus=all&format=rdb'

                # initiate variables for looping thru requests for multiple location filters
                unique_sites = {}  # type: ignore[var-annotated]
                observed_properties = {}  # type: ignore[var-annotated]
                loc_filters = []

                # Points by id: USGS calls these sites
                if query.monitoring_feature is not None:

                    # split up mf query types
                    mf_types = separate_list_types(query.monitoring_feature, {'named': str, 'bbox': tuple})
                    mf_named = mf_types.get('named', [])
                    mf_bbox = mf_types.get('bbox', [])
                    mf_bbox = [_translate_bbox(bbox_value) for bbox_value in mf_bbox]

                    if mf_named:
                        usgs_sites = ','.join(mf_named)
                        loc_filters.append(f'sites={usgs_sites}')

                    if mf_bbox:
                        bbox_coords = [_convert_tuple_to_str(bbox_tuple) for bbox_tuple in mf_bbox]
                        loc_filters.extend([f'bBox={bbox_cc}' for bbox_cc in bbox_coords])

                else:
                    # Point by subbasin: USGS calls subbasin as huc (instead of sites) to retrieve all subbasins
                    usgs_subbasin = ",".join(usgs_subbasins)
                    loc_filters.append(f'huc={usgs_subbasin}')
                    # url = base_url.format(self.datasource.location, f'huc={usgs_subbasin}')

                for a_filter in loc_filters:

                    url = base_url.format(self.datasource.location, a_filter)
                    logger.debug(f"{self.__class__.__name__}.list url:{url}")

                    usgs_site_response = get_url(url)

                    if usgs_site_response and usgs_site_response.status_code == 200:
                        observed_properties, unique_sites = _parse_sites_response(
                            usgs_site_response, observed_properties, unique_sites)
                    else:
                        msg = f'Problem with request to {url}'
                        if usgs_site_response:
                            msg = f'{msg}; {usgs_site_response.status_code}: {usgs_site_response.json}'
                        logger.warning(msg)
                        synthesis_messages.append(msg)

                if unique_sites:
                    for v in unique_sites.values():
                        yield _load_point_obj(datasource=self, json_obj=v,
                                              observed_property_variables=observed_properties,
                                              synthesis_messages=synthesis_messages)
        else:
            synthesis_messages.append(f"Feature type {feature_type} not supported by {self.datasource.name}.")
            logger.warning(f"Feature type {feature_type} not supported by {self.datasource.name}.")

        return StopIteration(synthesis_messages)

    def get_hydrological_unit_codes(self, synthesis_messages):
        """Get the hydrological unit codes for USGS"""

        try:
            response = get_url(URL_USGS_HUC, timeout=0.5)
            if response.status_code == 200:
                return response.text
            else:
                synthesis_messages.append(f"Get failed for {URL_USGS_HUC} - Failing over to stored HUC codes")
        except requests.exceptions.ReadTimeout:
            synthesis_messages.append(f"Read Timeout for {URL_USGS_HUC} - Failing over to stored HUC codes")
            logger.warning(f"Read Timeout for {URL_USGS_HUC} - Failing over to stored HUC codes")
        except requests.exceptions.ConnectTimeout:
            synthesis_messages.append(f"Connection Timeout for {URL_USGS_HUC} - Failing over to stored HUC codes")
            logger.warning(f"Connection Timeout for {URL_USGS_HUC} - Failing over to stored HUC codes")

        return usgs_huc_codes.CONTENT

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
            else:
                query.feature_type = FeatureTypeEnum.POINT

        query.monitoring_feature = [query.id]  # type: ignore[list-item]

        for o in self.list(query=query):
            return o

        # An 8 character code can also be a point, Try that
        if monitoring_feature_len == 8:
            query.feature_type = FeatureTypeEnum.POINT
            for o in self.list(query=query):
                return o
        return None

    def _load_huc_obj(self, json_obj, feature_type, description=None,
                      related_sampling_feature=None, related_sampling_feature_type=None):
        """
        Transform USGS huc information to a :class:`~basin3d.core.models.MonitoringFeature` object

        :param json_obj: USGS Site Service location object
        :return: a serialized :class:`~basin3d.core.models.MonitoringFeature` object
        """
        if not description:
            description = "{}: {}".format(feature_type, json_obj["basin"])

        related_sampling_feature_complex = list()
        if related_sampling_feature:
            related_sampling_feature_complex = [RelatedSamplingFeature(
                self,
                related_sampling_feature=related_sampling_feature,
                related_sampling_feature_type=related_sampling_feature_type,
                role=RelatedSamplingFeature.ROLE_PARENT
            )]

        result = None
        if json_obj:
            result = MonitoringFeature(
                self, id=json_obj["huc"],
                name=json_obj["basin"],
                description=description,
                feature_type=feature_type,
                shape=SpatialSamplingShapes.SHAPE_SURFACE,
                coordinates=None,
                related_sampling_feature_complex=related_sampling_feature_complex,
                observed_properties=None)
        return result


class USGSMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """
    USGS Daily Values Service: https://waterservices.usgs.gov/docs/dv-service/

    USGS Instantaneous Values Service: https://waterservices.usgs.gov/docs/instantaneous-values/

    Access for mapping USGS water services daily or instantaneous value data to
    :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` objects.

    Daily Value and Instantaneous Value Qualification Code (dv_rmk_cd)

    =============  =========  ================================================================================
    BASIN-3D Code  USGS Code  Description
    =============  =========  ================================================================================
    ESTIMATED      e          Value has been edited or estimated by USGS personnel and is write protected
    NOT_SUPPORTED  &          Value was computed from affected unit values
    ESTIMATED      E          Value was computed from estimated unit values.
    VALIDATED      A          Approved for publication -- Processing and review completed.
    UNVALIDATED    P          Provisional data subject to revision.
    NOT_SUPPORTED  <          The value is known to be less than reported value and is write protected.
    NOT_SUPPORTED  >          The value is known to be greater than reported value and is write protected.
    NOT_SUPPORTED  1          Value is write protected without any remark code to be printed
    NOT_SUPPORTED  2          Remark is write protected without any remark code to be printed
    NOT_SUPPORTED  _          No remark (blank)
    =============  =========  ================================================================================

    """

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """
        List of Measurement Timeseries TVP Observation objects for USGS Daily Values or Instantaneous Values

        :param query: :class:`basin3d.core.schema.query.QueryMeasurementTimeseriesTVP`
        :return: a generator object that yields :class:`~basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation` objects
        """
        synthesis_messages = []
        feature_obj_dict = {}
        if not query.monitoring_feature:
            return None

        loc_filters = []

        # split up mf query types
        mf_types = separate_list_types(query.monitoring_feature, {'named': str, 'bbox': tuple})
        mf_named = mf_types.get('named', [])
        mf_bbox = mf_types.get('bbox', [])
        mf_bbox = [_translate_bbox(bbox_value) for bbox_value in mf_bbox]

        if mf_named:
            usgs_sites = ','.join(mf_named)
            loc_filter = f'sites={usgs_sites}'
            if len(usgs_sites) < 3:
                loc_filter = f'huc={usgs_sites}'
            loc_filters.append(loc_filter)

        if mf_bbox:
            bbox_coords = [_convert_tuple_to_str(bbox_tuple) for bbox_tuple in mf_bbox]
            loc_filters.extend([f'bBox={bbox_cc}' for bbox_cc in bbox_coords])

        base_url = '{}site/?{}&seriesCatalogOutput=true&outputDataTypeCd=iv,dv&siteStatus=all&format=rdb'

        observed_properties = {}  # type: ignore[var-annotated]
        unique_sites = {}  # type: ignore[var-annotated]

        for a_filter in loc_filters:

            url = base_url.format(self.datasource.location, a_filter)
            logger.debug(f"{self.__class__.__name__}.list url:{url}")

            usgs_site_response = get_url(url)

            if usgs_site_response and usgs_site_response.status_code == 200:
                observed_properties, unique_sites = _parse_sites_response(
                    usgs_site_response, observed_properties, unique_sites)
            else:
                msg = f'Problem with request to {url}'
                if usgs_site_response:
                    msg = f'{msg}; {usgs_site_response.status_code}: {usgs_site_response.json}'
                logger.warning(msg)
                synthesis_messages.append(msg)

        if unique_sites:
            for v in unique_sites.values():
                if v["site_no"]:
                    feature_obj_dict[v["site_no"]] = v

        # Iterate over data objects returned
        processed_sites = []
        for data_json in generator_usgs_measurement_timeseries_tvp_observation(self, query, synthesis_messages):
            unit_of_measurement = data_json["variable"]["unit"]['unitCode']
            timezone_offset = data_json["sourceInfo"]["timeZoneInfo"]["defaultTimeZone"]["zoneOffset"]

            # name has agency, sitecode, parameter id and stat code
            #   e.g. "USGS:385106106571000:00060:00003"
            _, feature_id, parameter, statistic = data_json["name"].split(":")

            if feature_id in processed_sites:
                continue

            processed_sites.append(feature_id)

            if feature_id in feature_obj_dict.keys():
                monitoring_feature = _load_point_obj(
                    datasource=self,
                    json_obj=feature_obj_dict[feature_id],
                    observed_property_variables=observed_properties,
                    synthesis_messages=synthesis_messages)
            else:
                # ToDo: expand this to use the info in the data return
                # ToDo: log message
                monitoring_feature = None

            result_TVPs = []
            result_TVP_quality = []
            result_quality = set()
            has_filtered_data_points = 0

            if len(data_json["values"]) > 1:
                # Cannot think of the case in which response should have more than one values so adding a message to track it.
                synthesis_messages.append(
                    f'{feature_id} has more than one timeseries returned for {parameter}. Contact plugin developer if you see this message.')

            for values in data_json["values"]:
                # The original code checked what USGS reported for the qualities present in the timeseries. Instead will build from individual point qualities below.
                # result_quality.update(self.get_result_qualifiers(values["qualifier"]))

                for value in values["value"]:

                    if len(value['qualifiers']) > 1:
                        # ToDo: add some error messaging.
                        pass

                    # result_point_quality = self.map_result_quality(value['qualifiers'])
                    result_point_quality = value['qualifiers'][0]

                    if not query.result_quality or result_point_quality in query.result_quality:

                        # Get the broker parameter
                        try:
                            try:
                                data: Optional[float] = float(value['value'])
                                data_str = value['value']
                                # Hardcoded unit conversion for river discharge parameters
                                data, unit_of_measurement = convert_discharge(data, data_str, parameter, unit_of_measurement)

                                if data:
                                    result_quality.add(result_point_quality)
                                    result_TVPs.append(TimeValuePair(timestamp=value['dateTime'], value=data))
                                    result_TVP_quality.append(result_point_quality)

                                continue

                            except Exception as e:
                                synthesis_messages.append(f"Unit Conversion Issue: {str(e)}")
                                logger.error(str(e))

                        except Exception as e:
                            synthesis_messages.append(f"TimeValuePair ERROR: {str(e)}")
                            logger.error(e)

                    elif query.result_quality and result_point_quality not in query.result_quality:
                        has_filtered_data_points += 1

                if has_filtered_data_points > 0:
                    msg = f'{feature_id} - {parameter}: {str(has_filtered_data_points)} timestamps did not match data quality query.'
                    synthesis_messages.append(msg)

                if len(result_TVPs) == 0:
                    msg = f'{feature_id} had no valid data values for {parameter} that match the query.'
                    synthesis_messages.append(msg)
                    continue

            measurement_timeseries_tvp_observation = MeasurementTimeseriesTVPObservation(
                self,
                id=feature_id,  # FYI: this field is not unique and thus kinda useless
                unit_of_measurement=unit_of_measurement,
                feature_of_interest_type=FeatureTypeEnum.POINT,
                feature_of_interest=monitoring_feature,
                utc_offset=int(timezone_offset.split(":")[0]),
                result=ResultListTVP(plugin_access=self, value=result_TVPs, result_quality=result_TVP_quality),
                observed_property=parameter,
                result_quality=list(result_quality),
                aggregation_duration=query.aggregation_duration[0],
                time_reference_position=TimeMetadataMixin.TIME_REFERENCE_MIDDLE,
                statistic=statistic
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
        location = 'https://waterservices.usgs.gov/nwis/'
        id_prefix = 'USGS'
        name = 'USGS'  # Human Friendly Data Source Name
