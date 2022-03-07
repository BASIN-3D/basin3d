"""

`basin3d.plugins.usgs`
**********************

.. currentmodule:: basin3d.plugins.usgs

:platform: Unix, Mac
:synopsis: USGS Daily Values Plugin Definition and supporting views.
:module author: Val Hendrix <vhendrix@lbl.gov>


* :class:`USGSDataSourcePlugin` - This Data Source plugin maps the USGS Daily Values Data Source to the
    BASIN-3D Models

USGS to BASIN-3D Mapping
++++++++++++++++++++++++
The table below describes how BASIN-3D synthesis models are mapped to the USGS Daily Values Source
models.

=================== === ==================================================================================
USGS NWIS               BASIN-3D
=================== === ==================================================================================
``nwis/dv``         >>  :class:`basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation`
------------------- --- ----------------------------------------------------------------------------------
``nwis/sites``      >>  :class:`basin3d.synthesis.models.field.MonitoringFeature`
------------------- --- ----------------------------------------------------------------------------------
``new_huc_rdb.txt`` >>  :class:`basin3d.synthesis.models.field.MonitoringFeature`
                         * Region to Region
                         * Subregion to Subregion
                         * Accounting Unit to Basin
                         * Watershed to Subbasin
=================== === ==================================================================================


Access Classes
++++++++++++++

The following are the access classes that map *USGS Data Source API* to the *BASIN-3D Models*.

* :class:`USGSMeasurementTimeseriesTVPObservationAccess` - Access for accessing a group of data points grouped by time, space, model, sample  etc.
* :class:`USGSMonitoringFeatureAccess` - Access for accessing monitoring features



---------------------
"""
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

# Get an instance of a logger
from basin3d.core.schema.query import FeatureTypeEnum, QueryById, QueryMeasurementTimeseriesTVP, QueryMonitoringFeature, \
    ResultQualityEnum
from basin3d.core.access import get_url
from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, GeographicCoordinate, \
    MeasurementMetadataMixin, MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature, \
    TimeMetadataMixin, TimeValuePair
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin
from basin3d.core.types import SpatialSamplingShapes
from basin3d.plugins import usgs_huc_codes

logger = logging.getLogger(__name__)

URL_USGS_HUC = "https://water.usgs.gov/GIS/new_huc_rdb.txt"
USGS_STATISTIC_MAP: Dict = {
    MeasurementMetadataMixin.STATISTIC_MEAN: '00003',
    MeasurementMetadataMixin.STATISTIC_MIN: '00002',
    MeasurementMetadataMixin.STATISTIC_MAX: '00001'
}


def convert_discharge(data, parameter, units):
    """
    Convert the River Discharge to m^3
    :param data:
    :param parameter:
    :param units:
    :return:
    """
    if parameter in ['00060', '00061']:
        # Hardcode conversion from ft^3 to m^3
        # for River discharge
        data *= 0.028316847
        units = "m^3/s"
    return data, units


def map_statistic_code(stat_cd):
    for k, v in USGS_STATISTIC_MAP.items():
        if stat_cd == v:
            return k
    return 'NOT SUPPORTED'  # consider making this part of the Mixin Statistic type


def generator_usgs_measurement_timeseries_tvp_observation(view,
                                                          query: QueryMeasurementTimeseriesTVP,
                                                          synthesis_messages):
    """
    Get the Data Points for USGS Daily Values

    =================== === ===================
    USGS NWIS               BASIN-3D
    =================== === ===================
    ``nwis/dv``          >> ``data_points/``
    =================== === ===================

    :param view: The request object ( Please refer to the Django documentation ).
    :param query: Query information for this request
    :returns: a generator object that yields :class:`~basin3d.synthesis.models.field.DataPoint`
        objects
    """

    # Temporal resolution is always daily.
    search_params: List[Tuple[str, Any]] = list()

    search_params.append(("startDT", query.start_date))

    if query.end_date:
        search_params.append(("endDT", query.end_date))

    search_params.append(("parameterCd", ",".join([str(o) for o in query.observed_property_variables])))

    if query.statistic:
        statistics: List[str] = []
        for stat in query.statistic:
            sythesized_stat = USGS_STATISTIC_MAP.get(stat)
            if not sythesized_stat:
                synthesis_messages.append(f"USGS Daily Values service does not support statistic {stat}")
                logger.info(f"USGS Daily Values service does not support statistic {stat}")
            else:
                statistics.append(sythesized_stat)
        search_params.append(("statCd", ",".join(statistics)))
    else:
        search_params.append(("siteStatus", "all"))

    if len(query.monitoring_features[0]) > 2:
        # search for stations
        search_params.append(("sites", ",".join(query.monitoring_features)))
    else:
        # search for stations by specifying the huc
        search_params.append(("huc", ",".join(query.monitoring_features)))

    # look for station locations only
    search_params.append(("siteType", "ST"))

    # JSON format
    search_params.append(("format", "json"))

    # Request the data points
    response = get_url('{}dv'.format(view.datasource.location),
                       params=search_params)

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


def _load_point_obj(datasource, json_obj, feature_observed_properties, synthesis_messages,
                    observed_property_variables=None):
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
    :param feature_observed_properties: dictionary of locations and their available variables
    :type feature_observed_properties: dict
    :return:
    """

    if 'site_no' in json_obj:
        id = json_obj['site_no']

        # Get the location variables from the dictionary
        if observed_property_variables is None:
            if id in feature_observed_properties.keys():
                observed_property_variables = feature_observed_properties[id]

        lat, lon = None, None
        try:
            lat, lon = float(json_obj['dec_lat_va']), float(json_obj['dec_long_va'])
        except Exception as e:
            synthesis_messages.append(f"Error getting latlon: {str(e)}")
            logger.error(str(e))

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
            observed_property_variables=observed_property_variables,
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


class USGSMonitoringFeatureAccess(DataSourcePluginAccess):
    """
    Access for mapping USGS HUC Regions, SubRegions and Accounting Units to
    :class:`~basin3d.synthesis.models.field.Region` objects.

    ============== === ====================================================
    USGS NWIS          BASIN-3D
    ============== === ====================================================
    HUCs            >> :class:`basin3d.synthesis.models.field.Region`
    ============== === ====================================================

    """
    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature):
        """
        Get the Regions

        =================== === ===================
        USGS NWIS               BASIN-3D
        =================== === ===================
        ``new_huc_rdb.txt``  >> ``MonitoringFeature/``
        =================== === ===================

        :param query: The query information object
        :returns: a generator object that yields :class:`~basin3d.synthesis.models.field.MonitoringFeature`
            objects
        """
        synthesis_messages: List[str] = []

        feature_type = isinstance(query.feature_type, FeatureTypeEnum) and query.feature_type.value or query.feature_type
        if feature_type in USGSDataSourcePlugin.feature_types or feature_type is None:

            # Convert parent_features
            usgs_regions = []
            usgs_subbasins = []
            parent_features = []
            if query.parent_features:
                for value in query.parent_features:
                    parent_features.append(value)
                    if len(value) < 4:
                        usgs_regions.append(value)
                    elif len(value) == 8:
                        usgs_subbasins.append(value)

            if not feature_type or feature_type != FeatureTypeEnum.POINT:

                huc_text = self.get_hydrological_unit_codes(synthesis_messages=synthesis_messages)
                logging.debug(f"{self.__class__.__name__}.list url:{URL_USGS_HUC}")

                for json_obj in [o for o in iter_rdb_to_json(huc_text) if not parent_features or [p for p in parent_features if o["huc"].startswith(p)]]:

                    monitoring_feature = None
                    if (feature_type is None or feature_type == FeatureTypeEnum.REGION) and len(json_obj["huc"]) < 4:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypeEnum.REGION)

                    elif (feature_type is None or feature_type == FeatureTypeEnum.SUBREGION) and len(json_obj["huc"]) == 4:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypeEnum.SUBREGION,
                                                                related_sampling_feature=json_obj["huc"][0:2],
                                                                related_sampling_feature_type=FeatureTypeEnum.REGION)

                    elif (feature_type is None or feature_type == FeatureTypeEnum.BASIN) and len(json_obj["huc"]) == 6:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypeEnum.BASIN,
                                                                related_sampling_feature=json_obj["huc"][0:4],
                                                                related_sampling_feature_type=FeatureTypeEnum.SUBREGION)

                    elif (feature_type is None or feature_type == FeatureTypeEnum.SUBBASIN) and len(json_obj["huc"]) == 8:
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
                        if query.monitoring_features and json_obj['huc'] in query.monitoring_features:
                            yield monitoring_feature
                        elif not query.monitoring_features:
                            yield monitoring_feature

            # points: USGS calls these sites
            else:
                if query.monitoring_features:
                    usgs_sites = ",".join(query.monitoring_features)
                    feature_observed_properties = self.get_observed_properties_variables(query.monitoring_features)
                else:
                    # Get the variables with data
                    feature_observed_properties = self.get_observed_properties_variables(usgs_subbasins)
                    usgs_sites = ",".join(feature_observed_properties.keys())

                # Filter by locations with data
                url = '{}site/?sites={}'.format(self.datasource.location, usgs_sites)
                usgs_site_response = get_url(url)
                logging.debug(f"{self.__class__.__name__}.list url:{url}")

                if usgs_site_response and usgs_site_response.status_code == 200:

                    for v in iter_rdb_to_json(usgs_site_response.text):
                        yield _load_point_obj(datasource=self, json_obj=v, synthesis_messages=synthesis_messages,
                                              feature_observed_properties=feature_observed_properties)

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

    def get(self, query: QueryById):
        """ Get a single Region object

        ===================== === =====================
        USGS NWIS                 BASIN-3D
        ===================== === =====================
        ``{huc}``              >>  ``Primary key (query.id)``
        --------------------- --- ---------------------
        ``new_huc_rdb.txt``    >>  ``monitoringfeatures/<feature_type>/{query.id}``
        ===================== === =====================

        :param query: The query info object
        :return: a serialized ``MonitoringFeature`` object
        """

        if len(query.id) == 2:
            mf_query = QueryMonitoringFeature(monitoring_features=[query.id], feature_type=FeatureTypeEnum.REGION)
        elif len(query.id) == 4:
            mf_query = QueryMonitoringFeature(monitoring_features=[query.id], feature_type=FeatureTypeEnum.SUBREGION)
        elif len(query.id) == 6:
            mf_query = QueryMonitoringFeature(monitoring_features=[query.id], feature_type=FeatureTypeEnum.BASIN)
        elif len(query.id) == 8:
            mf_query = QueryMonitoringFeature(monitoring_features=[query.id], feature_type=FeatureTypeEnum.SUBBASIN)
        else:
            mf_query = QueryMonitoringFeature(monitoring_features=[query.id], feature_type=FeatureTypeEnum.POINT)

        for o in self.list(query=mf_query):
            return o

        # An 8 character code can also be a point, Try that
        if len(query.id) == 8:
            for o in self.list(query=QueryMonitoringFeature(monitoring_features=[query.id],
                                                            feature_type=FeatureTypeEnum.POINT)):
                return o
        return None

    def _load_huc_obj(self, json_obj, feature_type, description=None,
                      related_sampling_feature=None, related_sampling_feature_type=None):
        """
        Serialize an USGS Daily Values Data Source City object into a :class:`~basin3d.synthesis.models.field.Region` object

        ============== === =================
        USGS NWIS          BASIN-3D
        ============== === =================
        ``{huc}``  >>      ``MonitoringFeature.id``
        ============== === =================

        :param json_obj: USGS Daily Values Data Source Region object
        :type json_obj: dict
        :return: a serialized :class:`~basin3d.synthesis.models.field.MonitoringFeature` object
        :rtype: :class:`basin3d.synthesis.models.field.MonitoringFeature`
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
                observed_property_variables=None)
        return result

    def get_observed_properties_variables(self, usgs_sites):
        """
        Get a dictionary of location variables for the given location results
        :param usgs_sites: datasource JSON object of the locations
        :return:
        """

        # Gather the location ids and get the parameters available
        # Only search for the mean data statCd=00003.
        url = '{}dv?huc={}&format=json&statCd=00003'.format(self.datasource.location, ",".join(usgs_sites))
        response_variables = get_url(url)
        observed_properties_variables = {}
        if response_variables and response_variables.status_code == 200:
            for location in response_variables.json()['value']['timeSeries']:
                _, location_id, parameter, statistic = location['name'].split(":")
                # We only want the mean
                observed_properties_variables.setdefault(location_id, [])
                observed_properties_variables[location_id].append(parameter)
            logging.debug("Location DataTypes: {}".format(observed_properties_variables))
        return observed_properties_variables


class USGSMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """
    https://waterservices.usgs.gov/rest/DV-Service.html

    Access for mapping USGS water services daily value data to
    :class:`~basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation` objects.

    ============== === =========================================================
    USGS NWIS          BASIN-3D
    ============== === =========================================================
    Daily Values    >> :class:`basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation`
    ============== === =========================================================
    """

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def result_quality(self, qualifiers):
        """
        Daily Value Qualification Code (dv_rmk_cd)

        ====  ================================================================================
        Code  Description
        ====  ================================================================================
        e     Value has been edited or estimated by USGS personnel and is write protected
        &     Value was computed from affected unit values
        E     Value was computed from estimated unit values.
        A     Approved for publication -- Processing and review completed.
        P     Provisional data subject to revision.
        <     The value is known to be less than reported value and is write protected.
        >     The value is known to be greater than reported value and is write protected.
        1     Value is write protected without any remark code to be printed
        2     Remark is write protected without any remark code to be printed
        _     No remark (blank)
        ====  ================================================================================

        :param qualifiers:
        :return:
        """
        if "A" in qualifiers:
            return ResultQualityEnum.CHECKED
        elif "P" in qualifiers:
            return ResultQualityEnum.UNCHECKED
        else:
            return 'NOT_SET'

    def get_result_qualifiers(self, qualifiers):
        timeseries_qualifiers = set()
        for qualifier in qualifiers:
            timeseries_qualifiers.add(self.result_quality(qualifier["qualifierCode"]))
        return timeseries_qualifiers

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """
        Get the Data Points for USGS Daily Values

        =================== === ======================
        USGS NWIS               BASIN-3D
        =================== === ======================
        ``nwis/dv``          >> ``measurement_tvp_timeseries/``
        =================== === ======================

        :returns: a generator object that yields :class:`~basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation`
            objects
        """
        synthesis_messages = []
        feature_obj_dict = {}
        if not query.monitoring_features:
            return None

        search_params = ",".join(query.monitoring_features)

        url = '{}site/?sites={}'.format(self.datasource.location, search_params)

        if len(search_params) < 3:
            url = '{}site/?huc={}'.format(self.datasource.location, search_params)

        usgs_site_response = None
        try:
            usgs_site_response = get_url(url)
            logging.debug(f"{self.__class__.__name__}.list url:{url}")
        except Exception as e:
            synthesis_messages.append("Could not connect to USGS site info: {}".format(e))
            logging.warning("Could not connect to USGS site info: {}".format(e))

        if usgs_site_response:
            for v in iter_rdb_to_json(usgs_site_response.text):
                if v["site_no"]:
                    feature_obj_dict[v["site_no"]] = v

        # Iterate over data objects returned
        for data_json in generator_usgs_measurement_timeseries_tvp_observation(self, query, synthesis_messages):
            unit_of_measurement = data_json["variable"]["unit"]['unitCode']
            timezone_offset = data_json["sourceInfo"]["timeZoneInfo"]["defaultTimeZone"]["zoneOffset"]

            # name has agency, sitecode, parameter id and stat code
            #   e.g. "USGS:385106106571000:00060:00003"
            _, feature_id, parameter, statistic = data_json["name"].split(":")

            if feature_id in feature_obj_dict.keys():
                monitoring_feature = _load_point_obj(
                    datasource=self,
                    json_obj=feature_obj_dict[feature_id], feature_observed_properties=dict(),
                    synthesis_messages=synthesis_messages,
                    observed_property_variables="Find observed property variables at monitoring feature url")
            else:
                # ToDo: expand this to use the info in the data return
                # ToDo: log message
                monitoring_feature = None

            # deal with statistic
            basin3d_statistic = "NOT SET"
            if statistic:
                basin3d_statistic = map_statistic_code(statistic)

            result_points = []
            result_qualifiers = set()

            for values in data_json["values"]:
                result_qualifiers.update(self.get_result_qualifiers(values["qualifier"]))

                for value in values["value"]:

                    # VAL: with result quality here, the quality of last value in the timeseries will be used.
                    #      Is the value the same throughout the time series?
                    result_quality = self.result_quality(value['qualifiers'])

                    # TODO: write some tests for this which will require mocking a data return.
                    # Only filter if quality_checked is True
                    # if QUERY_PARAM_RESULT_QUALITY not in kwargs or not kwargs[QUERY_PARAM_RESULT_QUALITY] or \
                    #         (QUERY_PARAM_RESULT_QUALITY in kwargs and kwargs[
                    #          QUERY_PARAM_RESULT_QUALITY] == result_quality):
                    if not query.result_quality or query.result_quality == result_quality:

                        # Get the broker parameter
                        try:
                            try:
                                data: Optional[float] = float(value['value'])
                                # Hardcoded unit conversion for river discharge parameters
                                data, unit_of_measurement = convert_discharge(data, parameter, unit_of_measurement)
                            except Exception as e:
                                synthesis_messages.append(f"Unit Conversion Issue: {str(e)}")
                                logger.error(str(e))
                                data = None
                            # What do do with bad values?

                            result_points.append(TimeValuePair(timestamp=value['dateTime'], value=data))

                        except Exception as e:
                            synthesis_messages.append(f"TimeValuePair ERROR: {str(e)}")
                            logger.error(e)

            timeseries_result_quality = query.result_quality
            if not timeseries_result_quality:
                if ResultQualityEnum.CHECKED in result_qualifiers:
                    timeseries_result_quality = ResultQualityEnum.CHECKED
                if ResultQualityEnum.UNCHECKED in result_qualifiers:
                    timeseries_result_quality = ResultQualityEnum.UNCHECKED
                if (ResultQualityEnum.PARTIALLY_CHECKED in result_qualifiers and ResultQualityEnum.CHECKED in result_qualifiers):
                    timeseries_result_quality = ResultQualityEnum.PARTIALLY_CHECKED

            measurement_timeseries_tvp_observation = MeasurementTimeseriesTVPObservation(
                self,
                id=feature_id,  # FYI: this field is not unique and thus kinda useless
                unit_of_measurement=unit_of_measurement,
                feature_of_interest_type=FeatureTypeEnum.POINT,
                feature_of_interest=monitoring_feature,
                utc_offset=int(timezone_offset.split(":")[0]),
                result_points=result_points,
                observed_property_variable=parameter,
                result_quality=timeseries_result_quality,
                aggregation_duration=query.aggregation_duration,
                time_reference_position=TimeMetadataMixin.TIME_REFERENCE_MIDDLE,
                statistic=basin3d_statistic
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
        # Data Source attributes
        id = 'USGS'  # unique id for the datasource
        location = 'https://waterservices.usgs.gov/nwis/'
        id_prefix = 'USGS'
        name = 'USGS'  # Human Friendly Data Source Name
