"""

`basin3d.plugins.usgs`
****************************************

.. currentmodule:: basin3d.plugins.usgs

:platform: Unix, Mac
:synopsis: USGS Daily Values Plugin Definition and supporting views.
:module author: Val Hendrix <vhendrix@lbl.gov>


* :class:`USGSDataSourcePlugin` - This Data Source plugin maps the USGS Daily Values Data Source to the
    WFSFA broker REST API

USGS to WFSFA Broker Mapping
+++++++++++++++++++++++++++++++++
The table below describes how BASIN-3D synthesis models are mapped to the USGS Daily Values Source
models.

=================== === ============================================================
USGS NWIS              Broker
=================== === ============================================================
``nwis/dv``         >> :class:`basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation`
------------------- --- ------------------------------------------------------------
``nwis/sites``      >> :class:`basin3d.synthesis.models.field.MonitoringFeature`
------------------- --- ------------------------------------------------------------
``new_huc_rdb.txt`` >> :class:`basin3d.synthesis.models.field.MonitoringFeature`
 * Region to Region
 * Subregion to Subregion
 * Accounting Unit to Basin
 * Watershed to Subbasin
=================== === ============================================================


View Classes
++++++++++++

The following are the view classes that map *USGS Data Source API* to the *WFSFA
Broker REST API*.

* :class:`USGSMeasurementTimeseriesTVPObservationView` - View for accessing a group of data points grouped by time, space, model, sample  etc.
* :class:`USGSMonitoringFeatureView` - View for accessing monitoring features



---------------------
"""
import copy
import json
import logging
import requests

# Get an instance of a logger
from basin3d.core.access import get_url
from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, GeographicCoordinate, \
    MeasurementMetadataMixin, MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature, \
    TimeMetadataMixin, TimeValuePair
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin, get_feature_type
from basin3d.core.synthesis import QUERY_PARAM_END_DATE, QUERY_PARAM_MONITORING_FEATURES, \
    QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES, QUERY_PARAM_PARENT_FEATURES, QUERY_PARAM_RESULT_QUALITY, \
    QUERY_PARAM_START_DATE, QUERY_PARAM_FEATURE_TYPE
from basin3d.core.types import FeatureTypes, ResultQuality, SpatialSamplingShapes
from basin3d.plugins import usgs_huc_codes

logger = logging.getLogger(__name__)

URL_USGS_HUC = "https://water.usgs.gov/GIS/new_huc_rdb.txt"


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
    if stat_cd == '00001':
        return MeasurementMetadataMixin.STATISTIC_MAX
    if stat_cd == '00002':
        return MeasurementMetadataMixin.STATISTIC_MIN
    if stat_cd == '00003':
        return MeasurementMetadataMixin.STATISTIC_MEAN
    else:
        return 'NOT SUPPORTED'  # consider making this part of the Mixin Statistic type


def generator_usgs_measurement_timeseries_tvp_observation(view, **kwargs):
    """
    Get the Data Points for USGS Daily Values

    =================== === ===================
    USGS NWIS               Broker
    =================== === ===================
    ``nwis/dv``          >> ``data_points/``
    =================== === ===================

    :param view: The request object ( Please refer to the Django documentation ).
    :returns: a generator object that yields :class:`~basin3d.synthesis.models.field.DataPoint`
        objects
    """

    monitoring_features = kwargs[QUERY_PARAM_MONITORING_FEATURES]
    observed_property_variables = kwargs[QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES]

    # Temporal resolution is always daily.
    search_params = list()

    if QUERY_PARAM_START_DATE in kwargs:
        search_params.append(("startDT", kwargs[QUERY_PARAM_START_DATE]))

    if QUERY_PARAM_END_DATE in kwargs:
        search_params.append(("endDT", kwargs[QUERY_PARAM_END_DATE]))

    search_params.append(("parameterCd", ",".join([str(o) for o in observed_property_variables])))

    if len(monitoring_features[0]) > 2:
        # search for stations
        search_params.append(("statCd", "00003"))  # Only search for the mean statistic for now
        search_params.append(("sites", ",".join(monitoring_features)))
    else:
        # search for stations by specifying the huc
        search_params.append(("huc", ",".join(monitoring_features)))
        search_params.append(("siteStatus", "all"))  # per Charu's query

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

            logger.error("JSON Not Returned: {}".format(response.content))
    else:
        import re
        p = re.compile(r'<.*?>')
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


def _load_point_obj(datasource, json_obj, feature_observed_properties, observed_property_variables=None):
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
            logger.error(str(e))

        monitoring_feature = MonitoringFeature(
            datasource,
            id="{}".format(id),
            name=json_obj['station_nm'],
            feature_type=FeatureTypes.POINT,
            shape=SpatialSamplingShapes.SHAPE_POINT,
            related_sampling_feature_complex=[RelatedSamplingFeature(
                datasource,
                related_sampling_feature=json_obj['huc_cd'],
                related_sampling_feature_type=FeatureTypes.SUBBASIN,  # previously site
                role=RelatedSamplingFeature.ROLE_PARENT
            )],
            # geographical_group_id=huc_accounting_unit_id,
            # geographical_group_type=FeatureTypes.REGION,
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
    View for mapping USGS HUC Regions, SubRegions and Accounting Units to
    :class:`~basin3d.synthesis.models.field.Region` objects.

    ============== === ====================================================
    USGS NWIS          Broker
    ============== === ====================================================
    HUCs            >> :class:`basin3d.synthesis.models.field.Region`
    ============== === ====================================================

    """
    synthesis_model_class = MonitoringFeature

    def list(self, **kwargs):
        """
        Get the Regions

        =================== === ===================
        USGS NWIS               Broker
        =================== === ===================
        ``new_huc_rdb.txt``  >> ``MonitoringFeature/``
        =================== === ===================

        :param request: The request object ( Please refer to the Django documentation ).
        :returns: a generator object that yields :class:`~basin3d.synthesis.models.field.MonitoringFeature`
            objects
        """

        feature_type = get_feature_type(feature_type="feature_type" in kwargs and kwargs['feature_type'] or None)

        supported_feature_types = [None]
        for k, ft in FeatureTypes.TYPES.items():
            if ft in USGSDataSourcePlugin.feature_types:
                supported_feature_types.append(k)

        if feature_type in supported_feature_types:

            pk = None
            if "pk" in kwargs.keys():
                pk = kwargs["pk"]

            # Convert parent_features
            usgs_regions = []
            usgs_subbasins = []
            parent_features = []
            if QUERY_PARAM_PARENT_FEATURES in kwargs.keys() and kwargs[QUERY_PARAM_PARENT_FEATURES]:
                for value in kwargs[QUERY_PARAM_PARENT_FEATURES]:
                    parent_features.append(value)
                    if len(value) < 4:
                        usgs_regions.append(value)
                    elif len(value) == 8:
                        usgs_subbasins.append(value)

            if not feature_type or feature_type != FeatureTypes.POINT:

                huc_text = self.get_hydrological_unit_codes()
                logging.debug("{}.{}".format(self.__class__.__name__, "list"),
                              url=URL_USGS_HUC)

                for json_obj in [o for o in iter_rdb_to_json(huc_text) if not parent_features or [p for p in parent_features if o["huc"].startswith(p)]]:

                    if (feature_type is None or feature_type == FeatureTypes.REGION) and len(json_obj["huc"]) < 4:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypes.REGION)
                        if monitoring_feature and (not pk or pk == json_obj["huc"]):
                            yield monitoring_feature
                            if pk:
                                break

                    elif (feature_type is None or feature_type == FeatureTypes.SUBREGION) and len(json_obj["huc"]) == 4:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypes.SUBREGION,
                                                                related_sampling_feature=json_obj["huc"][0:2],
                                                                related_sampling_feature_type=FeatureTypes.REGION)
                        if monitoring_feature and (not pk or pk == json_obj["huc"]):
                            yield monitoring_feature
                            if pk:
                                break
                    elif (feature_type is None or feature_type == FeatureTypes.BASIN) and len(json_obj["huc"]) == 6:
                        monitoring_feature = self._load_huc_obj(json_obj, feature_type=FeatureTypes.BASIN,
                                                                related_sampling_feature=json_obj["huc"][0:4],
                                                                related_sampling_feature_type=FeatureTypes.SUBREGION)
                        if monitoring_feature and (not pk or pk == json_obj["huc"]):
                            yield monitoring_feature
                            if pk:
                                break
                    elif (feature_type is None or feature_type == FeatureTypes.SUBBASIN) and len(json_obj["huc"]) == 8:
                        hucs = {json_obj["huc"][0:i] for i in range(2, 8, 2)}

                        # Filter by regions if it is set
                        if not usgs_regions or not hucs.isdisjoint(usgs_regions):

                            # This is a Cataloging Unit (See https://water.usgs.gov/GIS/huc_name.html)
                            monitoring_feature = self._load_huc_obj(
                                json_obj=json_obj, feature_type=FeatureTypes.SUBBASIN,
                                description="{} Watershed: Drainage basin code is defined by the USGS State "
                                            "Office where the site is located.".format(json_obj["basin"]),
                                related_sampling_feature=json_obj["huc"][0:6],
                                related_sampling_feature_type=FeatureTypes.BASIN)
                            if monitoring_feature and (not pk or pk == json_obj["huc"]):
                                yield monitoring_feature
                                if pk:
                                    break
                    else:
                        logger.debug("Ignoring HUC {}".format(json_obj["huc"]))

            # points: USGS calls these sites
            else:
                if pk:
                    usgs_sites = pk
                    feature_observed_properties = self.get_observed_properties_variables([pk])
                else:
                    # Get the variables with data
                    feature_observed_properties = self.get_observed_properties_variables(usgs_subbasins)
                    usgs_sites = ",".join(feature_observed_properties.keys())

                # Filter by locations with data
                url = '{}site/?sites={}'.format(self.datasource.location, usgs_sites)
                usgs_site_response = get_url(url)
                logging.debug("{}.{}".format(self.__class__.__name__, "list"), url=url)

                if usgs_site_response and usgs_site_response.status_code == 200:

                    for v in iter_rdb_to_json(usgs_site_response.text):
                        yield _load_point_obj(datasource=self, json_obj=v,
                                              feature_observed_properties=feature_observed_properties)

        else:
            logger.warning("Feature type {} not supported by {}.".format(feature_type and FeatureTypes.TYPES[feature_type],
                                                                         self.datasource.name or feature_type))

    def get_hydrological_unit_codes(self):
        """Get the hydrological unit codes for USGS"""

        try:
            response = get_url(URL_USGS_HUC, timeout=0.5)
            if response.status_code == 200:
                return response.text
        except requests.exceptions.ReadTimeout:
            logger.warning(f"Read Timeout for {URL_USGS_HUC} - Failing over to stored HUC codes")
        except requests.exceptions.ConnectTimeout:
            logger.warning(f"Connection Timeout for {URL_USGS_HUC} - Failing over to stored HUC codes")

        return usgs_huc_codes.CONTENT

    def get(self, pk=None, **kwargs):
        """ Get a single Region object

        ===================== === =====================
        USGS NWIS                 Broker
        ===================== === =====================
        ``{huc}``              >>  ``Primary key (pk)``
        --------------------- --- ---------------------
        ``new_huc_rdb.txt``    >>  ``monitoringfeatures/<feature_type>/{pk}``
        ===================== === =====================

        :param request: The request object ( Please refer to the Django documentation )
        :param pk: USGS Daily Values Plugin primary key
        :return: a serialized ``MonitoringFeature`` object
        """
        feature_type = None
        if QUERY_PARAM_FEATURE_TYPE in kwargs and kwargs[QUERY_PARAM_FEATURE_TYPE]:
            feature_type = kwargs[QUERY_PARAM_FEATURE_TYPE]
        else:
            if len(pk) == 2:
                feature_type = FeatureTypes.TYPES[FeatureTypes.REGION]
            elif len(pk) == 4:
                feature_type = FeatureTypes.TYPES[FeatureTypes.SUBREGION]
            elif len(pk) == 6:
                feature_type = FeatureTypes.TYPES[FeatureTypes.BASIN]
            elif len(pk) == 8:
                feature_type = FeatureTypes.TYPES[FeatureTypes.SUBBASIN]

        for o in self.list(pk=pk, feature_type=feature_type):
            return o
        return None

    def _load_huc_obj(self, json_obj, feature_type, description=None,
                      related_sampling_feature=None, related_sampling_feature_type=None):
        """
        Serialize an USGS Daily Values Data Source City object into a :class:`~basin3d.synthesis.models.field.Region` object

        ============== === =================
        USGS NWIS          Broker
        ============== === =================
        ``{huc}``  >>      ``MonitoringFeature.id``
        ============== === =================

        :param json_obj: USGS Daily Values Data Source Region object
        :type json_obj: dict
        :return: a serialized :class:`~basin3d.synthesis.models.field.MonitoringFeature` object
        :rtype: :class:`basin3d.synthesis.models.field.MonitoringFeature`
        """
        if not description:
            description = "{}: {}".format(FeatureTypes.TYPES[feature_type], json_obj["basin"])

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

    View for mapping USGS water services daily value data to
    :class:`~basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation` objects.

    ============== === =========================================================
    USGS NWIS          Broker
    ============== === =========================================================
    Daily Values    >> :class:`basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation`
    ============== === =========================================================
    """

    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def result_quality(self, qualifiers):
        """
        Daily Value Qualification Code (dv_rmk_cd)
        ---------------------------------
        Code  Description
        ---------------------------------
        e     Value has been edited or estimated by USGS personnel and is write protected
        &     Value was computed from affected unit values
        E     Value was computed from estimated unit values.
        A     Approved for publication -- Processing and review completed.
        P     Provisional data subject to revision.
        <     The value is known to be less than reported value and is write protected.
        >     The value is known to be greater than reported value and is write protected.
        1     Value is write protected without any remark code to be printed
        2     Remark is write protected without any remark code to be printed
              No remark (blank)
        :param qualifiers:
        :return:
        """
        if "A" in qualifiers:
            return ResultQuality.RESULT_QUALITY_CHECKED
        elif "P" in qualifiers:
            return ResultQuality.RESULT_QUALITY_UNCHECKED
        else:
            return 'NOT_SET'

    def get_result_qualifiers(self, qualifiers):
        timeseries_qualifiers = set()
        for qualifier in qualifiers:
            timeseries_qualifiers.add(self.result_quality(qualifier["qualifierCode"]))
        return timeseries_qualifiers

    def list(self, **kwargs):
        """
        Get the Data Points for USGS Daily Values

        =================== === ======================
        USGS NWIS               Broker
        =================== === ======================
        ``nwis/dv``          >> ``measurement_tvp_timeseries/``
        =================== === ======================

        :returns: a generator object that yields :class:`~basin3d.synthesis.models.measurement.MeasurementTimeseriesTVPObservation`
            objects
        """
        search_params = ""
        feature_obj_dict = {}
        if QUERY_PARAM_MONITORING_FEATURES not in kwargs:
            return None

        search_params = ",".join(kwargs[QUERY_PARAM_MONITORING_FEATURES])

        url = '{}site/?sites={}'.format(self.datasource.location, search_params)

        if len(search_params) < 3:
            url = '{}site/?huc={}'.format(self.datasource.location, search_params)

        usgs_site_response = None
        try:
            usgs_site_response = get_url(url)
            logging.debug("{}.{}".format(self.__class__.__name__, "list"), url=url)
        except Exception as e:
            logging.warning("Could not connect to USGS site info: {}".format(e))

        if usgs_site_response:
            for v in iter_rdb_to_json(usgs_site_response.text):
                if v["site_no"]:
                    feature_obj_dict[v["site_no"]] = v

        # Iterate over data objects returned
        for data_json in generator_usgs_measurement_timeseries_tvp_observation(self, **kwargs):
            unit_of_measurement = data_json["variable"]["unit"]['unitCode']
            timezone_offset = data_json["sourceInfo"]["timeZoneInfo"]["defaultTimeZone"]["zoneOffset"]

            # name has agency, sitecode, parameter id and stat code
            #   e.g. "USGS:385106106571000:00060:00003"
            _, feature_id, parameter, statistic = data_json["name"].split(":")

            if feature_id in feature_obj_dict.keys():
                monitoring_feature = _load_point_obj(
                    datasource=self,
                    json_obj=feature_obj_dict[feature_id], feature_observed_properties=dict(),
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
            # result_quality = None
            result_quality_filter = None
            result_qualifiers = set()
            timeseries_result_quality = None

            if QUERY_PARAM_RESULT_QUALITY in kwargs:
                result_quality_filter = QUERY_PARAM_RESULT_QUALITY

            for values in data_json["values"]:
                result_qualifiers.update(self.get_result_qualifiers(values["qualifier"]))

                for value in values["value"]:

                    # VAL: with result quality here, the quality of last value in the timeseries will be used.
                    #      Is the value the same throughout the time series?
                    result_quality = self.result_quality(value['qualifiers'])

                    # TODO: write some tests for this which will require mocking a data return.
                    # Only filter if quality_checked is True
                    # if QUERY_PARAM_RESULT_QUALITY not in kwargs or not kwargs[QUERY_PARAM_RESULT_QUALITY] or \
                            # (QUERY_PARAM_RESULT_QUALITY in kwargs and kwargs[
                                # QUERY_PARAM_RESULT_QUALITY] == result_quality):
                    if not result_quality_filter or result_quality_filter == result_quality:

                        # Get the broker parameter
                        try:
                            try:
                                data = float(value['value'])
                                # Hardcoded unit conversion for river discharge parameters
                                data, unit_of_measurement = convert_discharge(data, parameter, unit_of_measurement)
                            except Exception as e:
                                logger.error(str(e))
                                data = None
                            # What do do with bad values?

                            result_points.append(TimeValuePair(timestamp=value['dateTime'], value=data))

                        except Exception as e:
                            logger.error(e)

            timeseries_result_quality = result_quality_filter
            if not timeseries_result_quality:
                if ResultQuality.RESULT_QUALITY_CHECKED in result_qualifiers:
                    timeseries_result_quality = ResultQuality.RESULT_QUALITY_CHECKED
                if ResultQuality.RESULT_QUALITY_UNCHECKED in result_qualifiers:
                    timeseries_result_quality = ResultQuality.RESULT_QUALITY_UNCHECKED
                if (ResultQuality.RESULT_QUALITY_UNCHECKED in result_qualifiers and
                        ResultQuality.RESULT_QUALITY_CHECKED in result_qualifiers):
                    timeseries_result_quality = ResultQuality.RESULT_QUALITY_PARTIALLY_CHECKED

            measurement_timeseries_tvp_observation = MeasurementTimeseriesTVPObservation(
                self,
                id=feature_id,  # FYI: this field is not unique and thus kinda useless
                unit_of_measurement=unit_of_measurement,
                feature_of_interest_type=FeatureTypes.POINT,
                feature_of_interest=monitoring_feature,
                utc_offset=int(timezone_offset.split(":")[0]),
                result_points=result_points,
                observed_property_variable=parameter,
                result_quality=timeseries_result_quality,
                aggregation_duration=kwargs["aggregation_duration"],
                time_reference_position=TimeMetadataMixin.TIME_REFERENCE_MIDDLE,
                statistic=basin3d_statistic
            )

            yield measurement_timeseries_tvp_observation


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
