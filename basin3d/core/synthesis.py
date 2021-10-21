"""
`basin3d.core.synthesis`
************************

.. currentmodule:: basin3d.core.synthesis

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` synthesis classes
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""

import logging
from typing import Dict, Iterator

from basin3d.core.connection import InvalidOrMissingCredentials
from basin3d.core.models import Base, MeasurementTimeseriesTVPObservation, MonitoringFeature, TimeMetadataMixin
from basin3d.core.plugin import DataSourcePluginAccess, get_feature_type
from basin3d.core.types import FeatureTypes

logger = logging.getLogger(__name__)
QUERY_PARAM_DATASOURCE = "datasource"
QUERY_PARAM_MONITORING_FEATURES = "monitoring_features"
QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES = "observed_property_variables"
QUERY_PARAM_AGGREGATION_DURATION = "aggregation_duration"
QUERY_PARAM_STATISTICS = "statistic"
QUERY_PARAM_START_DATE = "start_date"
QUERY_PARAM_END_DATE = "end_date"
QUERY_PARAM_PARENT_FEATURES = "parent_features"
QUERY_PARAM_RESULT_QUALITY = "result_quality"
QUERY_PARAM_FEATURE_TYPE = "feature_type"


def extract_id(identifer):
    """
    Extract the datasource identifier from the broker identifier
    :param identifer:
    :return:
    """
    if identifer:
        site_list = identifer.split("-")
        identifer = identifer.replace("{}-".format(site_list[0]),
                                      "", 1)  # The datasource id prefix needs to be removed
    return identifer


def filter_query_param_values(param_name, id_prefix, query_params, **kwargs):
    """
    Filter query param values for those with the specified id_prefix
    :param param_name:
    :param id_prefix:
    :param query_params:
    :return:
    """
    # Synthesize the ids (remove datasource id_prefix)
    if param_name in kwargs:
        values = kwargs.get(param_name, None)

        if values:
            query_params[param_name] = [x for x in
                                        values
                                        if x.startswith("{}-".format(id_prefix))]


def extract_query_param_ids(param_name, id_prefix, query_params, **kwargs):
    """
    Extract the ids from the specified query params

    :param param_name: the name of the list parameter
    :param id_prefix:  the datasource id prefix
    :param query_params: the query params to populate
    :type query_params: dict
    :return:
    """
    # Synthesize the ids (remove datasource id_prefix)
    values = kwargs.get(param_name, None)
    if isinstance(values, str):
        values = values.split(",")

    if values:
        query_params[param_name] = [extract_id(x) for x in
                                    values
                                    if x.startswith("{}-".format(id_prefix))]


class DataSourceModelAccess:
    """
    Base ViewsSet for all DataSource plugins.  The inheritance diagram shows that this class extends the
    `Django Rest Framework <https://www.django-rest-framework.org/>`_
    class :class:`rest_framework.viewsets.ViewSet`. These are based on `Django generic views
    <https://docs.djangoproject.com/en/2.2/topics/class-based-views/generic-display/>`_.

    .. inheritance-diagram:: rest_framework.viewsets.ViewSet basin3d.synthesis.viewsets.DataSourcePluginViewSet

    """

    def __init__(self, plugins, catalog):
        self._plugins = plugins
        self._catalog = catalog

    @property
    def plugins(self):
        return self._plugins

    @property
    def synthesis_model(self):
        raise NotImplementedError

    def synthesize_query_params(self, plugin_access: DataSourcePluginAccess, **kwargs) -> Dict[str, str]:
        """
        Synthesizes query parameters, if necessary

        :param request: the request to synthesize
        :param plugin_access: The plugin view to synthesize query params for
        :return: The query parameters
        """
        # do nothing, subclasses may override this
        raise NotImplementedError

    def list(self, **kwargs) -> Iterator[Base]:
        """
        Return the synthesized plugin results

        :param request: The incoming request object
        :type request: :class:`rest_framework.request.Request`
        :param format: The format to present the data (default is json)
        :return: The HTTP Response
        :rtype: :class:`rest_framework.request.Response`
        """
        for plugin in self.plugins.values():  # Get the plugin model

            # Skip datasource if it is filtered out.
            if QUERY_PARAM_DATASOURCE in kwargs and kwargs[QUERY_PARAM_DATASOURCE]:
                if plugin.get_datasource().id_prefix not in kwargs[QUERY_PARAM_DATASOURCE].split(","):
                    continue

            plugin_views = plugin.get_plugin_access()
            if self.synthesis_model in plugin_views and \
                    hasattr(plugin_views[self.synthesis_model], "list"):
                try:
                    synthesized_query_params = self.synthesize_query_params(plugin_views[self.synthesis_model], **kwargs)
                    for obj in plugin_views[self.synthesis_model].list(**synthesized_query_params):
                        yield obj
                except InvalidOrMissingCredentials as e:
                    logger.error(e)

    def retrieve(self, pk: str, **kwargs) -> Base:
        """
        Retrieve a single synthesized value

        :param request: The request object
        :type request: :class:`rest_framework.request.Request`
        :param pk: The primary key
        :return: The HTTP Response
        :rtype: :class:`rest_framework.request.Response`
        """

        # split the datasource id prefix from the primary key
        pk_list = pk.split("-")

        try:
            plugin = self.plugins[pk_list[0]]
            datasource = plugin.get_datasource()
            if datasource:
                datasource_pk = pk.replace("{}-".format(pk_list[0]),
                                           "", 1)  # The datasource id prefix needs to be removed

                plugin_views = plugin.get_plugin_access()
                if self.synthesis_model in plugin_views:
                    synthesized_query_params = self.synthesize_query_params(plugin_views[self.synthesis_model],
                                                                            **kwargs)
                    obj: Base = plugin_views[self.synthesis_model].get(pk=datasource_pk, **synthesized_query_params)
                    return obj
                else:
                    raise Exception(f"There is no detail for {pk}")
            else:
                raise Exception(f"DataSource not not found for pk {pk}")
        except KeyError:
            raise Exception(f"Invalid pk {pk}")


class MonitoringFeatureAccess(DataSourceModelAccess):
    """
    MonitoringFeature: A feature upon which monitoring is made. OGC Timeseries Profile OM_MonitoringFeature.

    **Properties**

    * *id:* string, Unique feature identifier
    * *name:* string, Feature name
    * *description:* string, Description of the feature
    * *feature_type:* sting, FeatureType: REGION, SUBREGION, BASIN, SUBBASIN, WATERSHED, SUBWATERSHED, SITE, PLOT, HORIZONTAL PATH, VERTICAL PATH, POINT
    * *observed_property_variables:* list of observed variables made at the feature. Observed property variables are configured via the plugins.
    * *related_sampling_feature_complex:* list of related_sampling features. PARENT features are currently supported.
    * *shape:* string, Shape of the feature: POINT, CURVE, SURFACE, SOLID
    * *coordinates:* location of feature in absolute and/or representative datum
    * *description_reference:* string, additional information about the Feature
    * *related_party:* (optional) list of people or organizations responsible for the Feature
    * *utc_offset:* float, Coordinate Universal Time offset in hours (offset in hours), e.g., +9
    * *url:* url, URL with details for the feature

    **Filter** by the following attributes (/?attribute=parameter&attribute=parameter&...)

    * *datasource (optional):* a single data source id prefix (e.g ?datasource=`datasource.id_prefix`)

    **Restrict fields**  with query parameter ‘fields’. (e.g. ?fields=id,name)
    """
    synthesis_model = MonitoringFeature

    def synthesize_query_params(self, plugin_access: DataSourcePluginAccess, **kwargs) -> Dict[str, str]:
        """
        Synthesizes query parameters, if necessary

        Parameters Synthesized:

        :param request: the request to synthesize
        :type request: :class:`rest_framework.request.Request`
        :param plugin_access: The plugin view to synthesize query params for
        :return: The query parameters
        """
        query_params = {}

        # Look in Request to find URL and get type out if there
        # ToDo: potentially remove -- need to figure out how to handle in plugin
        k, _ = self.extract_type(**kwargs)
        if k is not None:
            query_params[QUERY_PARAM_FEATURE_TYPE] = k

        for key, value in kwargs.items():
            query_params[key] = value

        id_prefix = plugin_access.datasource.id_prefix
        for param_name in [QUERY_PARAM_MONITORING_FEATURES, QUERY_PARAM_PARENT_FEATURES]:
            extract_query_param_ids(param_name=param_name,
                                    id_prefix=id_prefix,
                                    query_params=query_params, **kwargs)

        return query_params

    def extract_type(self, **kwargs):
        """
        Extract the feature types from the request

        :param request: The Request object
        :return: Tuple `(feature_type_code, feature_type_name)`. (e.g (0, 'REGION')
        :rtype: tuple

        """
        k = get_feature_type("feature_type" in kwargs and kwargs["feature_type"] or None)
        if k:
            return k, FeatureTypes.TYPES[k]
        return None, None


class MeasurementTimeseriesTVPObservationAccess(DataSourceModelAccess):
    """
    MeasurementTimeseriesTVPObservation: Series of measurement (numerical) observations in
    TVP (time value pair) format grouped by time (i.e., a timeseries).

    **Properties**

    * *id:* string, Observation identifier (optional)
    * *type:* enum, MEASUREMENT_TVP_TIMESERIES
    * *observed_property:* url, URL for the observation's observed property
    * *phenomenon_time:* datetime, datetime of the observation, for a timeseries the start and end times can be provided
    * *utc_offset:* float, Coordinate Universal Time offset in hours (offset in hours), e.g., +9
    * *feature_of_interest:* MonitoringFeature obj, feature on which the observation is being made
    * *feature_of_interest_type:* enum (FeatureTypes), feature type of the feature of interest
    * *result_points:* list of TimeValuePair obj, observed values of the observed property being assessed
    * *time_reference_position:* enum, position of timestamp in aggregated_duration (START, MIDDLE, END)
    * *aggregation_duration:* enum, time period represented by observation (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
    * *unit_of_measurement:* string, units in which the observation is reported
    * *statistic:* enum, statistical property of the observation result (MEAN, MIN, MAX, TOTAL)
    * *result_quality:* enum, quality assessment of the result (CHECKED, UNCHECKED)

    **Filter** by the following attributes (?attribute=parameter&attribute=parameter&...):

    * *monitoring_features (required):* List of monitoring_features ids
    * *observed_property_variables (required):* List of observed property variable ids
    * *start_date (required):* date YYYY-MM-DD
    * *end_date (optional):* date YYYY-MM-DD
    * *aggregation_duration (default: DAY):* enum (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)
    * *statistic (optional):* List of statistic options, enum (INSTANT|MEAN|MIN|MAX|TOTAL)
    * *datasource (optional):* a single data source id prefix (e.g ?datasource=`datasource.id_prefix`)
    * *result_quality (optional):* enum (UNCHECKED|CHECKED)

    **Restrict fields** with query parameter ‘fields’. (e.g. ?fields=id,name)


    """
    synthesis_model = MeasurementTimeseriesTVPObservation

    def synthesize_query_params(self, plugin_access: DataSourcePluginAccess, **kwargs) -> Dict[str, str]:
        """
        Synthesizes query parameters, if necessary

        Parameters Synthesized:
          + monitoring_features
          + observed_property_variables
          + aggregation_duration (default: DAY)
          + statistic
          + quality_checked

        :param plugin_access: The plugin view to synthesize query params for
        :return: The query parameters
        """

        id_prefix = plugin_access.datasource.id_prefix
        query_params = {}
        for key, value in kwargs.items():
            if isinstance(value, str):
                value = value.split(",")
            query_params[key] = value

        extract_query_param_ids(param_name=QUERY_PARAM_MONITORING_FEATURES,
                                id_prefix=id_prefix,
                                query_params=query_params, **kwargs)

        # Synthesize ObservedPropertyVariable (from BASIN-3D to DataSource variable name)
        if QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES in query_params:
            observed_property_variables = query_params.get(QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES, [])
            query_params[QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES] = [o.datasource_variable for o in
                                                                     plugin_access.get_observed_properties(
                                                                         observed_property_variables)]
        # Override Aggregation to always be DAY
        query_params[QUERY_PARAM_AGGREGATION_DURATION] = TimeMetadataMixin.AGGREGATION_DURATION_DAY

        return query_params
