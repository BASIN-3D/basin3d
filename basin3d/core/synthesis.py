"""

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
from typing import Iterator, List, Optional

from basin3d.core import monitor
from basin3d.core.models import Base, MeasurementTimeseriesTVPObservation, MonitoringFeature
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint
from basin3d.core.schema.enum import MessageLevelEnum, AggregationDurationEnum
from basin3d.core.schema.query import QueryBase, QueryMeasurementTimeseriesTVP, \
    QueryMonitoringFeature, SynthesisMessage, SynthesisResponse
from basin3d.core.translate import translate_query

logger = monitor.get_logger(__name__)


class MonitorMixin(object):
    """
    Adds monitor log functionality to a class for logging synthesis messages
    """
    def log(self,
            message: str, level: Optional[MessageLevelEnum] = None, where: Optional[List] = None) -> Optional[SynthesisMessage]:
        """
        Add a synthesis message to the synthesis response
        :param message: The message
        :param level:  The message level
        :param where:  Where the message is from
        :return: SynthesisMessage
        """
        logger_level = logging.INFO
        synthesis_message = None
        if level:
            logger_level = level == MessageLevelEnum.CRITICAL and logging.CRITICAL or level == MessageLevelEnum.WARN and logging.WARNING \
                 or level == MessageLevelEnum.ERROR and logging.ERROR or logging.INFO
            synthesis_message = SynthesisMessage(msg=message, level=level, where=where)
        logger.log(logger_level, msg=message, extra=where and {"basin3d_where": ".".join(where)} or {})  # type: ignore
        return synthesis_message

    def info(self, message: str, where: Optional[List] = None):
        """
        Add a info level message
        :param where:
        :param message:
        :return: None
        """
        self.log(message, where=where)

    def warn(self, message: str, where: Optional[List] = None) -> Optional[SynthesisMessage]:
        """
        Add a warning level message
        :param where:
        :param message:
        :return: SynthesisMessage
        """
        return self.log(message, MessageLevelEnum.WARN, where)

    def error(self, message: str, where: Optional[List] = None) -> Optional[SynthesisMessage]:
        """
        Add a error level message
        :param where:
        :param message:
        :return: SynthesisMessage
        """
        return self.log(message, MessageLevelEnum.ERROR, where)

    def critical(self, message: str, where: Optional[List] = None) -> Optional[SynthesisMessage]:
        """
        Add a critical level message
        :param where:
        :param message:
        :return:
        """
        return self.log(message, MessageLevelEnum.CRITICAL, where)


class DataSourceModelIterator(MonitorMixin, Iterator):
    """
    BASIN-3D Data Source Model generator
    """

    @property
    def synthesis_response(self) -> SynthesisResponse:
        """Response object for the Synthesis"""
        return self._synthesis_response

    def __init__(self, query: QueryBase, model_access: 'DataSourceModelAccess'):
        """
        Initialize the generator with the query and the model access

        :param query: the unsynthesized query
        :param model_access: Model access
        """

        self._synthesis_response = SynthesisResponse(query=query)  # type: ignore[call-arg]
        self._model_access: 'DataSourceModelAccess' = model_access

        # Filter the plugins, if specified
        if not self._synthesis_response.query.datasource:
            self._plugins = list(self._model_access.plugins.values())
        elif self._synthesis_response.query.datasource:
            self._plugins = [self._model_access.plugins[d] for d in self._synthesis_response.query.datasource if
                             d in self._model_access.plugins.keys()]

        # Internal attributes that contain the iterator state
        self._plugin_index = -1
        self._model_access_iterator = None
        self._next = None
        self._where: Optional[List] = None
        self._where_context_token = None

    def __next__(self) -> Base:
        """
        Return the next item from the iterator. If there are no further items, raise the StopIteration exception.

        """

        while True:
            # Is there an iterator?  Return the next data item
            if self._model_access_iterator:

                try:
                    self._next = next(self._model_access_iterator)
                    if self._next:
                        return self._next
                except StopIteration as se:
                    # ignore sub iterator StopIteration exception
                    # Get any warnings that may have been generated
                    if hasattr(se, "value") and se.value and se.value.args:
                        if isinstance(se.value.args[0], (list, tuple, set)):
                            for m in se.value.args[0]:
                                self.warn(message=m)
                        else:
                            self.warn("Synthesis generated warnings but they are in the wrong format")

            # Setup to get the data from the next data source plugin
            self._plugin_index += 1
            self._model_access_iterator = None

            # Are there any more plugins?
            if self._plugin_index < len(self._plugins):
                self._where = [self._plugins[self._plugin_index].get_datasource().id,
                               self._model_access.synthesis_model.__name__]
                self._where_context_token = monitor.set_ctx_basin3d_where(self._where)
                plugin: DataSourcePluginPoint = self._plugins[self._plugin_index]

                try:
                    # Get the plugin view and determine if it has a list method
                    plugin_views = plugin.get_plugin_access()
                    if self._model_access.synthesis_model in plugin_views and \
                            hasattr(plugin_views[self._model_access.synthesis_model], "list"):

                        # Now translate the query object
                        translated_query_params: QueryBase = self._model_access.synthesize_query(
                            plugin_views[self._model_access.synthesis_model],
                            self._synthesis_response.query)
                        translated_query_params.datasource = [plugin.get_datasource().id]

                        # Get the model access iterator if synthesized query is valid
                        if translated_query_params.is_valid_translated_query:
                            self._model_access_iterator = plugin_views[self._model_access.synthesis_model].list(
                                query=translated_query_params)
                        else:
                            self.warn(f'Translated query for datasource {plugin.get_datasource().id} is not valid.')

                    else:
                        self.warn("Plugin view does not exist")

                except Exception as e:
                    self.error(f"Unexpected Error({e.__class__.__name__}): {str(e)}")

            else:
                # Clear all context
                if self._where_context_token:
                    monitor.basin3d_where.reset(self._where_context_token)
                raise StopIteration

    def log(self, message: str, level: Optional[MessageLevelEnum] = None, where: Optional[List] = None):  # type: ignore[override]
        """
        Add a synthesis message to the synthesis response
        :param message: The message
        :param level:  The message level
        :return: None
        """
        synthesis_message = super().log(message, level, self._where)
        if synthesis_message:
            self._synthesis_response.messages.append(synthesis_message)


class DataSourceModelAccess(MonitorMixin):
    """
    Base class for DataSource model access.
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

    def synthesize_query(self, plugin_access: DataSourcePluginAccess,
                         query: QueryBase) -> QueryBase:
        """
        Synthesizes query parameters, if necessary

        :param query: The query information to be synthesized
        :param plugin_access: The plugin view to synthesize query params for
        :return: The synthesized query information
        """
        # do nothing, subclasses may override this
        raise NotImplementedError

    @monitor.ctx_synthesis
    def list(self, query: QueryBase) -> DataSourceModelIterator:
        """
        Return the synthesized plugin results

        :param query: The query for this function
        """
        return DataSourceModelIterator(query, self)

    @monitor.ctx_synthesis
    def retrieve(self, query: QueryBase, **kwargs) -> SynthesisResponse:
        """
        Retrieve a single synthesized value

        :param query: The query for this request
        :param kwargs: messages, list of messages to be returned in the SynthesisResponse
        """
        datasource_id: Optional[str] = None
        datasource = None
        messages: List[Optional[SynthesisMessage]] = []

        if 'messages' in kwargs and kwargs.get('messages'):
            msg = kwargs.get('messages')
            if isinstance(msg, List) and isinstance(msg[0], SynthesisMessage):
                messages.extend(msg)
            else:
                messages.append(self.log('Message mis-configured.', MessageLevelEnum.WARN))

        # if the datasource is in the query, utilize it straight up
        if query.datasource:
            datasource_id = query.datasource[0]

        # otherwise, get from a prefixed field.
        # Not tying specifically to id at this point to enable different retrieve approaches from future classes.
        elif query.prefixed_fields:
            value = None
            for prefixed_field in query.prefixed_fields:
                attr_value = getattr(query, prefixed_field)
                if attr_value and isinstance(attr_value, str):
                    value = attr_value
                    break
                elif attr_value and isinstance(attr_value, list):
                    value = attr_value[0]
                    break

            if value and isinstance(value, str):
                datasource_id = value.split("-")[0]

        try:
            plugin = self.plugins[datasource_id]
            datasource = plugin.get_datasource()
        except KeyError:
            pass

        if datasource:

            plugin_views = plugin.get_plugin_access()
            monitor.set_ctx_basin3d_where([plugin.get_datasource().id, self.synthesis_model.__name__])
            if self.synthesis_model in plugin_views and hasattr(plugin_views[self.synthesis_model], 'get'):

                # Now translate the query object
                translated_query_params: QueryBase = self.synthesize_query(plugin_views[self.synthesis_model], query)
                translated_query_params.datasource = [plugin.get_datasource().id]

                # Get the model access iterator if synthesized query is valid
                if translated_query_params.is_valid_translated_query:
                    item: Optional[Base] = plugin_views[self.synthesis_model].get(query=translated_query_params)
                else:
                    logger.warning(f'Translated query for datasource {plugin.get_datasource().id} is not valid.')

                if item:
                    return SynthesisResponse(query=query, data=item, messages=messages)  # type: ignore[call-arg]
            else:
                messages.append(self.log("Plugin view does not exist", MessageLevelEnum.WARN,
                                         [plugin.get_datasource().id, self.synthesis_model.__name__],))

        else:
            messages.append(self.log("DataSource not found for retrieve request", MessageLevelEnum.ERROR))

        return SynthesisResponse(query=query, messages=messages)  # type: ignore[call-arg]


class MonitoringFeatureAccess(DataSourceModelAccess):
    """
    MonitoringFeature: A feature upon which monitoring is made. OGC Timeseries Profile OM_MonitoringFeature.

    **Properties**

    * *id:* string, Unique feature identifier
    * *name:* string, Feature name
    * *description:* string, Description of the feature
    * *feature_type:* sting, FeatureType: REGION, SUBREGION, BASIN, SUBBASIN, WATERSHED, SUBWATERSHED, SITE, PLOT, HORIZONTAL PATH, VERTICAL PATH, POINT
    * *observed_properties:* list of observed properties (variables) made at the feature. Observed properties are configured via the plugins.
    * *related_sampling_feature_complex:* list of related_sampling features. PARENT features are currently supported.
    * *shape:* string, Shape of the feature: POINT, CURVE, SURFACE, SOLID
    * *coordinates:* location of feature in absolute and/or representative datum
    * *description_reference:* string, additional information about the Feature
    * *related_party:* (optional) list of people or organizations responsible for the Feature
    * *utc_offset:* float, Coordinate Universal Time offset in hours (offset in hours), e.g., +9
    * *url:* url, URL with details for the feature

    **Filter** by the following attributes

    * *datasource (optional):* str, a single data source id prefix
    * *id (optional):* str, a single monitoring feature id. Cannot be combined with monitoring_feature
    * *parent_feature (optional)*: list, a list of parent feature ids. Plugin must have this functionality enabled.
    * *monitoring_feature (optional)*: list, a list of monitoring feature ids. Cannot be combined with id, which will take precedence.

    """
    synthesis_model = MonitoringFeature

    def synthesize_query(self, plugin_access: DataSourcePluginAccess, query: QueryMonitoringFeature) -> QueryBase:  # type: ignore[override]
        """
        Synthesizes query parameters, if necessary

        Parameters Synthesized:

        :param query: The query information to be synthesized
        :param plugin_access: The plugin view to synthesize query params for
        :return: The synthesized query information
        """
        return translate_query(plugin_access, query)

    def retrieve(self, query: QueryMonitoringFeature) -> SynthesisResponse:
        """
        Retrieve the specified Monitoring Feature

        :param query: :class:`basin3d.core.schema.query.QueryMonitoringFeature`, id must be specified; monitoring_feature if specified will be removed.
        :return: The synthesized response containing the specified MonitoringFeature if it exists
        """

        # validate that id is specified
        if not query.id:
            return SynthesisResponse(
                query=query,
                messages=[self.log('query.id field is missing and is required for monitoring feature request by id.',
                                   MessageLevelEnum.ERROR)])  # type: ignore[call-arg]

        msg = []

        # remove monitoring_feature specification (i.e., id takes precedence)
        if query.monitoring_feature:
            mf_text = ', '.join(query.monitoring_feature)
            query.monitoring_feature = None
            msg.append(self.log(f'Monitoring Feature query has both id {query.id} and monitoring_feature {mf_text} '
                                f'specified. Removing monitoring_feature and using id.', MessageLevelEnum.WARN))

        # retrieve / get method order should be: MonitoringFeatureAccess, DataSourceModelAccess, <plugin>MonitoringFeatureAccess.get
        synthesis_response: SynthesisResponse = super().retrieve(query=query, messages=msg)
        return synthesis_response


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
    * *result:* dictionary of 2 lists: "value" contains TimeValuePair obj and "quality" the corresponding quality assessment per value, observed values and their quality for the observed property being assessed
    * *time_reference_position:* enum, position of timestamp in aggregated_duration (START, MIDDLE, END)
    * *aggregation_duration:* enum, time period represented by observation (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, NONE)
    * *unit_of_measurement:* string, units in which the observation is reported
    * *statistic:* list, statistical properties of the observation result (MEAN, MIN, MAX, TOTAL)
    * *result_quality:* list, quality assessment values contained in the result (VALIDATED, UNVALIDATED, SUSPECTED, REJECTED, ESTIMATED)

    **Filter** by the following attributes:

    * *monitoring_feature (required):* List of monitoring_features ids
    * *observed_property (required):* List of observed property variable ids
    * *start_date (required):* date YYYY-MM-DD
    * *end_date (optional):* date YYYY-MM-DD
    * *aggregation_duration (default: DAY):* enum (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|NONE)
    * *statistic (optional):* List of statistic options, enum (INSTANT|MEAN|MIN|MAX|TOTAL)
    * *datasource (optional):* a single data source id prefix (e.g ?datasource=`datasource.id_prefix`)
    * *result_quality (optional):* enum (VALIDATED|UNVALIDATED|SUSPECTED|REJECTED|ESTIMATED)
    * *sampling_medium (optional):* enum (SOLID_PHASE|WATER|GAS|OTHER)

    """
    synthesis_model = MeasurementTimeseriesTVPObservation

    def synthesize_query(self, plugin_access: DataSourcePluginAccess, query: QueryMeasurementTimeseriesTVP) -> QueryBase:  # type: ignore[override]
        """
        Synthesizes query parameters, if necessary

        Parameters Synthesized:
          + monitoring_feature
          + observed_property
          + aggregation_duration (default: DAY)
          + statistic
          + quality_checked

        :param query:
        :param plugin_access: The plugin view to synthesize query params for
        :return: The query parameters
        """

        # only allow instantaneous data (NONE) or daily data (DAY) data
        # NOTE: query at this point is still in BASIN-3D vocab
        if query.aggregation_duration != AggregationDurationEnum.NONE:
            query.aggregation_duration = AggregationDurationEnum.DAY

        return translate_query(plugin_access, query)
