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
from typing import Iterator, List, Optional, Union

from basin3d.core import monitor
from basin3d.core.catalog import verify_query_param, verify_attr_type
from basin3d.core.models import Base, MeasurementTimeseriesTVPObservation, MonitoringFeature
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint
from basin3d.core.schema.enum import MAPPING_DELIMITER, NO_MAPPING_TEXT, MessageLevelEnum, TimeFrequencyEnum
from basin3d.core.schema.query import QueryBase, QueryById, QueryMeasurementTimeseriesTVP, \
    QueryMonitoringFeature, SynthesisMessage, SynthesisResponse

logger = monitor.get_logger(__name__)


# def _synthesize_query_identifiers(values, id_prefix) -> List[str]:
#     """
#     Extract the ids from the specified query params
#
#     :param values:  the ids to synthesize
#     :param id_prefix:  the datasource id prefix
#     :return: The list of synthesizes identifiers
#     """
#     # Synthesize the ids (remove datasource id_prefix)
#     if isinstance(values, str):
#         values = values.split(",")
#
#     def extract_id(identifer):
#         """
#         Extract the datasource identifier from the broker identifier
#         :param identifer:
#         :return:
#         """
#         if identifer:
#             site_list = identifer.split("-")
#             identifer = identifer.replace("{}-".format(site_list[0]),
#                                           "", 1)  # The datasource id prefix needs to be removed
#         return identifer
#
#     return [extract_id(x) for x in
#             values
#             if x.startswith("{}-".format(id_prefix))]


class MonitorMixin(object):
    """
    Adds monitor log functionality to a class for logging synthesis messages
    """
    def log(self,
            message: str, level: Optional[MessageLevelEnum] = None, where: Optional[List] = None) -> Optional[SynthesisMessage]:
        """
        Add a synthesis message to the synthesis respoonse
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
        logger.log(logger_level, msg=message, extra=where and {"basin3d_where": ".".join(where)} or {}) # type: ignore
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


class TranslatorMixin(object):
    """
    Adds translator functionality to data source access
    """

    def translate_query(self, plugin_access, query: Union[QueryMeasurementTimeseriesTVP, QueryMonitoringFeature, QueryById]) -> QueryBase:
        """
        Main translator method that calls individual methods
        :param plugin_access: plugin access
        :param query: query to be translated
        :return: translated query
        """
        translated_query = query.copy()
        self.translate_mapped_query_attrs(plugin_access, translated_query)
        self.translate_prefixed_query_attrs(plugin_access, translated_query)
        is_valid_translated_query = self.is_translated_query_valid(plugin_access.datasource.id, query, translated_query)

        if is_valid_translated_query:
            translated_query.is_valid_translated_query = is_valid_translated_query
            self.clean_query(translated_query)

        return translated_query

    @staticmethod
    def _order_mapped_fields(plugin_access, query_mapped_fields):
        """

        :param plugin_access:
        :param query_mapped_fields:
        :return:
        """
        query_mapped_fields_ordered = []

        # get list of compound mappings if any
        compound_mappings = plugin_access.get_compound_mapping_str()

        # If there are compound mappings...
        if compound_mappings:
            cm_fields = []
            # Split up the compound mappings, preserving the order of the attributes as specified in the plugin mapping file.
            # The order only matters relative to the individual compound mapping.
            for cm in compound_mappings:
                cm_attrs = cm.split(MAPPING_DELIMITER)
                cm_fields.extend([verify_query_param(cm_attr) for cm_attr in cm_attrs])
            # first loop thru the compound mapping fields
            for cm in cm_fields:
                # if the attribute is one of the mapped fields in this particular query
                if cm in query_mapped_fields:
                    # add it to the ordered list
                    query_mapped_fields_ordered.append(cm)
                    # then remove it from the mapped field list
                    query_mapped_fields.pop(query_mapped_fields.index(cm))
            # then, add any remaining non-compound fields
            query_mapped_fields_ordered.extend(query_mapped_fields)
        else:
            # if there are no compound mappings, then the order doesn't matter, just copy the mapped field list.
            query_mapped_fields_ordered = query_mapped_fields

        return query_mapped_fields_ordered

    def translate_mapped_query_attrs(self, plugin_access, query: Union[QueryMeasurementTimeseriesTVP, QueryMonitoringFeature, QueryById]) -> QueryBase:
        """
        Translation functionality
        """
        query_mapped_fields = query.get_mapped_fields()

        # if there are no mapped fields, return the query as is.
        if not query_mapped_fields:
            return query

        # order the query fields by any compound attributes
        query_mapped_fields_ordered = self._order_mapped_fields(plugin_access, query_mapped_fields)

        for attr in query_mapped_fields_ordered:
            # if the attribute is specified, proceed to translate it
            # NOTE: looking in the translated_query which is mutable. As the translation occurs, translated query fields may change
            #       and the if statement may have different values for a given field during the loop.
            if getattr(query, attr):
                b3d_vocab = getattr(query, attr)

                if isinstance(b3d_vocab, str):
                    ds_vocab = plugin_access.get_ds_vocab(attr.upper(), b3d_vocab, query)
                else:
                    ds_vocab = []
                    for b3d_value in b3d_vocab:
                        # handle multiple values returned
                        ds_vocab.extend(plugin_access.get_ds_vocab(attr.upper(), b3d_value, query))
                setattr(query, attr, ds_vocab)

                # look up whether the attr is part of a compound mapping
                compound_attrs = plugin_access.get_compound_mapping_attributes(attr.upper(), is_query=True)
                # if so: for any compound attrs, clear out the values in the synthesized query b/c search needs to be done on the coupled datasource_vocab
                for compound_attr in compound_attrs:
                    compound_attr = verify_query_param(compound_attr, is_query=True)
                    setattr(query, compound_attr, None)

        # NOTE: always returns list for each attr b/c multiple mappings are possible.
        return query

    @staticmethod
    def translate_prefixed_query_attrs(plugin_access, query: Union[QueryMeasurementTimeseriesTVP, QueryMonitoringFeature, QueryById]) -> QueryBase:
        """

        :param plugin_access:
        :param query:
        :return:
        """
        def extract_id(identifer):
            """
            Extract the datasource identifier from the broker identifier
            :param identifer:
            :return:
            """
            if identifer:
                site_list = identifer.split("-")
                identifer = identifer.replace("{}-".format(site_list[0]), "", 1)  # The datasource id prefix needs to be removed
            return identifer

        id_prefix = plugin_access.datasource.id_prefix

        for attr in query.get_prefixed_fields():
            attr_value = getattr(query, attr)
            if attr_value:
                if isinstance(attr_value, str):
                    attr_value = attr_value.split(",")

                translated_values = [extract_id(x) for x in attr_value if x.startswith("{}-".format(id_prefix))]

                setattr(query, attr, translated_values)

        return query

    @staticmethod
    def is_translated_query_valid(datasource_id, query, translated_query) -> Optional[bool]:
        # loop thru kwargs
        for attr in query.get_mapped_fields():
            translated_attr_value = getattr(translated_query, attr)
            b3d_attr_value = getattr(query, attr)
            if isinstance(b3d_attr_value, list):
                b3d_attr_value = ', '.join(b3d_attr_value)
            if translated_attr_value and isinstance(translated_attr_value, list):
                # if list and all of list == NOT_SUPPORTED, False
                if all([x == NO_MAPPING_TEXT for x in translated_attr_value]):
                    logger.warning(f'Translated query for datasource {datasource_id} is invalid. No vocabulary found for attribute {attr} with values: {b3d_attr_value}.')
                    return False
            elif translated_attr_value and isinstance(translated_attr_value, str):
                # if single NOT_SUPPORTED, False
                if translated_attr_value == NO_MAPPING_TEXT:
                    logger.warning(f'Translated query for datasource {datasource_id} is invalid. No vocabulary found for attribute {attr} with value: {b3d_attr_value}.')
                    return False
            elif translated_attr_value:
                logger.warning(
                    f'Translated query for datasource {datasource_id} cannot be assessed. Translated value for {attr} is not expected type.')
                return None
        return True

    @staticmethod
    def clean_query(translated_query: QueryBase) -> QueryBase:
        """
        Remove any NOT_SUPPORTED translations
        :param translated_query:
        :return:
        """
        for attr in translated_query.get_mapped_fields():
            attr_value = getattr(translated_query, attr)
            if attr_value and isinstance(attr_value, list):
                clean_list = [val for val in attr_value if val != NO_MAPPING_TEXT]
                unique_list = list(set(clean_list))
                setattr(translated_query, attr, unique_list)
            elif attr_value and attr_value == NO_MAPPING_TEXT:
                setattr(translated_query, attr, None)
        return translated_query


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

        self._synthesis_response = SynthesisResponse(query=query)
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

                        # Now synthesize the query object
                        translated_query_params: QueryBase = self._model_access.synthesize_query(
                            plugin_views[self._model_access.synthesis_model],
                            self._synthesis_response.query)
                        translated_query_params.datasource = [plugin.get_datasource().id]

                        # Get the model access iterator if synthesized query is valid
                        if translated_query_params.is_valid_translated_query:
                            self._model_access_iterator = plugin_views[self._model_access.synthesis_model].list(
                                query=translated_query_params)
                        else:
                            self.warn("Translated query is not valid.")

                    else:
                        self.warn("Plugin view does not exist")

                except Exception as e:
                    self.error(f"Unexpected Error({e.__class__.__name__}): {str(e)}")

            else:
                # Clear all context
                if self._where_context_token:
                    monitor.basin3d_where.reset(self._where_context_token)
                raise StopIteration

    def log(self, message: str, level: Optional[MessageLevelEnum] = None,  where: Optional[List] = None):  # type: ignore[override]
        """
        Add a synthesis message to the synthesis respoonse
        :param message: The message
        :param level:  The message level
        :return: None
        """
        synthesis_message = super().log(message, level, self._where)
        if synthesis_message:
            self._synthesis_response.messages.append(synthesis_message)


class DataSourceModelAccess(MonitorMixin, TranslatorMixin):
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
    def retrieve(self, query: QueryById) -> SynthesisResponse:
        """
        Retrieve a single synthesized value

        :param query: The query for this request
        """
        messages = []
        if query.id:

            # split the datasource id prefix from the primary key
            id_list = query.id.split("-")
            datasource = None
            try:
                plugin = self.plugins[id_list[0]]
                datasource = plugin.get_datasource()
            except KeyError:
                pass

            if datasource:
                datasource_pk = query.id.replace("{}-".format(id_list[0]),
                                                 "", 1)  # The datasource id prefix needs to be removed

                plugin_views = plugin.get_plugin_access()
                monitor.set_ctx_basin3d_where([plugin.get_datasource().id, self.synthesis_model.__name__])
                if self.synthesis_model in plugin_views:
                    synthesized_query: QueryById = query.copy()
                    synthesized_query.id = datasource_pk
                    synthesized_query.datasource = [datasource.id]
                    obj: Base = plugin_views[self.synthesis_model].get(query=synthesized_query)
                    return SynthesisResponse(query=query, data=obj)
                else:
                    messages.append(self.log("Plugin view does not exist",
                                             MessageLevelEnum.WARN,
                                             [plugin.get_datasource().id, self.synthesis_model.__name__],
                                             ))

            else:
                messages.append(self.log(f"DataSource not not found for id {query.id}",
                                MessageLevelEnum.ERROR))

        return SynthesisResponse(query=query, messages=messages)


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

    def synthesize_query(self, plugin_access: DataSourcePluginAccess, query: QueryMonitoringFeature) -> QueryBase:  # type: ignore[override]
        """
        Synthesizes query parameters, if necessary

        Parameters Synthesized:

        :param query: The query information to be synthesized
        :param plugin_access: The plugin view to synthesize query params for
        :return: The synthesized query information
        """
        return self.translate_query(plugin_access, query)


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
    * *aggregation_duration:* enum, time period represented by observation (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, NONE)
    * *unit_of_measurement:* string, units in which the observation is reported
    * *statistic:* enum, statistical property of the observation result (MEAN, MIN, MAX, TOTAL)
    * *result_quality:* enum, quality assessment of the result (VALIDATED, UNVALIDATED, SUSPECTED, REJECTED, ESTIMATED)

    **Filter** by the following attributes (?attribute=parameter&attribute=parameter&...):

    * *monitoring_features (required):* List of monitoring_features ids
    * *observed_property_variables (required):* List of observed property variable ids
    * *start_date (required):* date YYYY-MM-DD
    * *end_date (optional):* date YYYY-MM-DD
    * *aggregation_duration (default: DAY):* enum (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|NONE)
    * *statistic (optional):* List of statistic options, enum (INSTANT|MEAN|MIN|MAX|TOTAL)
    * *datasource (optional):* a single data source id prefix (e.g ?datasource=`datasource.id_prefix`)
    * *result_quality (optional):* enum (VALIDATED|UNVALIDATED|SUSPECTED|REJECTED|ESTIMATED)
    * *sampling_medium (optional):* ADD here -- probably should be enum

    **Restrict fields** with query parameter ‘fields’. (e.g. ?fields=id,name)


    """
    synthesis_model = MeasurementTimeseriesTVPObservation

    def synthesize_query(self, plugin_access: DataSourcePluginAccess, query: QueryMeasurementTimeseriesTVP) -> QueryBase:  # type: ignore[override]
        """
        Synthesizes query parameters, if necessary

        Parameters Synthesized:
          + monitoring_features
          + observed_property_variables
          + aggregation_duration (default: DAY)
          + statistic
          + quality_checked

        :param query:
        :param plugin_access: The plugin view to synthesize query params for
        :return: The query parameters
        """

        # query at this point is still basin3d vocabulary
        if query.aggregation_duration != TimeFrequencyEnum.NONE:
            query.aggregation_duration = TimeFrequencyEnum.DAY

        return self.translate_query(plugin_access, query)
