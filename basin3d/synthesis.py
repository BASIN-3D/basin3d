"""

.. currentmodule:: basin3d.synthesis

:synopsis: BASIN-3D Synthesis API
:module author: Val Hendrix <vhendrix@lbl.gov>, Danielle Svehla Christianson <dschristianson@lbl.gov>. Catherine Wong <catwong@lbl.gov>


Functions
----------------
* :func:`register` - Register the specified plugins or implicitly register loaded plugins


Classes
--------
* :class:`DataSynthesizer` - Synthesis API

synthesis.DataSynthesizer Functions
-----------------------------------

* :func:`DataSynthesizer.attribute_mappings`- Search for attribute_mappings which describe how the datasource vocabularies are mapped to BASIN-3D vocabularies, including observed properties, statistics, result_quality, etc.
* :func:`DataSynthesizer.measurement_timeseries_tvp_observations`- Search for Measurement Timeseries TVP Observations from specified Monitoring features and observed property variables
* :func:`DataSynthesizer.monitoring_features`- Search for all monitoring features, features by parent monitoring features, features by monitoring feature identifiers, or look for a single monitoring feature by id.
* :func:`DataSynthesizer.observed_properties`- Search for observed properties

----------------------------------
"""
from importlib import import_module
from typing import List, Union, cast

from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import DataSource
from basin3d.core.plugin import PluginMount
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature, SynthesisResponse
from basin3d.core.synthesis import DataSourceModelIterator, MeasurementTimeseriesTVPObservationAccess, \
    MonitoringFeatureAccess, logger


class SynthesisException(Exception):
    """Special Exception for Synthesis module"""
    pass


def register(plugins: List[str] = None):
    """
    Register the specified plugins or implicitly register loaded plugins


    >>> from basin3d import synthesis
    >>> synthesizer = synthesis.register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    >>> synthesizer.datasources
    [DataSource(id='USGS', name='USGS', id_prefix='USGS', location='https://waterservices.usgs.gov/nwis/', credentials={})]

    :param plugins: [Optional] plugins to registered
    :return: DataSynthesizer(plugin_dict, catalog)
    """

    if not plugins:
        # Implicit registration of loaded plugins
        plugins = list(PluginMount.plugins.values())

    if not plugins:
        raise SynthesisException("There are no plugins to register")

    plugin_dict = {}
    catalog = CatalogTinyDb()
    for plugin in plugins:
        if isinstance(plugin, str):
            # If this is a string  convert to module and class then load
            class_name_list = plugin.split(".")
            module_name = plugin.replace(".{}".format(class_name_list[-1]), "")
            module = import_module(module_name)
            plugin_class = getattr(module, class_name_list[-1])
        else:
            # This is already a class
            plugin_class = plugin

        # Instantiate the plugin with the new catalog
        plugin = plugin_class(catalog)
        plugin_dict[plugin_class.get_meta().id_prefix] = plugin

        logger.info("Loading Plugin = {}".format(plugin_class.__name__))

    # Instantiate a synthesizer.
    return DataSynthesizer(plugin_dict, catalog)


class DataSynthesizer:
    """
    Synthesis API
    """

    def __init__(self, plugins: dict, catalog: CatalogTinyDb):
        self._plugins = plugins
        self._catalog = catalog
        self._datasources = {}
        for p in self._plugins.values():
            datasource = p.get_datasource()
            self._datasources[datasource.id] = datasource

        self._catalog.initialize(list(self._plugins.values()))
        self._monitoring_feature_access = MonitoringFeatureAccess(plugins, self._catalog)
        self._measurement_timeseries_tvp_observation_access = \
            MeasurementTimeseriesTVPObservationAccess(plugins, self._catalog)

    @property
    def datasources(self) -> List[DataSource]:
        """
        The Datasources loaded in this synthesizer
        :return:
        """
        return list(self._datasources.values())

    def observed_properties(self):
        """

        >>> from basin3d.plugins import usgs
        >>> from basin3d import synthesis
        >>> synthesizer = synthesis.register()
        >>> response = synthesizer.observed_properties()
        >>> for opv in response:
        ...     print(f'{opv.basin3d_vocab} -- {opv.full_name} -- {opv.units}')
        ACT -- Acetate (CH3COO) -- mM
        Br -- Bromide (Br) -- mM
        Cl -- Chloride (Cl) -- mM
        DIN -- Dissolved Inorganic Nitrogen (Nitrate + Nitrite) -- mg/L
        DTN -- Dissolved Total Nitrogen (DTN) -- mg/L
        F -- Fluoride (F) -- mM
        ...

        BASIN-3D observed properties. An observed property defines what is being measured.
        Data source observed property vocabularies are mapped and thus synthesized to the BASIN-3D observed property vocabulary.

        :return: an iterator of :class:`basin3d.core.models.ObservedProperty` objects

        """
        return self._catalog.find_observed_properties()

    def attribute_mappings(self, datasource_id=None, attr_type=None, attr_vocab=None, from_basin3d=False):
        """

        >>> from basin3d.plugins import usgs
        >>> from basin3d import synthesis
        >>> synthesizer = synthesis.register()
        >>> response = synthesizer.attribute_mappings()  # list all attribute mappings registered
        >>> for attr_mapping in response:
        ...     print(f'{attr_mapping.attr_type} | {attr_mapping.basin3d_vocab} -- {attr_mapping.datasource_vocab}')
        OBSERVED_PROPERTY:SAMPLING_MEDIUM | PH:WATER -- 00400
        OBSERVED_PROPERTY:SAMPLING_MEDIUM | RDC:WATER -- 00060
        OBSERVED_PROPERTY:SAMPLING_MEDIUM | WLE:WATER -- 63161
        OBSERVED_PROPERTY:SAMPLING_MEDIUM | WT:WATER -- 00010
        OBSERVED_PROPERTY:SAMPLING_MEDIUM | DO:WATER -- 00300
        ...

        >>> response = synthesizer.attribute_mappings(datasource_id='USGS', attr_type='STATISTIC')
        >>> for attr_mapping in response:
        ...     print(f'{attr_mapping.attr_type} | {attr_mapping.basin3d_vocab} -- {attr_mapping.datasource_vocab}')
        STATISTIC | MEAN -- 00003
        STATISTIC | MIN -- 00002
        STATISTIC | MAX -- 00001
        STATISTIC | TOTAL -- 00006

        >>> response = synthesizer.attribute_mappings(datasource_id='USGS', attr_type='RESULT_QUALITY', attr_vocab=['VALIDATED', 'ESTIMATED'], from_basin3d=True)
        >>> for attr_mapping in response:
        ...     print(f'{attr_mapping.attr_type} | {attr_mapping.basin3d_vocab} -- {attr_mapping.datasource_vocab}, {attr_mapping.datasource_desc}')
        RESULT_QUALITY | ESTIMATED -- e, Value has been edited or estimated by USGS personnel and is write protected
        RESULT_QUALITY | ESTIMATED -- E, Value was computed from estimated unit values.
        RESULT_QUALITY | VALIDATED -- A, Approved for publication -- Processing and review completed.

        Return all the :class:`basin3d.core.models.AttributMapping` registered or those that match the specified fields.

        :param datasource_id: str, The datasource identifier
        :param attr_type: str, The attribute type (e.g., OBSERVED_PROPERTY, STATISTIC, etc)
        :param attr_vocab: str, The attribute vocabulary, either the BASIN-3D vocabulary or the datasource vocabulary
        :param from_basin3d: bool, True = the specified attr_vocab is a BASIN-3D vocabulary, False: the specified attr_vocab is from the datasource

        :return: iterator of :class:`basin3d.core.models.AttributeMapping` objects
        """
        return self._catalog.find_attribute_mappings(datasource_id=datasource_id, attr_type=attr_type, attr_vocab=attr_vocab, from_basin3d=from_basin3d)

    def monitoring_features(self, query: QueryMonitoringFeature = None, **kwargs) -> Union[DataSourceModelIterator, SynthesisResponse]:
        """
        Search for all Monitoring Features, Monitoring Features by parent monitoring features, or Monitoring Feature by id(s).

        To see feature types for a given plugin: **<plugin_module>.<plugin_class>.feature_types**

        **Search for a single monitoring feature by id:**

        >>> from basin3d.plugins import usgs, epa
        >>> from basin3d import synthesis
        >>> synthesizer = synthesis.register()
        >>> response = synthesizer.monitoring_features(id='USGS-0101')
        >>> mf = response.data
        >>> print(f"{mf.id} - {mf.description}")
        USGS-0101 - SUBREGION: St. John

        **Search for all USGS monitoring features:**

        >>> for mf in synthesizer.monitoring_features(datasource='USGS', feature_type='region'): # doctest: +ELLIPSIS
        ...     print(f"{mf.id} - {mf.description}")
        USGS-01 - REGION: New England
        USGS-02 - REGION: Mid Atlantic
        USGS-03 - REGION: South Atlantic-Gulf
        ...

        **Search for USGS points by parent (subbasin) monitoring features:**

        >>> for mf in synthesizer.monitoring_features(feature_type='point',parent_feature=['USGS-17040101']): # doctest: +ELLIPSIS
        ...    print(f"{mf.id} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        USGS-13010000 [(-110.6647222, 44.1336111)]
        USGS-13010065 [(-110.6675, 44.09888889)]
        USGS-13010450 [(-110.5874305, 43.9038296)]
        ...

        **Search for USGS points by monitoring features identifiers:**

        >>> for mf in synthesizer.monitoring_features(feature_type='point', monitoring_feature=['USGS-13010000', 'USGS-13010450']): # doctest: +ELLIPSIS
        ...    print(f"{mf.id} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        USGS-13010000 [(-110.6647222, 44.1336111)]
        USGS-13010450 [(-110.5874305, 43.9038296)]

        **Unsupported feature types warning:**

        The code below is an example of what you will see if a registered plugin does not support
        the requested feature type.

        >>> response_itr = synthesizer.monitoring_features(feature_type='horizontal_path')
        >>> for mf in response_itr:
        ...   print(mf)
        ...


        **Output warning messages from the returned iterator**

        This is an example of checking the synthesis response messages in the :class:`basin3d.core.synthesis.DataSourceModelIterator`.

        >>> response_itr.synthesis_response.messages
        [SynthesisMessage(msg='Feature type HORIZONTAL_PATH not supported by USGS.', level='WARN', where=['USGS', 'MonitoringFeature']), SynthesisMessage(msg='Feature type HORIZONTAL_PATH not supported by EPA Water Quality eXchange.', level='WARN', where=['EPA', 'MonitoringFeature'])]

        :param query: (optional) A Monitoring Feature Query :class:`basin3d.core.schema.query.QueryMonitoringFeature` object
        :param kwargs: (optional) Monitoring Feature Query parameters. See Query info below.
        :return: a single :class:`~basin3d.core.schema.query.SynthesisResponse` for a query by id
            or a :class:`~basin3d.core.synthesis.DataSourceModelIterator` for multple :class:`basin3d.core.models.MonitoringFeature` objects.

        .. note::
            **Monitoring Feature Query parameters**
            :class:`basin3d.core.schema.query.QueryMonitoringFeature`

            | All parameters are optional.
            |
            |    * **datasource** A single data source id prefix.
            |    * **feature_type** The :class:`basin3d.core.schema.enum.FeatureTypeEnum` of the desired Monitoring Feature(s). Data Sources may not support all Feature Types.
            |
            | Only one of the following can be specified in a query:
            |
            |    * **id** A single Monitoring Feature ID for the Monitoring Feature desired. Returns a single Synthesis Response object.
            |    * **monitoring_feature** List of Monitoring Feature IDs for the Monitoring Features desired. Returns iterator of MonitoringFeature objects.
            |    * **parent_feature** List of Monitoring Feature IDs for the parent features of the desired Monitoring Features. Returns iterator of MonitoringFeature objects.


        .. note::
            **Monitoring Feature attributes**
            :class:`basin3d.core.models.MonitoringFeature`

            | Attributes values are dependent on data source features.
            |
            |    * **id** Unique feature identifier, prefixed by data source id
            |    * **name** Feature name
            |    * **description** Description of the Monitoring Feature
            |    * **feature_type** :class:`basin3d.core.schema.enum.FeatureTypeEnum` REGION, SUBREGION, BASIN, SUBBASIN, WATERSHED, SUBWATERSHED, SITE, PLOT, HORIZONTAL PATH, VERTICAL PATH, POINT
            |    * **observed_properties** List of observed properties :class:`basin3d.core.models.ObservedProperty` collected at the feature.
            |    * **related_sampling_feature_complex** List of :class:`basin3d.core.models.RelatedSamplingFeature`. PARENT features are currently supported.
            |    * **shape** Shape of the feature: POINT, CURVE, SURFACE, SOLID
            |    * **coordinates** Location of feature in absolute and/or representative datum: :class:`basin3d.core.models.Coordinate`
            |    * **description_reference** Additional information about the feature
            |    * **related_party** List of people or organizations responsible for the feature
            |    * **utc_offset** Coordinate Universal Time offset in hours (offset in hours), e.g., +9
            |    * **datasource** The feature's data source :class:`basin3d.core.models.DataSource`

        """

        if not query:
            query = QueryMonitoringFeature(**kwargs)

        # Search for single or list?
        if query.id:
            #  mypy casts are only used as hints for the type checker,
            #  and they don’t perform a runtime type check.
            return cast(SynthesisResponse, self._monitoring_feature_access.retrieve(query=query))
        else:
            #  mypy casts are only used as hints for the type checker,
            #  and they don’t perform a runtime type check.
            return cast(DataSourceModelIterator,
                        self._monitoring_feature_access.list(query=query))

    def measurement_timeseries_tvp_observations(self, query: QueryMeasurementTimeseriesTVP = None, **kwargs) -> DataSourceModelIterator:
        """
        Search for Measurement Timeseries TVP Observations for the specified query arguments.

        Aggregation Duration for DAY (default) and NONE are both supported.

            **Search with aggregation duration DAY:**

            >>> from basin3d.plugins import usgs
            >>> from basin3d import synthesis
            >>> synthesizer = synthesis.register()
            >>> timeseries = synthesizer.measurement_timeseries_tvp_observations(monitoring_feature=['USGS-09110990'],observed_property=['RDC','WT'],start_date='2019-10-01',end_date='2019-10-30',aggregation_duration='DAY')
            >>> for timeseries in timeseries:
            ...    print(f"{timeseries.feature_of_interest.id} - {timeseries.observed_property.get_basin3d_vocab()}")
            USGS-09110990 - RDC

            **Search with aggregation duration NONE:**

            >>> from basin3d.plugins import usgs
            >>> from basin3d import synthesis
            >>> synthesizer = synthesis.register()
            >>> timeseries = synthesizer.measurement_timeseries_tvp_observations(monitoring_feature=["USGS-09110990", "USGS-09111250"],observed_property=['RDC','WT'],start_date='2020-04-01',end_date='2020-04-30',aggregation_duration='NONE')
            >>> for timeseries in timeseries:
            ...    print(f"{timeseries.feature_of_interest.id} - {timeseries.observed_property.get_basin3d_vocab()}")
            USGS-09110990 - RDC
            USGS-09111250 - RDC

        :param query: (optional) :class:`basin3d.core.schema.query.QueryMeasurementTimeseriesTVP` object
        :param kwargs: (required) Measurement Timeseries TVP Query parameters. See Query info below.
        :return: a :class:`~basin3d.core.synthesis.DataSourceModelIterator` that yields :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` objects

        .. note::
            **Measurement Timeseries TVP Query parameters**
            :class:`basin3d.core.schema.query.QueryMeasurementTimeseriesTVP`

            | Required arguments:
            |
            |    * **monitoring_feature** List of monitoring features id(s)
            |    * **observed_property** List of observed property(ies), i.e., BASIN-3D observed property vocabulary. See :func:`basin3d.synthesis.DataSynthesizer.observed_properties`
            |    * **start_date** Start date YYYY-MM-DD
            |
            | Optional arguments:
            |
            |    * **end_date** End date YYYY-MM-DD
            |    * **aggregation_duration** A single aggregation duration :class:`basin3d.core.schema.enum.AggregationDurationEnum` (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|NONE)
            |    * **statistic** List of statistic(s) :class:`basin3d.core.schema.enum.StatisticEnum` (MEAN|MIN|MAX|INSTANTANEOUS)
            |    * **result_quality** List of result quality(ies) :class:`basin3d.core.schema.enum.ResultQualityEnum` (VALIDATED|UNVALIDATED|SUSPECTED|REJECTED|ESTIMATED)
            |    * **sampling_medium** List of sampling medium(s) :class:`basin3d.core.schema.enum.SamplingMediumEnum` (SOLID_PHASE|WATER|GAS|OTHER)
            |    * **datasource** A single data source id prefix


        .. note::
            **Measurement Timeseries TVP Observation attributes**
            :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation`

            | Attributes values are dependent on data source features.
            |
            | :class:`basin3d.core.models.MappedAttribute` are returned for several attributes so that the data source values are also available.
            |
            |    * **id** Observation identifier
            |    * **type** Type of observation: MEASUREMENT_TVP_TIMESERIES
            |    * **observed_property** The observation's observed property :class:`basin3d.core.models.MappedAttribute`
            |    * **datasource** The data source :class:`basin3d.core.models.DataSource`
            |    * **sampling_medium** Observed property sampling medium :class:`basin3d.core.models.MappedAttribute` (SOLID_PHASE, WATER, GAS, OTHER)
            |    * **phenomenon_time** Datetime of the observation, for a timeseries the start and end times can be provided
            |    * **utc_offset** Coordinate Universal Time offset in hours (offset in hours), e.g., +9
            |    * **feature_of_interest** Monitoring Feature object :class:`basin3d.core.models.MonitoringFeature`, feature on which the observation is being made
            |    * **feature_of_interest_type** Feature type of the feature of interest, :class:`basin3d.core.schema.enum.FeatureTypeEnum`
            |    * **result** Observed values of the observed property being assessed, and (opt) their result quality, :class:`basin3d.core.models.ResultListTVP`
            |    * **time_reference_position** Position of timestamp in aggregated_duration (START, MIDDLE, END)
            |    * **aggregation_duration** Time period represented by observation :class:`basin3d.core.models.MappedAttribute` (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
            |    * **unit_of_measurement** Units in which the observation is reported
            |    * **statistic** Statistical property of the observation result :class:`basin3d.core.models.MappedAttribute` (MEAN, MIN, MAX, TOTAL)
            |    * **result_quality** List quality assessment found in the results :class:`basin3d.core.models.MappedAttribute` (VALIDATED, UNVALIDATED, SUSPECTED, REJECTED, ESTIMATED)


        """
        if not query:
            # Raises validation errors
            query = QueryMeasurementTimeseriesTVP(**kwargs)

        #  mypy casts are only used as hints for the type checker,
        #  and they don’t perform a runtime type check.
        return cast(DataSourceModelIterator,
                    self._measurement_timeseries_tvp_observation_access.list(
                        query))
