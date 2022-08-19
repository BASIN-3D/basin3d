"""
`basin3d.synthesis`
*******************

.. currentmodule:: basin3d.synthesis

:synopsis: BASIN-3D Synthesis API
:module author: Val Hendrix <vhendrix@lbl.gov>, Danielle Svehla Christianson <dschristianson@lbl.gov>. Catherine Wong <catwong@lbl.gov>


Functions
----------------
* :func:`register` - Register the specified plugins or implicitly register loaded plugins

Utility Classes
---------------
* :py:class:`TimeseriesOutputType` - Enumeration for :func:`get_timeseries_data` output types
* :py:exc:`SynthesisException` - Special Exception for Synthesis module

Classes
--------
* :class:`DataSynthesizer` - Synthesis API

synthesis.DataSynthesizer Functions
-----------------------------------

* :func:`DataSynthesizer.measurement_timeseries_tvp_observations`- Search for Measurement Timeseries TVP Observation (Instantaneous and Daily Values supported) from USGS Monitoring features and observed property variables
* :func:`DataSynthesizer.monitoring_features`- Search for all USGS monitoring features, USGS points by parent monitoring features, or look for a single monitoring feature by id.
* :func:`DataSynthesizer.observed_properties`- Search for observed properties
* :func:`DataSynthesizer.observed_property_variables`- Common names for observed property variables. An observed property variable defines what is being measured. Data source observed property variables are mapped to these synthesized observed property variables.

----------------------------------
"""
from importlib import import_module
from typing import List, Union, cast

from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import DataSource
from basin3d.core.plugin import PluginMount
from basin3d.core.schema.query import QueryById, QueryMeasurementTimeseriesTVP, \
    QueryMonitoringFeature, SynthesisResponse
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

    def observed_properties(self, datasource_id=None, variable_names=None):
        """
        Search for observed properties

        :param datasource_id: Unique feature identifier of datasource
        :param variable_names: Observed property variables
        :return: a list of observed properties

        """
        return self._catalog.find_observed_properties(datasource_id, variable_names)

    def observed_property_variables(self, datasource_id=None):
        """

        >>> from basin3d.plugins import usgs
        >>> from basin3d import synthesis
        >>> synthesizer = synthesis.register()
        >>> response = synthesizer.observed_property_variables(datasource_id='USGS')
        >>> for opv in response:
        ...     print(opv)
        pH
        River Discharge
        Water Level Elevation
        Water Temperature
        Dissolved Oxygen (DO)
        Specific Conductance (SC)
        Total Dissolved Solids (TDS)
        ...


        Common names for observed property variables. An observed property variable defines what is being measured.
        Data source observed property variables are mapped to these synthesized observed property variables.

        :param datasource_id: filter observer properity variables by data source
        :return: a list of observed property variables

        """
        return self._catalog.find_observed_property_variables(datasource_id=datasource_id)

    def monitoring_features(self, query: Union[QueryById, QueryMonitoringFeature] = None, **kwargs) -> Union[
            DataSourceModelIterator, SynthesisResponse]:
        """
        Search for all USGS monitoring features, USGS points by parent monitoring features, or look for a single monitoring feature by id.

        To see feature types for a given plugin: **<plugin_module>.<plugin_class>.feature_types**


        **Search for a single monitoring feature by id:**

        >>> from basin3d.plugins import usgs
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

        >>> for mf in synthesizer.monitoring_features(feature_type='point',parent_features=['USGS-17040101']): # doctest: +ELLIPSIS
        ...    print(f"{mf.id} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        USGS-13010000 [(-110.6647222, 44.1336111)]
        USGS-13010065 [(-110.6675, 44.09888889)]
        USGS-13010450 [(-110.5874305, 43.9038296)]
        ...

        **Unsupported feature types warning:**

        The code below is an example of what you will see if a registered plugin does not support
        the requested feature type.

        >>> response_itr = synthesizer.monitoring_features(feature_type='horizontal path')
        >>> for mf in response_itr:
        ...   print(mf)
        ...


        **Output warning messages from the returned iterator**

        This is an example of checking the synthesis response messages in the :class:`basin3d.core.synthesis.DataSourceModelIterator`.

        >>> response_itr.synthesis_response.messages
        [SynthesisMessage(msg='Feature type HORIZONTAL PATH not supported by USGS.', level='WARN', where=['USGS', 'MonitoringFeature'])]


        :param query: (optional) The Monitoring Feature Query object
        :param id: (optional) Unique feature identifier. This returns a single monitoring feature
        :param feature_type: (optional) feature type
        :param datasource: (optional) Datasource id prefix (e.g USGS)
        :param monitoring_features: (optional) List of monitoring feature identifiers (eg. USGS-0010)
        :param parent_features: (optional) List of parent monitoring features to search by


        :return: a single :class:`~basin3d.core.schema.query.SynthesisResponse` for a query by id
            or a :class:`~basin3d.core.synthesis.DataSourceModelIterator` for multple.
        """
        if not query:
            if "id" in kwargs and isinstance(kwargs["id"], str):
                query = QueryById(**kwargs)
            else:
                query = QueryMonitoringFeature(**kwargs)

        # Search for single or list?
        if isinstance(query, QueryById):
            #  mypy casts are only used as hints for the type checker,
            #  and they don’t perform a runtime type check.
            return cast(SynthesisResponse, self._monitoring_feature_access.retrieve(query=query))
        else:
            #  mypy casts are only used as hints for the type checker,
            #  and they don’t perform a runtime type check.
            return cast(DataSourceModelIterator,
                        self._monitoring_feature_access.list(query=query))

    def measurement_timeseries_tvp_observations(self, query: QueryMeasurementTimeseriesTVP = None, **kwargs) -> \
            DataSourceModelIterator:
        """
        Search for Measurement Timeseries TVP Observation from USGS Monitoring features and observed property variables.

        Aggregation Duration for DAY and NONE are both supported. DAY will call USGS Daily Values Service and NONE will call USGS Instantaneous Values Service.

            **Search with aggregation duration DAY (Daily Values Service):**

            >>> from basin3d.plugins import usgs
            >>> from basin3d import synthesis
            >>> synthesizer = synthesis.register()
            >>> timeseries = synthesizer.measurement_timeseries_tvp_observations(monitoring_features=['USGS-09110990'],observed_property_variables=['RDC','WT'],start_date='2019-10-01',end_date='2019-10-30',aggregation_duration='DAY')
            >>> for timeseries in timeseries:
            ...    print(f"{timeseries.feature_of_interest.id} - {timeseries.observed_property_variable}")
            USGS-09110990 - RDC

            **Search with aggregation duration NONE (Instantaneous Values Service):**

            >>> from basin3d.plugins import usgs
            >>> from basin3d import synthesis
            >>> synthesizer = synthesis.register()
            >>> timeseries = synthesizer.measurement_timeseries_tvp_observations(monitoring_features=["USGS-09110990", "USGS-09111250"],observed_property_variables=['RDC','WT'],start_date='2020-04-01',end_date='2020-04-30',aggregation_duration='NONE')
            >>> for timeseries in timeseries:
            ...    print(f"{timeseries.feature_of_interest.id} - {timeseries.observed_property_variable}")
            USGS-09110990 - RDC
            USGS-09111250 - RDC


        :param query: The query information for this function
        :return: generator that yields MeasurementTimeseriesTVPObservations

        """
        if not query:
            # Raises validation errors
            query = QueryMeasurementTimeseriesTVP(**kwargs)

        #  mypy casts are only used as hints for the type checker,
        #  and they don’t perform a runtime type check.
        return cast(DataSourceModelIterator,
                    self._measurement_timeseries_tvp_observation_access.list(
                        query))


