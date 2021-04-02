"""
`basin3d.synthesis`
****************************

.. currentmodule:: basin3d.synthesis

:synopsis: BASIN-3D Synthesis API
:module author: Val Hendrix <vhendrix@lbl.gov>, Danielle Svehla Christianson <dschristianson@lbl.gov>. Catherine Wong <catwong@lbl.gov>


Functions
----------------
* :func:`register` - Register the specified plugins or implicitly register loaded plugins
* :func:`get_timeseries_data` - Wrapper for DataSynthesizer.get_data for timeseries data types. Currently only MeasurementTimeseriesTVPObservations are supported.

Exceptions
-----------
* :py:exc:`SynthesisException` - Special Exception for Synthesis module


synthesis.DataSynthesizer
---------------------------
Classes
--------
* :class:`DataSynthesizer` - Synthesis API

Functions
----------
* :func:`DataSynthesizer.measurement_timeseries_tvp_observations`- Search for Measurement Timeseries TVP Observation from USGS Monitoring features and observed property variables
* :func:`DataSynthesizer.monitoring_features`- Search for all USGS monitoring features, USGS points by parent monitoring features, or look for a single monitoring feature by id.
* :func:`DataSynthesizer.observed_properties`- Search for observed properties
* :func:`DataSynthesizer.observed_property_variables`- Common names for observed property variables. An observed property variable defines what is being measured. Data source observed property variables are mapped to these synthesized observed property variables.

----------------------------------
"""
import datetime as dt
import json
import os
import pandas as pd
import tempfile

# Method to query (write query in basin3d language)
from importlib import import_module
from typing import Iterator, List, Union, cast, Tuple

# Get an instance of a logger
from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import DataSource, MonitoringFeature, MeasurementTimeseriesTVPObservation, TimeMetadataMixin
from basin3d.core.plugin import PluginMount
from basin3d.core.synthesis import MeasurementTimeseriesTVPObservationAccess, MonitoringFeatureAccess, logger, \
    QUERY_PARAM_MONITORING_FEATURES, QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES, QUERY_PARAM_START_DATE
from basin3d.core.types import TimeFrequency


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

        Common names for observed property variables. An observed property variable defines what is being measured. Data source observed property variables are mapped to these synthesized observed property variables.

        :param datasource_id: filter observer properity variables by data source
        :return: a list of observed property variables

        """
        return self._catalog.find_observed_property_variables(datasource_id=datasource_id)

    def monitoring_features(self, id: str = None, feature_type: str = None, datasource: str = None,
                            monitoring_features: List[str] = None, parent_features: List[str] = None) -> Union[
        Iterator[MonitoringFeature], MonitoringFeature]:
        """
        Search for all USGS monitoring features, USGS points by parent monitoring features, or look for a single monitoring feature by id.

        To see feature types for a given plugin: **<plugin_module>.<plugin_class>.feature_types**


        **Search for a single monitoring feature by id:**

        >>> from basin3d.plugins import usgs
        >>> from basin3d import synthesis
        >>> synthesizer = synthesis.register()
        >>> mf = synthesizer.monitoring_features(id='USGS-0101')
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

        :param id: Unique feature identifier
        :param feature_type: feature type
        :param datasource: Datasource id prefix (e.g USGS)
        :param monitoring_features: List of monitoring feature identifiers (eg. USGS-0010)
        :param parent_features: List of parent monitoring features to search by

        :return: a single `MonitoringFeature` or a list
        """

        # Search for single or list?
        if id:
            #  mypy casts are only used as hints for the type checker,
            #  and they don’t perform a runtime type check.
            return cast(MonitoringFeature, self._monitoring_feature_access.retrieve(pk=id))
        else:
            #  mypy casts are only used as hints for the type checker,
            #  and they don’t perform a runtime type check.
            return cast(Iterator[MonitoringFeature],
                        self._monitoring_feature_access.list(feature_type=feature_type, datasource=datasource,
                                                             monitoring_features=monitoring_features,
                                                             parent_features=parent_features))

    def measurement_timeseries_tvp_observations(
            self, monitoring_features: List[str], observed_property_variables: List[str], start_date: str,
            end_date: str = None, aggregation_duration: str = TimeMetadataMixin.AGGREGATION_DURATION_DAY,
            results_quality: str = None, datasource: str = None) -> Iterator[MeasurementTimeseriesTVPObservation]:
        """
        Search for Measurement Timeseries TVP Observation from USGS Monitoring features and observed property variables

            >>> from basin3d.plugins import usgs
            >>> from basin3d import synthesis
            >>> synthesizer = synthesis.register()
            >>> timeseries = synthesizer.measurement_timeseries_tvp_observations(monitoring_features=['USGS-09110990'], \
                observed_property_variables=['RDC','WT'], start_date='2019-10-01', end_date='2019-10-30', \
                aggregation_duration='DAY')
            >>> for timeseries in timeseries:
            ...    print(f"{timeseries.feature_of_interest.id} - {timeseries.observed_property_variable}")
            USGS-09110990 - RDC

        :param monitoring_features: List of monitoring_features ids (eg. USGS-09110990)
        :param observed_property_variables: List of observed property variable ids (basin3d variable names)
        :param start_date: start date YYYY-MM-DD
        :param end_date: end date YYYY-MM-DD
        :param aggregation_duration: aggregation time period, default = 'DAY' enum (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)
        :param results_quality: enum (UNCHECKED|CHECKED)
        :param datasource: Datasource id prefix (e.g USGS)

        :return: generator that yields MeasurementTimeseriesTVPObservations

        """
        if not monitoring_features or not observed_property_variables or not start_date:
            logger.error('Values for one or more of the requred variables was not provided: '
                         'monitoring_features, observed_property_variables, start_date.')
            raise SynthesisException
        #  mypy casts are only used as hints for the type checker,
        #  and they don’t perform a runtime type check.
        return cast(Iterator[MeasurementTimeseriesTVPObservation],
                    self._measurement_timeseries_tvp_observation_access.list(
                        monitoring_features=monitoring_features,
                        observed_property_variables=observed_property_variables,
                        start_date=start_date, end_date=end_date, aggregation_duration=aggregation_duration,
                        datasource=datasource, results_quality=results_quality))


def get_timeseries_data(synthesizer: DataSynthesizer, temporal_resolution: str = 'DAY', **kwargs) -> Tuple[
    Union[pd.DataFrame, None], dict]:
    """

    Wrapper for *DataSynthesizer.get_data* for timeseries data types. Currently only *MeasurementTimeseriesTVPObservations* are supported.

    >>> from basin3d.plugins import usgs
    >>> from basin3d import synthesis
    >>> synthesizer = synthesis.register()
    >>> usgs_df, usgs_metadata = synthesis.get_timeseries_data(synthesizer, monitoring_features=['USGS-09110000'], observed_property_variables=['RDC','WT'], start_date='2019-10-25', end_date='2019-10-30')
    >>> usgs_df
                TIMESTAMP  USGS-09110000__WT  USGS-09110000__RDC
    2019-10-25 2019-10-25                3.2            4.247527
    2019-10-26 2019-10-26                4.1            4.219210
    2019-10-27 2019-10-27                4.3            4.134260
    2019-10-28 2019-10-28                3.2            4.332478
    2019-10-29 2019-10-29                2.2            4.219210
    2019-10-30 2019-10-30                0.5            4.247527

    >>> for k, v in usgs_metadata['USGS-09110000__WT'].items():
    ...     print(f'{k} = {v}')
    data_start = 2019-10-25 00:00:00
    data_end = 2019-10-30 00:00:00
    records = 6
    units = deg C
    basin_3d_variable = WT
    basin_3d_variable_full_name = Water Temperature
    statistic = MEAN
    temporal_aggregation = DAY
    quality = CHECKED
    sampling_medium = WATER
    sampling_feature_id = USGS-09110000
    datasource = USGS
    datasource_variable = 00010

    :param synthesizer: DataSnythesizer object
    :param temporal_resolution: temporal resolution of output (in future, we can be smarter about this, e.g., detect it from the results or average higher frequency data)
    :param kwargs:
           Required parameters for a *MeasurementTimeseriesTVPObservation*:
               * **monitoring_features**
               * **observed_property_variables**
               * **start_date**
           Optional parameters for *MeasurementTimeseriesTVPObservation*:
               * **end_date**
               * **aggregation_duration** = resolution = DAY  (only DAY is currently supported)
               * **result_quality**
               * **datasource**

    :return:
         a Tuple: synthesized data with timestamp, monitoring feature id, observed property variable id in a pandas DataFrame (optional) and metadata dictionary.

         Optional means a return value of None for the pandas DataFrame if no arguments are passed into **kwargs. The min. required arguments to return a pandas DataFrame are monitoring features, observed property variables, and start date


            **[optional] pandas dataframe:** with synthesized data of timestamp, monitoring feature, and observed property variable id ::

                                TIMESTAMP  USGS-09110000__WT  USGS-09110000__RDC
                    2019-10-25 2019-10-25                3.2            4.247527
                    2019-10-26 2019-10-26                4.1            4.219210
                    2019-10-27 2019-10-27                4.3            4.134260
                    2019-10-28 2019-10-28                3.2            4.332478
                    2019-10-29 2019-10-29                2.2            4.219210
                    2019-10-30 2019-10-30                0.5            4.247527

                    # timestamp column: datetime, repr as ISO format
                    column name format = f'{start_date end_date}

                    # data columns: monitoring feature id and observed property variable id
                    column name format = f'{monitoring_feature_id}__{observed_property_variable_id}'


            **metadata dictionary**::

                key = f'{monitoring_feature_id}__{observed_property_variable_id}',
                value =
                {
                    data_start = str
                    data_end = str
                    records = int
                    units = str
                    basin_3d_variable = str
                    basin_3d_variable_full_name = str
                    statistic = str
                    temporal_aggregation = str
                    quality = str
                    sampling_medium = str
                    sampling_feature_id = str
                    datasource = str
                    datasource_variable
                }
    """
    # Check that required parameters are provided. May have to rethink this when we expand to mulitple observation types
    if not all([QUERY_PARAM_MONITORING_FEATURES in kwargs, QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES in kwargs,
                QUERY_PARAM_START_DATE in kwargs]):
        logger.error(f'One or more of the required parameters: {QUERY_PARAM_MONITORING_FEATURES}, '
                     f'{QUERY_PARAM_OBSERVED_PROPERTY_VARIABLES}, or {QUERY_PARAM_START_DATE} was not provided.')
        raise SynthesisException

    # For now set the aggregation_duration to match the resolution
    # ToDo: expand to detect from results and/or aggregate higher-resolution data to specified resolution
    kwargs['aggregation_duration'] = temporal_resolution

    # Get the data
    data_generator = synthesizer.measurement_timeseries_tvp_observations(**kwargs)

    metadata_store = {}
    first_timestamp = dt.datetime.now()
    last_timestamp = dt.datetime(1990, 1, 1)
    has_results = False

    # By using the temporary directory, all the files are eventually removed when the directory is removed.
    with tempfile.TemporaryDirectory() as temp_wd:

        for data_obj in data_generator:

            # Collect stats
            feature_of_interest = data_obj.feature_of_interest  # In future will need feature of interest as a separate obj
            sampling_feature_id = feature_of_interest.id
            observed_property_variable_id = data_obj.observed_property_variable
            aggregation_duration = data_obj.aggregation_duration
            # Double check that returned aggregation_duration matches resolution. They should be the same.
            if aggregation_duration != temporal_resolution:
                logger.warning(f'Results aggregation_duration {aggregation_duration} does not match '
                               f'specified temporal_resolution {temporal_resolution}.')
                continue

            synthesized_variable_name = f'{sampling_feature_id}__{observed_property_variable_id}'

            results_start = None
            results_end = None
            results = data_obj.result_points
            if results:
                results_start = results[0][0]
                results_end = results[-1][0]
                iso_format = '%Y-%m-%dT%H:%M:%S.%f'
                if isinstance(results_start, str):
                    results_start = dt.datetime.strptime(results_start, iso_format)
                if isinstance(results_end, str):
                    results_end = dt.datetime.strptime(results_end, iso_format)
                if results_start < first_timestamp:
                    first_timestamp = results_start
                if results_end > last_timestamp:
                    last_timestamp = results_end

            # Collect rest of variable metadata and store it
            # ToDo: other metadata files
            observed_property = data_obj.observed_property
            metadata_store[synthesized_variable_name] = {
                'data_start': results_start,
                'data_end': results_end,
                'records': len(results),
                'units': data_obj.unit_of_measurement,
                'basin_3d_variable': observed_property_variable_id,
                'basin_3d_variable_full_name': observed_property.observed_property_variable.full_name,
                'statistic': data_obj.statistic,
                'temporal_aggregation': aggregation_duration,
                'quality': data_obj.result_quality,
                'sampling_medium': observed_property.sampling_medium,
                'sampling_feature_id': sampling_feature_id,
                'datasource': data_obj.datasource.name,
                'datasource_variable': observed_property.datasource_variable}

            if not results:
                logger.info(f'{synthesized_variable_name} returned no data.')
                continue

            has_results = True

            # Write results to temp file
            with open(os.path.join(temp_wd, f'{synthesized_variable_name}.json'), mode='w') as f:
                f.write('{')
                f.write(f'"{results[0][0]}": {results[0][1]}')
                for result in results[1:]:
                    f.write(f',"{result[0]}": {result[1]}')
                f.write('}')

        if not has_results:
            return None, metadata_store

        # Prep the dataframe
        time_index = pd.date_range(first_timestamp, last_timestamp,
                                   freq=TimeFrequency.PANDAS_FREQUENCY_MAP[temporal_resolution])
        time_series = pd.Series(time_index, index=time_index)
        output_df = pd.DataFrame({'TIMESTAMP': time_series})
        # ToDo: expand to have TIMESTAMP_START and TIMESTAMP_END for resolutions HOUR, MINUTE

        # Fill the dataframe
        for synthesized_variable_name in metadata_store.keys():
            num_records = metadata_store[synthesized_variable_name]['records']
            if num_records == 0:
                continue
            file_path = os.path.join(temp_wd, f'{synthesized_variable_name}.json')
            with open(file_path, mode='r') as f:
                result_dict = json.load(f)
                pd_series = pd.Series(result_dict, name=synthesized_variable_name)
                output_df = output_df.join(pd_series)
                logger.info(f'Added variable {synthesized_variable_name} with {num_records} records.')

    return output_df, metadata_store
