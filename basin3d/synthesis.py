"""
`basin3d.synthesis`
*******************

.. currentmodule:: basin3d.synthesis

:synopsis: BASIN-3D Synthesis API
:module author: Val Hendrix <vhendrix@lbl.gov>, Danielle Svehla Christianson <dschristianson@lbl.gov>. Catherine Wong <catwong@lbl.gov>


Functions
----------------
* :func:`register` - Register the specified plugins or implicitly register loaded plugins
* :func:`get_timeseries_data` - Wrapper for DataSynthesizer.get_data for timeseries data types. Currently only *MeasurementTimeseriesTVPObservations* with DAY aggregation is supported for getting synthesized timeseries data.

Utility Classes
---------------
* :py:class:`TimeseriesOutputType` - Enumeration for :func:`get_timeseries_data` output types
* :py:exc:`SynthesisException` - Special Exception for Synthesis module

Classes
--------
* :class:`DataSynthesizer` - Synthesis API
* :class:`SynthesizedTimeseriesData` - Base class for Synthesized Timeseries Data Class
* :class:`PandasTimeseriesData` - Synthesized Timeseries Data Class in Pandas format
* :class:`HDFTimeseriesData` - Synthesized Timeseries Data Class in HDF5 format
* :class:`QueryInfo` - Information about a BASIN-3D query execution


synthesis.DataSynthesizer Functions
-----------------------------------

* :func:`DataSynthesizer.measurement_timeseries_tvp_observations`- Search for Measurement Timeseries TVP Observation (Instantaneous and Daily Values supported) from USGS Monitoring features and observed property variables
* :func:`DataSynthesizer.monitoring_features`- Search for all USGS monitoring features, USGS points by parent monitoring features, or look for a single monitoring feature by id.
* :func:`DataSynthesizer.observed_properties`- Search for observed properties
* :func:`DataSynthesizer.observed_property_variables`- Common names for observed property variables. An observed property variable defines what is being measured. Data source observed property variables are mapped to these synthesized observed property variables.

----------------------------------
"""
import datetime as dt
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from typing import Generator, List, Union, cast

import h5py
import pandas as pd

from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import DataSource
from basin3d.core.plugin import PluginMount
from basin3d.core.schema.enum import PANDAS_TIME_FREQUENCY_MAP, StatisticEnum, TimeFrequencyEnum
from basin3d.core.schema.query import QueryBase, QueryById, QueryMeasurementTimeseriesTVP, \
    QueryMonitoringFeature, SynthesisResponse
from basin3d.core.synthesis import DataSourceModelIterator, MeasurementTimeseriesTVPObservationAccess, \
    MonitoringFeatureAccess, logger


class SynthesisException(Exception):
    """Special Exception for Synthesis module"""
    pass


class TimeseriesOutputType(Enum):
    """Enumeration of synthesized time series output types"""

    PANDAS = 0
    HDF = 1
    JSON = 2


@dataclass
class QueryInfo:
    """
    basin3d query information.
    """

    """The start time of the basin3d query"""
    start_time: dt.datetime

    """The end time of the successful completion of a basin3d query"""
    end_time: Union[dt.datetime, None]

    """The query parameters of the basin3d query"""
    parameters: QueryBase


@dataclass
class SynthesizedTimeseriesData:
    """
    Base class used in the return for :func:`get_timeseries_data` function.

    This class contains synthesized data with timestamp, monitoring feature id,
    observed property variable id in a pandas DataFrame (optional) and metadata dictionary (optional).

    """

    data: pd.DataFrame
    """The time series data
    **pandas dataframe:** with synthesized data of timestamp, monitoring feature, and observed property variable id

    .. code-block::

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


    """

    metadata: pd.DataFrame
    """Metadata for the observations that have data"""

    metadata_no_observations: pd.DataFrame
    """Metadata for the observations that have **no** data"""

    output_path: Union[str, None]
    """The file path where the output data was saved, if exists. Optional means a return value of ``None``."""

    query: QueryInfo
    """Time series synthesis query information"""

    @property
    def variables(self) -> list:
        """List of synthesized variable names with recorded observations"""
        return []

    @property
    def variables_no_observations(self) -> list:
        """List of synthesized variable names with **no** recorded observations"""
        return []


@dataclass
class PandasTimeseriesData(SynthesizedTimeseriesData):
    """
    Class for Pandas time series data
    """

    @property
    def variables(self) -> list:
        """List of synthesized variable names with recorded observations"""
        return [c for c in self.metadata.columns if c != 'TIMESTAMP']

    @property
    def variables_no_observations(self) -> list:
        """List of synthesized variable names with **no** recorded observations"""
        return [c for c in self.metadata_no_observations.columns if c != 'TIMESTAMP']


@dataclass(init=False)
class HDFTimeseriesData(SynthesizedTimeseriesData):
    """
    Class for HDF5 time series data. The underlying
    data storage is an HDF5 file
    """
    hdf: h5py.File

    def __init__(self, hdf: h5py.File, output_path: str, query: QueryInfo):
        """
        Time Series data stored as HDF files.

        :param hdf: The hdf timeseries dafile
        :param output_path:
        :param query:
        """
        self.hdf = hdf
        self.output_path = output_path
        self.query = query

    @property
    def data(self) -> pd.DataFrame:
        """The time series data as a pandas dataframe"""
        return pd.read_hdf(self.hdf.filename, key='data')

    @property
    def metadata(self) -> pd.DataFrame:
        """Metadata for the observations that have data"""
        return pd.read_hdf(self.hdf.filename, key='metadata').T

    @property
    def metadata_no_observations(self) -> pd.DataFrame:
        """Metadata for the observations that have **no** data"""
        return pd.read_hdf(self.hdf.filename, key='metadata_no_observations').T

    @property
    def variables(self) -> list:
        """List of synthesized variable names with recorded observations"""
        return [c for c in self.hdf['metadata'].attrs['column_names'] if c != 'TIMESTAMP']

    @property
    def variables_no_observations(self) -> list:
        """List of synthesized variable names with **no** recorded observations"""
        return [c for c in self.hdf['metadata_no_observations'].attrs['column_names'] if c != 'TIMESTAMP']


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


def get_timeseries_data(synthesizer: DataSynthesizer, location_lat_long: bool = True,
                        temporal_resolution: str = None,
                        output_path: str = None, output_name: str = None, cleanup: bool = True,
                        output_type: TimeseriesOutputType = TimeseriesOutputType.PANDAS,
                        **kwargs) -> SynthesizedTimeseriesData:
    """

    Wrapper for *DataSynthesizer.get_data* for timeseries data types.
    Currently only *MeasurementTimeseriesTVPObservations* with DAY aggregation is supported for getting synthesized timeseries data.

    >>> from basin3d.plugins import usgs
    >>> from basin3d import synthesis
    >>> synthesizer = synthesis.register()
    >>> usgs_data = synthesis.get_timeseries_data( \
        synthesizer,monitoring_features=['USGS-09110000'], observed_property_variables=['RDC','WT'], \
        start_date='2019-10-25', end_date='2019-10-30', statistic=['MEAN'])
    >>> usgs_data.data
                TIMESTAMP  USGS-09110000__WT__MEAN  USGS-09110000__RDC__MEAN
    2019-10-25 2019-10-25                      3.2                  4.247527
    2019-10-26 2019-10-26                      4.1                  4.219210
    2019-10-27 2019-10-27                      4.3                  4.134260
    2019-10-28 2019-10-28                      3.2                  4.332478
    2019-10-29 2019-10-29                      2.2                  4.219210
    2019-10-30 2019-10-30                      0.5                  4.247527

    >>> for k, v in usgs_data.metadata['USGS-09110000__WT__MEAN'].items():
    ...     print(f'{k} = {v}')
    data_start = 2019-10-25 00:00:00
    data_end = 2019-10-30 00:00:00
    records = 6
    units = deg C
    basin_3d_variable = WT
    basin_3d_variable_full_name = Water Temperature
    statistic = MEAN
    temporal_aggregation = DAY
    quality = VALIDATED
    sampling_medium = WATER
    sampling_feature_id = USGS-09110000
    sampling_feature_name = TAYLOR RIVER AT ALMONT, CO.
    datasource = USGS
    datasource_variable = 00010
    sampling_feature_lat = 38.66443715
    sampling_feature_long = -106.8453172
    sampling_feature_lat_long_datum = NAD83
    sampling_feature_altitude = 8010.76
    sampling_feature_alt_units = None
    sampling_feature_alt_datum = NGVD29

    :param synthesizer: DataSnythesizer object
    :param location_lat_long: boolean: True = look for lat, long, elev coordinates and return in the metadata, False = ignore
    :param temporal_resolution: temporal resolution of output (in future, we can be smarter about this, e.g., detect it from the results or average higher frequency data)
    :param output_path: directory to place any output/intermediate data.  if None, will create a temporary path.
    :param output_name: name to give output file and/or intermediate directory. if None, output name will be timestamp (%Y%m%dT%H%M%S)
    :param cleanup: if True, this will remove intermediate data generated
    :param output_type: format for output data

    :param kwargs: The minimum required arguments to return a pandas DataFrame are monitoring features, observed property variables, and start date

           Required parameters for a *MeasurementTimeseriesTVPObservation*:
               * **monitoring_features (list)**
               * **observed_property_variables (list)**
               * **start_date**
           Optional parameters for *MeasurementTimeseriesTVPObservation*:
               * **end_date**
               * **aggregation_duration** = resolution in (SECOND, MINUTE, HOUR, DAY, MONTH)  Default: DAY
               * **statistic (list)**
               * **result_quality (list)**
               * **datasource**

    :return: A Synthesized Timeseries Data Class

    """

    # Check that required parameters are provided. May have to rethink this when we expand to mulitple observation types
    if temporal_resolution:
        kwargs['aggregation_duration'] = temporal_resolution
    query = QueryMeasurementTimeseriesTVP(**kwargs)

    # Get the data
    query_info = QueryInfo(dt.datetime.now(), None, query)
    data_generator = synthesizer.measurement_timeseries_tvp_observations(query=query)
    query_info.end_time = dt.datetime.now()

    # Store for all variable metadata
    metadata_store = {}

    # data start and end dates for all results
    # Note that all timestamps should have Timezone removed.
    first_timestamp = None
    last_timestamp = None
    has_results = False

    # Prepare working directory
    if output_path:
        if not os.path.exists(output_path):
            raise SynthesisException(f"Specified output directory '{output_path}' does not exist.")

        working_directory = output_path
    else:
        working_directory = tempfile.mkdtemp()

    # Prepare directory for raw output from basin3d
    output_name = output_name or query_info.start_time.strftime("%Y%m%dT%H%M%S")
    intermediate_directory = os.path.join(working_directory, output_name)
    os.mkdir(intermediate_directory)

    output: SynthesizedTimeseriesData = SynthesizedTimeseriesData(data=None, metadata_no_observations=None,
                                                                  metadata=None,
                                                                  output_path=not cleanup and working_directory or output_path,
                                                                  query=query_info)
    try:
        # if using the temporary directory, all the files are eventually removed when the directory is removed.
        for data_obj in data_generator:

            # Collect stats
            feature_of_interest = data_obj.feature_of_interest  # In future will need feature of interest as a separate obj
            sampling_feature_id = feature_of_interest.id
            observed_property_variable_id = data_obj.observed_property_variable
            aggregation_duration = data_obj.aggregation_duration
            # Double check that returned aggregation_duration matches resolution. They should be the same.
            statistic = data_obj.statistic
            # Double check that returned statistic matches any statistic specified.
            if query.statistic and statistic not in query.statistic:
                logger.warning(
                    f'Results statistic {statistic} not in the specified query statistic(s): '
                    f'{query.statistic}.')
            # convert result_quality from a list to str if there are quality values
            result_quality = None
            if data_obj.result_quality:
                result_quality = ';'.join(data_obj.result_quality)

            synthesized_variable_name = f'{sampling_feature_id}__{observed_property_variable_id}'
            # Only add statistic to the column name if it exists
            if statistic and statistic in StatisticEnum.values()[1:]:
                synthesized_variable_name += f"__{statistic}"

            results_start = None
            results_end = None
            records = 0
            results = data_obj.result.value
            if results:
                records = len(results)
                results_start = results[0][0]
                results_end = results[-1][0]
                if isinstance(results_start, str):
                    results_start = dt.datetime.fromisoformat(results_start).replace(tzinfo=None)
                if isinstance(results_end, str):
                    results_end = dt.datetime.fromisoformat(results_end).replace(tzinfo=None)
                if not first_timestamp or results_start < first_timestamp:
                    first_timestamp = results_start
                if not last_timestamp or results_end > last_timestamp:
                    last_timestamp = results_end

            # Collect rest of variable metadata and store it
            # ToDo: other metadata files
            observed_property = data_obj.observed_property
            metadata_store[synthesized_variable_name] = {
                'data_start': results_start,
                'data_end': results_end,
                'records': records,
                'units': data_obj.unit_of_measurement,
                'basin_3d_variable': observed_property_variable_id,
                'basin_3d_variable_full_name': observed_property.observed_property_variable.full_name,
                'statistic': statistic,
                'temporal_aggregation': aggregation_duration,
                'quality': result_quality,
                'sampling_medium': observed_property.sampling_medium,
                'sampling_feature_id': sampling_feature_id,
                'sampling_feature_name': feature_of_interest.name,
                'datasource': data_obj.datasource.name,
                'datasource_variable': observed_property.datasource_variable}

            # not every observation type / sampling feature type will have simple lat long so set up with toggle
            #    we may need to modify this for broader applicaiton with BASIN-3D
            if location_lat_long:
                latitude, longitude, lat_long_datum, altitude, alt_units, alt_datum = None, None, None, None, None, None

                if len(feature_of_interest.coordinates.absolute.horizontal_position) > 0:
                    latitude = feature_of_interest.coordinates.absolute.horizontal_position[0].latitude
                    longitude = feature_of_interest.coordinates.absolute.horizontal_position[0].longitude
                    lat_long_datum = feature_of_interest.coordinates.absolute.horizontal_position[0].datum

                if len(feature_of_interest.coordinates.absolute.vertical_extent) > 0:
                    if feature_of_interest.coordinates.absolute.vertical_extent[0].type == 'ALTITUDE':
                        altitude = feature_of_interest.coordinates.absolute.vertical_extent[0].value
                        alt_units = feature_of_interest.coordinates.absolute.vertical_extent[0].distance_units
                        alt_datum = feature_of_interest.coordinates.absolute.vertical_extent[0].datum

                metadata_store[synthesized_variable_name].update(
                    {'sampling_feature_lat': latitude,
                     'sampling_feature_long': longitude,
                     'sampling_feature_lat_long_datum': lat_long_datum,
                     'sampling_feature_altitude': altitude,
                     'sampling_feature_alt_units': alt_units,
                     'sampling_feature_alt_datum': alt_datum
                     }
                )

            if not results:
                logger.info(f'{synthesized_variable_name} returned no data.')
                continue

            has_results = True

            # Write results to temp file
            with open(os.path.join(intermediate_directory, f'{synthesized_variable_name}.json'), mode='w') as f:
                f.write('{')
                f.write(f'"{results[0][0]}": {results[0][1]}')
                for result in results[1:]:
                    f.write(f',"{result[0]}": {result[1]}')
                f.write('}')

        if not has_results:
            return output

        # Determine requested output type
        if output_type is TimeseriesOutputType.PANDAS:
            output = _output_df(working_directory, output_name, query_info, metadata_store, first_timestamp,
                                last_timestamp,
                                query.aggregation_duration)
            if cleanup:
                # There will be no output data left
                output.output_path = None
        elif output_type is TimeseriesOutputType.HDF:
            output = _output_hdf(working_directory, output_name, query_info, metadata_store, first_timestamp,
                                 last_timestamp,
                                 query.aggregation_duration)
        else:
            raise NotImplemented

        return output
    finally:
        # Clean up
        if cleanup:
            # This is a temporary directory and
            # should be cleaned up
            shutil.rmtree(os.path.join(intermediate_directory))

        # This is a temp directory and not output was saved.
        if not output_path and not output.output_path:
            shutil.rmtree(working_directory)


def _output_df(output_directory, output_name, query_info, metadata_store, first_timestamp, last_timestamp,
               temporal_resolution) -> \
        PandasTimeseriesData:
    """
    Output timeseries data as a pandas data frame

    :param output_directory: directory that the query results are written to
    :param output_name: name to give output file. if None, output name will be timestamp (%Y%m%dT%H%M%S)
    :param query_info: The query information
    :param metadata_store: dictionary of all metadata for query
    :param first_timestamp: datetime for the first observation of all data
    :param last_timestamp: datetime for the last observation of all data
    :param temporal_resolution: timescale (e.g. DAY)
    :return: The synthesized time series data
    """
    # Prep the data dataframe
    time_index = pd.date_range(first_timestamp, last_timestamp,
                               freq=PANDAS_TIME_FREQUENCY_MAP[temporal_resolution], tz=None)
    # Remove the timezone
    time_index = time_index.tz_localize(None)
    time_series = pd.Series(time_index, index=time_index)
    output_df = pd.DataFrame({'TIMESTAMP': time_series})
    # ToDo: expand to have TIMESTAMP_START and TIMESTAMP_END for resolutions HOUR, MINUTE
    # Fill the data dataframe
    for synthesized_variable_name, result_dict in _result_generator(os.path.join(output_directory, output_name),
                                                                    metadata_store):
        num_records = metadata_store[synthesized_variable_name]['records']

        # Collect timestamps and values for the varialbe series
        # Remove timezone (offset) from tvp timestamps
        result_timestamps = [dt.datetime.fromisoformat(k).replace(tzinfo=None) for k in list(result_dict.keys())]
        result_values = list(result_dict.values())
        pd_series = pd.Series(data=result_values, index=result_timestamps, name=synthesized_variable_name)

        # Join the series to output dataframe
        output_df = output_df.join(pd_series, on='TIMESTAMP', rsuffix=synthesized_variable_name)
        logger.debug(f'Added variable {synthesized_variable_name} with {num_records} records.')

    # generate the metadata -- only separate metadata info with data
    # from metadata without metadata
    synthesized_var_list = list(output_df.columns)
    synthesized_no_var_list = list(set(metadata_store.keys()).difference(set(synthesized_var_list)))

    metadata_data_df = _fill_metadata_df(metadata_store, synthesized_var_list)
    metadata_nodata_df = _fill_metadata_df(metadata_store, synthesized_no_var_list)

    # Only Drop NaNs if temporal resolution is < 'DAY'
    if temporal_resolution in [TimeFrequencyEnum.MINUTE.value,
                               TimeFrequencyEnum.HOUR.value,
                               TimeFrequencyEnum.SECOND.value]:
        logger.info("Removing NaNs from data")
        output_df = output_df.dropna()

    if len(list(metadata_data_df.columns)) + len(list(metadata_nodata_df.columns)) - 2 != len(metadata_store):
        logger.warning('Metadata records mismatch. Please take a look')

    if not all(output_df.columns == metadata_data_df.columns):
        logger.warning('Data and metadata data frames columns do not match!')

    return PandasTimeseriesData(data=output_df, metadata=metadata_data_df, metadata_no_observations=metadata_nodata_df,
                                output_path=output_directory,
                                query=query_info)


def _output_hdf(output_directory: str, output_name: str, query_info: QueryInfo, metadata_store: dict, first_timestamp,
                last_timestamp,
                temporal_resolution) -> \
        SynthesizedTimeseriesData:
    """
    [Unoptimized] output time series data to HDF from the pandas result.

    :param output_directory: directory that the query results are written to
    :param output_name: name to give output file. if None, output name will be timestamp (%Y%m%dT%H%M%S)
    :param query_info: The query information
    :param metadata_store: dictionary of all metadata for query
    :param first_timestamp: datetime for the first observation of all data
    :param last_timestamp: datetime for the last observation of all data
    :param temporal_resolution: timescale (e.g. DAY)
    :return: The synthesized time series data
    """

    pandas_result: PandasTimeseriesData = _output_df(output_directory, output_name, query_info, metadata_store,
                                                     first_timestamp, last_timestamp,
                                                     temporal_resolution)

    # Write the data frames to the specified hdf5 file
    filename = os.path.join(output_directory, f"{output_name}.h5")

    if isinstance(pandas_result, PandasTimeseriesData):
        # Metadata with observations
        metadata_transpose = _transpose_metadata_df(pandas_result.metadata)
        metadata_transpose.to_hdf(filename, key='metadata', mode='w')

        # Metadata with no observations
        metadata_transpose = _transpose_metadata_df(pandas_result.metadata_no_observations)
        metadata_transpose.to_hdf(filename, key='metadata_no_observations', mode='a')

        pandas_result.data.to_hdf(filename, key='data', mode='a')

    # Now append the attributes about the query to the file
    with h5py.File(filename, 'a') as f:
        f['metadata'].attrs['column_names'] = list(pandas_result.metadata.columns)
        f['metadata_no_observations'].attrs['column_names'] = list(pandas_result.metadata_no_observations.columns)
        query_dict = json.loads(pandas_result.query.parameters.json())
        for field in query_dict.keys():
            if query_dict[field]:
                f.attrs[field] = query_dict[field]
        f.attrs['query_start_time'] = pandas_result.query.start_time.isoformat()
        f.attrs[
            'query_end_time'] = pandas_result.query.end_time and pandas_result.query.end_time.isoformat()
        f.attrs['variables_data'] = pandas_result.variables
        f.attrs['variables_nodata'] = pandas_result.variables_no_observations
    f.close()

    return HDFTimeseriesData(hdf=h5py.File(filename, 'r'),
                             output_path=output_directory,
                             query=query_info)


def _transpose_metadata_df(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transpose the metedata dataframe for HDF file with the specified metadata

    :param metadata_df: the metadata to transpose
    :return: The transposed data frame

    """
    # Transpose the metadata returned.  This is because HDF5 does not like columns
    #  that are of mixed type. So columns are the metadata (e.g data_start, basin_3d_variable)
    #  and row indices are the site/variables (e.g. USGS-02041000__RDC)
    metadata_transpose = metadata_df.T
    # Now convert columns with object type to string.  H5 does not like object
    #  type columns. Due to the fact that string data types have variable length, it is by
    #  default stored as object dtype which will need to be converted in the data frame
    columns = list(metadata_transpose.select_dtypes(include=['object']).columns)
    for c in columns:
        # which will by default set the length to the max len it encounters
        metadata_transpose[c] = metadata_transpose[c].astype(h5py.string_dtype(encoding='utf-8'))
    return metadata_transpose


def _fill_metadata_df(metadata_store, synthesized_variables) -> pd.DataFrame:
    """
    Fill the metadata dataframe for the given synthesized variables. Returns an empty dataframe, if
    the list is empty

    :param metadata_store: dictionary of all metadata for query
    :param synthesized_variables: list of synthesized variable names
    :return: The filled data frame
    """
    if not synthesized_variables:
        # Return an empty dataframe
        return pd.DataFrame({'TIMESTAMP': []})

    metadata_fields_list = list(metadata_store[synthesized_variables[-1]].keys())  # don't use first TIMESTAMP column
    empty_list = [None] * len(metadata_fields_list)
    metadata_fields = pd.Series(empty_list, index=metadata_fields_list)
    metadata_df = pd.DataFrame({'TIMESTAMP': metadata_fields})

    for synthesized_var in synthesized_variables:
        if synthesized_var == 'TIMESTAMP':
            continue
        metadata_df = metadata_df.join(pd.Series(empty_list, name=synthesized_var))
        synthesized_var_metadata = metadata_store[synthesized_var]
        synthesized_var_metadata_list = [synthesized_var_metadata[key] for key in metadata_fields_list]
        metadata_df[synthesized_var] = pd.array(synthesized_var_metadata_list)
    return metadata_df


def _result_generator(output_directory: str, metadata_store: dict) -> Generator[tuple, None, None]:
    """
    Generator for the result data. This yields a tuple with the synthesized variable name and
    the results in JSON.

    :param output_directory: directory that the query results are written to
    :param metadata_store: dictionary of all metadata for query
    """
    for synthesized_variable_name in metadata_store.keys():
        num_records = metadata_store[synthesized_variable_name]['records']
        if num_records == 0:
            continue
        file_path = os.path.join(output_directory, f'{synthesized_variable_name}.json')
        with open(file_path, mode='r') as f:
            yield synthesized_variable_name, json.load(f)
