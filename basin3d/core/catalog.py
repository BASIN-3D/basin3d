"""
`basin3d.core.catalog`
************************

.. currentmodule:: basin3d.core.synthesis

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` catalog classes
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""
import csv
import logging
from importlib import resources
from inspect import getmodule
from string import whitespace
from typing import Dict, Iterator, List, Optional

from basin3d.core.models import DataSource, ObservedProperty, ObservedPropertyVariable
from basin3d.core.types import SamplingMedium

logger = logging.getLogger(__name__)


class CatalogException(Exception):
    pass


class CatalogBase:

    def __init__(self, variable_filename: str = 'basin3d_variables_hydrology.csv'):
        self.variable_dir = 'basin3d.data'
        self.variable_filename = variable_filename

    def initialize(self, plugin_list: list):
        """

        :param plugin_list: list of plugins
        :return: tinyDB object
        """
        if not self.is_initialized():

            # initiate db
            self._init_catalog()

            if not plugin_list:
                plugin_list = []

            # generate variable store
            self._gen_variable_store()

            # Load plugins
            for plugin in plugin_list:
                try:
                    # if the plugin doesn't exist then getting the id should fail
                    plugin_id = plugin.get_meta().id
                except Exception:
                    logger.error(
                        f'Could not retrieve plugin_id. Check that plugin is configured / registered properly.')
                    raise CatalogException

                mapping_filename = f'mapping_{plugin_id.lower()}.csv'
                plugin_module = getmodule(plugin)
                if not plugin_module:
                    logger.error(f'Plugin {plugin_id} cannot be found')
                    raise CatalogException(f'Plugin {plugin_id} cannot be found')

                plugin_package = ".".join(plugin_module.__name__.split(".")[0:-1])
                # checking for the mapping file might occur elsewhere or look different depending on where plugins live
                has_mapping_file = resources.is_resource(plugin_package, mapping_filename)

                if not has_mapping_file:
                    logger.error(f'Plugin {plugin_id} does not have mapping file {mapping_filename}')
                    raise CatalogException(f'Plugin {plugin_id} does not have mapping file {mapping_filename}')

                datasource = plugin.get_datasource()
                self._process_plugin_variable_mapping(plugin, mapping_filename, datasource)

    def _init_catalog(self):
        """

        :return:
        """
        raise NotImplementedError

    def _insert(self, record):
        """
        Insert the record
        :param record:
        """
        raise NotImplementedError

    def _get_observed_property_variable(self, basin3d_id) -> Optional[ObservedPropertyVariable]:
        """
        Access a single observed property variable

        :param basin3d_id: the observed property variable identifier
        :return:
        """
        raise NotImplementedError

    def _get_observed_property(self, datasource_id, basin3d_id, datasource_variable_id) -> Optional[ObservedProperty]:
        """
        Access a single observed property

        :param datasource_id:  datasource identifier
        :param basin3d_id:  BASIN-3D variable identifier
        :param datasource_variable_id: datasource variable identifier
        :return:
        """
        raise NotImplementedError

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        raise NotImplementedError

    def find_observed_property(self, datasource_id, variable_name) -> Optional[ObservedProperty]:
        """
        Get the measurement to the specified variable_name

        :param datasource_id: the datasource identifier
        :param variable_name: the variable name to get the :class:`~basin3d.models.ObservedProperty` for
        :return: :class:`~basin3d.models.ObservedProperty`
        """
        raise NotImplementedError

    def find_observed_properties(self, datasource_id=None, variable_names=None) -> Iterator[ObservedProperty]:
        """
        Get the measurement to the specified variable_name

        :param datasource_id:  the datasource identifier
        :param variable_names: the variable names to get the :class:`~basin3d.models.ObservedProperty` for
        :type variable_names: list
        :return: :class:`~basin3d.models.ObservedProperty`
        """
        raise NotImplementedError

    def find_observed_property_variable(self, datasource_id, variable_name, from_basin3d=False) -> Optional[
            ObservedPropertyVariable]:
        """
        Convert the given name to either BASIN-3D from :class:`~basin3d.models.DataSource`
        variable name or the other way around.

        :param datasource_id: the datasource identifier
        :param variable_name:  The :class:`~basin3d.models.ObservedPropertyVariable`
             name to convert
        :param: from_basin3d: boolean that says whether the variable name is a
            BASIN-3D variable. If not, then this a datasource variable name.
        :type from_basin3d: boolean
        :return: A variable name
        :rtype: str
        """
        raise NotImplementedError

    def find_observed_property_variables(self, datasource_id=None, variable_names=None, from_basin3d=False) -> Iterator[
            ObservedPropertyVariable]:
        """
        Convert the given list of names to either BASIN-3D from :class:`~basin3d.models.DataSource`
        variable name or the other way around.

        :param datasource_id: the datasource identifier
        :param variable_names:  The :class:`~basin3d.models.ObservedPropertyVariable`
             names to convert
        :type variable_names: iterable
        :param: from_basin3d: boolean that says whether the variable name is a
            BASIN-3D variable. If not, then this a datasource variable names.
        :type from_basin3d: boolean
        :return: list of variable names
        :rtype: iterable
        """
        raise NotImplementedError

    def _gen_variable_store(self):
        """
        Generate a variable store. Loads this from the provided vocabulary CSV
        :return:
        """
        fields = ['basin3d_id', 'description', 'categories', 'units']
        # location of mapping file might change.
        with resources.open_text(self.variable_dir, self.variable_filename) as variable_file:

            reader = csv.DictReader(variable_file)

            # For now, force a specific file format; could change later to just require specific field names
            if reader.fieldnames != fields:
                logger.critical(f'{self.variable_filename} is not in correct format. Cannot create catalog.')
                raise CatalogException

            for row in reader:
                basin3d_id = row[fields[0]]

                if self._get_observed_property_variable(basin3d_id) is not None:
                    logger.warning(f'Duplicate BASIN-3D variable {basin3d_id} found. Skipping duplicate.')
                    continue

                categories_list = []
                if row[fields[2]]:
                    categories_str = row[fields[2]]
                    categories_list = categories_str.split(',')
                    categories_list = [category.strip(whitespace) for category in categories_list]

                observed_property_variable = ObservedPropertyVariable(
                    basin3d_id=basin3d_id,
                    full_name=row[fields[1]],
                    categories=categories_list,
                    units=row[fields[3]])

                self._insert(observed_property_variable)

    def _process_plugin_variable_mapping(self, plugin, map_filename: str, datasource: DataSource):
        """

        :param plugin:
        :param map_filename:
        :param datasource:
        :param variable_store:
        :return:
        """
        fields = ['basin3d_id', 'datasource_variable', 'sampling_medium', 'description']

        plugin_id = plugin.get_meta().id
        plugin_module = getmodule(plugin)
        if not plugin_module:
            logger.error(f'Plugin {plugin_id} cannot be found')
            raise CatalogException(f'Plugin {plugin_id} cannot be found')

        plugin_package = ".".join(plugin_module.__name__.split(".")[0:-1])

        with resources.open_text(plugin_package, map_filename) as map_file:
            reader = csv.DictReader(map_file)

            # For now, force a specific file format; could change later to just require specific field names
            if reader.fieldnames != fields:
                logger.critical(
                    f'Plugin {plugin_id}: {map_filename} is not in correct format. Cannot create catalog.')
                raise CatalogException(
                    f'Plugin {plugin_id}: {map_filename} is not in correct format. Cannot create catalog.')

            for row in reader:
                basin3d_id = row[fields[0]]
                datasource_var = row[fields[1]]
                sampling_medium = row[fields[2]]
                description = row[fields[3]]
                observed_property_variable = self._get_observed_property_variable(basin3d_id)
                if observed_property_variable is None:
                    logger.warning(
                        f'Plugin {plugin_id}: Datasource variable {datasource_var} not in BASIN-3D variables.')
                    continue
                if self._get_observed_property(datasource.id, basin3d_id, datasource_var) is not None:
                    logger.warning(
                        f'Plugin {plugin_id}: Duplicate BASIN-3D variable detected. Cannot handle duplicate mappings yet. '
                        f'Skipping datasource_variable {datasource_var}.')
                    continue
                if not sampling_medium in SamplingMedium.SAMPLING_MEDIUMS:
                    logger.warning(
                        f'Plugin {plugin_id}: Datasource sampling medium {sampling_medium} is not BASIN-3D sampling medium type.')
                    continue

                observed_property = ObservedProperty(
                    datasource_variable=datasource_var,
                    observed_property_variable=observed_property_variable,
                    sampling_medium=sampling_medium, datasource=datasource,
                    datasource_description=description)

                self._insert(observed_property)


class CatalogTinyDb(CatalogBase):

    def __init__(self, variable_filename: str = 'basin3d_variables_hydrology.csv'):
        super().__init__(variable_filename)

        self.in_memory_db = None
        self._observed_properties: Dict[str, ObservedProperty] = {}
        self._observed_property_variables: Dict[str, ObservedPropertyVariable] = {}

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        return self.in_memory_db is not None

    def _get_observed_property_variable(self, basin3d_id) -> Optional[ObservedPropertyVariable]:
        """
        Access a single observed property variable

        :param basin3d_id: the observed property variable identifier
        :return:
        """
        return self._observed_property_variables.get(basin3d_id, None)

    def _get_observed_property(self, datasource_id, basin3d_id, datasource_variable_id) -> Optional[ObservedProperty]:
        """
        Access a single observed property

        :param datasource_id:  datasource identifier
        :param basin3d_id:  BASIN-3D variable identifier
        :param datasource_variable_id: datasource variable identifier
        :return:
        """
        return self._observed_properties.get(f"{datasource_id}-{basin3d_id}-{datasource_variable_id}", None)

    def find_observed_property(self, datasource_id, variable_name) -> Optional[ObservedProperty]:
        """
        Get the measurement to the specified variable_name

        :param variable_name: the variable name to get the :class:`~basin3d.models.ObservedProperty` for
        :return: :class:`~basin3d.models.ObservedProperty`
        """
        if self.in_memory_db is None:
            raise CatalogException("Variable Store has not been initialized")

        from tinydb import Query
        query = Query()
        results = self.in_memory_db.search((query.basin3d_id == variable_name) & (query.datasource_id == datasource_id))

        if len(results) > 0:
            return self._get_observed_property(**results[0])
        return None

    def find_observed_properties(self, datasource_id=None, variable_names: List[str] = None) -> Iterator[
            ObservedProperty]:
        """
        Get the observed properties to the specified variable_names and datasource

        :param variable_names: the variable names to get the :class:`~basin3d.models.ObservedProperty` for
        :type variable_names: list
        :param datasource_id: The datasource to filter by

        :return: :class:`~basin3d.models.ObservedProperty`
        """
        if self.in_memory_db is None:
            raise CatalogException("Variable Store has not been initialized")

        from tinydb import Query
        query = Query()
        is_in = lambda x: x in variable_names
        if not datasource_id:
            if variable_names:
                results = self.in_memory_db.search(
                    (query.basin3d_id.any(variable_names)))
            else:
                results = self.in_memory_db.all()
        else:
            if variable_names:
                results = self.in_memory_db.search(
                    (query.basin3d_id.test(is_in)) & (query.datasource_id == datasource_id))
            else:
                results = self.in_memory_db.search(query.datasource_id == datasource_id)

        for r in results:
            yield self._get_observed_property(**r)

    def find_observed_property_variable(self, datasource_id, variable_name, from_basin3d=False) -> Optional[
            ObservedPropertyVariable]:
        """
        Convert the given name to either BASIN-3D from :class:`~basin3d.models.DataSource`
        variable name or the other way around.

        :param variable_name:  The :class:`~basin3d.models.ObservedPropertyVariable`
            name to convert
        :param: from_basin3d: boolean that says whether the variable name is a
           BASIN-3D variable. If not, then this a datasource variable name.
        :type from_basin3d: boolean
        :return: A variable name
        :rtype: str
        """
        if self.in_memory_db is None:
            raise CatalogException("Variable Store has not been initialized")

        from tinydb import Query
        query = Query()
        if from_basin3d:
            results = self.in_memory_db.search(
                (query.basin3d_id == variable_name) & (query.datasource_id == datasource_id))
        else:
            # Convert from DataSource variable name to BASIN-3D
            results = self.in_memory_db.search(
                (query.datasource_variable_id == variable_name) & (query.datasource_id == datasource_id))
        basin3d_variable_id = results[0]['basin3d_id']
        return self._get_observed_property_variable(basin3d_variable_id)

    def find_observed_property_variables(self, datasource_id=None, variable_names=None, from_basin3d=False) -> Iterator[
            ObservedPropertyVariable]:
        """
        Convert the given list of names to either BASIN-3D from :class:`~basin3d.models.DataSource`
        variable name or the other way around.

        :param variable_names:  The :class:`~basin3d.models.ObservedPropertyVariable`
             names to convert
        :type variable_names: iterable
        :param: from_basin3d: boolean that says whether the variable name is a
            BASIN-3D variable. If not, then this a datasource variable names.
        :type from_basin3d: boolean
        :return: list of variable names
        :rtype: iterable
        """

        if self.in_memory_db is None:
            raise CatalogException("Variable Store has not been initialized")

        from tinydb import Query
        query = Query()

        is_in = lambda x: x in variable_names
        if not datasource_id and not variable_names:
            # return all observe_property_variables possible
            for opv in self._observed_property_variables.values():
                yield opv
        else:
            if not datasource_id:
                # It wouldn't make sense to return observed property variables without a data source filter
                raise CatalogException(
                    "find_observed_property_variables: 'datasource' should be specified with 'variable_names'")

            if not variable_names:
                # This returns all possible observed property variables for a data source
                results = self.in_memory_db.search(query.datasource_id == datasource_id)

            elif from_basin3d:

                # Convert from BASIN-3D to DataSource variable name
                results = self.in_memory_db.search(
                    (query.basin3d_id.test(is_in)) & (query.datasource_id == datasource_id))

            else:
                # Convert from DataSource variable name to BASIN-3D
                results = self.in_memory_db.search(
                    (query.datasource_variable_id.test(is_in)) & (query.datasource_id == datasource_id))

            # Yield the results
            for r in results:
                yield self._get_observed_property_variable(r['basin3d_id'])

    def _init_catalog(self):
        """
        Initialize the catalog database

        :return:
        """
        from tinydb import TinyDB
        from tinydb.storages import MemoryStorage
        self.in_memory_db = TinyDB(storage=MemoryStorage)
        self.in_memory_db.truncate()

    def _insert(self, record):
        """
        :param record:
        """
        if self.in_memory_db is not None:
            if isinstance(record, ObservedPropertyVariable):
                self._observed_property_variables[record.basin3d_id] = record
            elif isinstance(record, ObservedProperty):
                self._observed_properties[
                    f"{record.datasource.id}-{record.observed_property_variable.basin3d_id}-{record.datasource_variable}"] = record
                self.in_memory_db.insert({'datasource_id': record.datasource.id,
                                          'datasource_variable_id': record.datasource_variable,
                                          'basin3d_id': record.observed_property_variable.basin3d_id, })
        else:
            raise CatalogException(f'Could not insert record.  Catalog not initialize')
