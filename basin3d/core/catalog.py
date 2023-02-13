"""

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
from basin3d.core import monitor
from importlib import resources
from inspect import getmodule
import re
from string import whitespace
from typing import Dict, Iterator, List, Optional, Union

from basin3d.core.schema.enum import MAPPING_DELIMITER, NO_MAPPING_TEXT, MappedAttributeEnum, set_mapped_attribute_enum_type
from basin3d.core.models import DataSource, AttributeMapping, ObservedProperty

logger = monitor.get_logger(__name__)


class CatalogException(Exception):
    pass


class CatalogBase:

    def __init__(self, variable_filename: str = 'basin3d_observed_property_vocabulary.csv'):
        self.variable_dir = 'basin3d.data'
        self.variable_filename = variable_filename

    def initialize(self, plugin_list: list):
        """

        :param plugin_list: list of plugins
        :return: tinyDB object
        """
        if not self.is_initialized():
            logger.debug(f"Initializing {self.__class__.__name__} metadata catalog ")

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
                    logger.info(f"Loading metadata catalog for Plugin {plugin_id}")
                except Exception:
                    logger.error(
                        'Could not retrieve plugin_id. Check that plugin is configured / registered properly.')
                    raise CatalogException

                mapping_filename = f'{plugin_id.lower()}_mapping.csv'
                plugin_module = getmodule(plugin)
                if not plugin_module:
                    logger.error(f'Plugin {plugin_id} cannot be found')
                    raise CatalogException(f'Plugin {plugin_id} cannot be found')

                plugin_package = ".".join(plugin_module.__name__.split(".")[0:-1])
                # checking for the mapping file might occur elsewhere or look different depending on where plugins live
                has_mapping_file = resources.is_resource(plugin_package, mapping_filename)

                if not has_mapping_file:
                    logger.error(f'Plugin {plugin_id} does not have variable mapping file {mapping_filename}')
                    raise CatalogException(f'Plugin {plugin_id} does not have variable mapping file {mapping_filename}')

                datasource = plugin.get_datasource()
                self._process_plugin_attr_mapping(plugin, mapping_filename, datasource)
                self._insert(datasource)

            logger.info(f"Initialized {self.__class__.__name__} metadata catalog ")

    # ---------------------------------
    # Methods to be over-witten by children classes

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

    def _get_datasource(self, datasource_id) -> Optional[DataSource]:
        """
        Access a single datasource

        :param datasource_id: the datasource_identifier
        :return:
        """
        raise NotImplementedError

    def _get_observed_property(self, basin3d_vocab) -> Optional[ObservedProperty]:
        """
        Access a single observed property variable

        :param basin3d_vocab: the observed property variable identifier
        :return:
        """
        raise NotImplementedError

    def _get_attribute_mapping(self, datasource_id, attr_type, basin3d_vocab, datasource_vocab, **kwargs) -> Optional[AttributeMapping]:
        """
        Access a single attribute mapping

        :param datasource_id:
        :param attr_type:
        :param basin3d_vocab:
        :param datasource_vocab:
        :return:
        """
        raise NotImplementedError

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        raise NotImplementedError

    def find_observed_property(self, basin3d_vocab) -> Optional[ObservedProperty]:
        """
        Return the :class:`basin3d.models.ObservedProperty` object for the BASIN-3D vocabulary specified.

        :param basin3d_vocab: BASIN-3D vocabulary
        :return: a :class:`basin3d.models.ObservedProperty` object
        """
        raise NotImplementedError

    def find_observed_properties(self, basin3d_vocab=None) -> Iterator[Optional[ObservedProperty]]:
        """
        Report the observed_properties available based on the BASIN-3D vocabularies specified. If no BASIN-3D vocabularies are specified, then return all observed properties available.

        :param basin3d_vocab: list of the BASIN-3D observed properties
        :return: generator that yields :class:`basin3d.models.ObservedProperty` objects
        """
        raise NotImplementedError

    def find_datasource_attribute_mapping(self, datasource_id, attr_type, attr_vocab) -> Optional[AttributeMapping]:
        """
        Find the datasource attribute vocabulary to BASIN-3D mapping given a specific datasource_id, attr_type, and datasource attr_vocab.

        :param: datasource_id: the datasource identifier
        :param: attr_type: attribute type
        :param: datasource_vocab: the datasource attribute vocabulary
        :return: a :class:`basin3d.models.AttributeMapping` object
        """
        raise NotImplementedError

    def find_attribute_mappings(self, datasource, attr_type, attr_vocab, from_basin3d) -> Iterator[AttributeMapping]:
        """
        Find the list of attribute mappings given the specified fields.
        Exact matches are returned (see attr_vocab formats below for BASIN-3D vocab nuances).
        If no fields are specified, all registered attribute mappings will be returned.

        :param datasource_id: the datasource identifier
        :param attr_type: the attribute type
        :param attr_vocab: the attribute vocabulary, the formats are one of the following:
                           1) datasource vocab that is a complete vocab
                           2) BASIN-3D vocab that is a complete vocab for a given attr_type regardless if it is compound or not.
                           3) BASIN-3D vocab that is compound and fully specified for each attr_type either with the complete vocab or with wildcards.
        :param from_basin3d: boolean that says whether the attr_vocab is a BASIN-3D vocabulary. If not, then this a datasource vocabulary.
        :return: generator that yields :class:`basin3d.models.AttributeMapping` objects
        """
        # class extensions should use _validate_fine_attribute_mappings_arguments
        raise NotImplementedError

    # --------------------------------------

    def _gen_variable_store(self):
        """
        Generate a variable store. Loads this from the provided vocabulary CSV
        :return:
        """
        fields = ['basin3d_vocab', 'description', 'categories', 'units']
        # location of mapping file might change.
        with resources.open_text(self.variable_dir, self.variable_filename) as variable_file:

            reader = csv.DictReader(variable_file)

            # For now, force a specific file format; could change later to just require specific field names
            if reader.fieldnames != fields:
                logger.critical(f'{self.variable_filename} is not in correct format. Cannot create catalog.')
                raise CatalogException

            for row in reader:
                basin3d_vocab = row[fields[0]]

                if self._get_observed_property(basin3d_vocab) is not None:
                    logger.warning(f'Duplicate BASIN-3D variable {basin3d_vocab} found. Skipping duplicate.')
                    continue

                categories_list = []
                if row[fields[2]]:
                    categories_str = row[fields[2]]
                    categories_list = categories_str.split(',')
                    categories_list = [category.strip(whitespace) for category in categories_list]

                observed_property_variable = ObservedProperty(
                    basin3d_vocab=basin3d_vocab,
                    full_name=row[fields[1]],
                    categories=categories_list,
                    units=row[fields[3]])

                self._insert(observed_property_variable)

    def _get_attribute_enum(self, str_value, enum_type):
        """
        Get the enum for a given string value
        :param str_value: string value to convert to enum, e.g., Max
        :param enum_type: the enum type, e.g., StatisticEnum
        :return:
        """
        enum_value = None
        try:
            enum_value = getattr(enum_type, str_value)
        except AttributeError:
            pass
        return enum_value

    def _process_plugin_attr_mapping(self, plugin, filename: str, datasource: DataSource):
        """

        :param plugin:
        :param filename:
        :param datasource:
        :return:
        """
        fields = ['attr_type', 'basin3d_vocab', 'datasource_vocab', 'datasource_desc']

        plugin_id = plugin.get_meta().id
        plugin_module = getmodule(plugin)
        if not plugin_module:
            logger.error(f'Plugin {plugin_id} cannot be found')
            raise CatalogException(f'Plugin {plugin_id} cannot be found')

        plugin_package = ".".join(plugin_module.__name__.split(".")[0:-1])

        with resources.open_text(plugin_package, filename) as attr_file:
            logger.debug(f"Mapping file {filename} for plugin package {plugin_package}")
            reader = csv.DictReader(attr_file)

            # For now, force a specific file format; could change later to just require specific field names
            if reader.fieldnames != fields:
                logger.critical(
                    f'Plugin {plugin_id}: {filename} is not in correct format. Cannot create catalog.')
                raise CatalogException(
                    f'Plugin {plugin_id}: {filename} is not in correct format. Cannot create catalog.')

            # loop thru the file
            for row in reader:
                attr_type = row[fields[0]]
                basin3d_vocab = row[fields[1]]
                datasource_vocab = row[fields[2]]
                datasource_desc = row[fields[3]]

                # ToDo: add some validation if Cat does not have it in her reader
                if self._get_attribute_mapping(datasource.id, attr_type, basin3d_vocab, datasource_vocab) is not None:
                    logger.warning(
                        f'Plugin {plugin_id}: Duplicate BASIN-3D attr detected. Cannot handle duplicate mappings yet. '
                        f'Skipping datasource attribute {datasource_vocab}.')

                # build the basin3d_description from objects (e.g., for OBSERVED_PROPERTY) or enums
                basin3d_desc = []
                for a_type, b3d_vocab in zip(attr_type.split(MAPPING_DELIMITER), basin3d_vocab.split(MAPPING_DELIMITER)):
                    valid_type_vocab = True

                    b3d_attr_type = None
                    try:
                        b3d_attr_type = getattr(MappedAttributeEnum, a_type)
                        if b3d_attr_type == MappedAttributeEnum.OBSERVED_PROPERTY:
                            b3d_desc = self._get_observed_property(b3d_vocab)
                        else:
                            b3d_enum_type = set_mapped_attribute_enum_type(b3d_attr_type)
                            b3d_desc = self._get_attribute_enum(b3d_vocab, b3d_enum_type)
                    except AttributeError:
                        logger.warning(f'Datasource {datasource.id}: Attribute type {a_type} is not supported. Skipping mapping.')
                        valid_type_vocab = False
                        break

                    if not b3d_desc:
                        logger.warning(f'Datasource {datasource.id}: basin3d_vocab {b3d_vocab} for attr_type {a_type} is not a valid BASIN-3D vocabulary. Skipping attribute mapping.')
                        valid_type_vocab = False
                        break

                    basin3d_desc.append(b3d_desc)

                if not valid_type_vocab:
                    continue

                attr_mapping = AttributeMapping(
                    attr_type=attr_type,
                    basin3d_vocab=basin3d_vocab,
                    basin3d_desc=basin3d_desc,
                    datasource_vocab=datasource_vocab,
                    datasource_desc=datasource_desc,
                    datasource=datasource)

                self._insert(attr_mapping)
                logger.debug(f"{datasource.id}: Mapped {attr_type} {datasource_vocab} to {basin3d_vocab}")

    def _validate_find_attribute_mappings_arguments(self, datasource_id: str = None, attr_type: str = None):
        """
        Validate arguments for method find_attribute_mappings

        :param datasource_id: datasource ID
        :param attr_type: Attribute Type
        """
        if datasource_id and self._get_datasource(datasource_id) is None:
            msg = f'Specified datasource_id: "{datasource_id}" has not been registered.'
            logger.critical(msg)
            raise CatalogException(msg)

        if attr_type and attr_type not in MappedAttributeEnum.values():
            msg = f'"{attr_type}" is not an attribute type supported by BASIN-3D.'
            logger.critical(msg)
            raise CatalogException(msg)


class CatalogTinyDb(CatalogBase):

    def __init__(self, variable_filename: str = 'basin3d_observed_property_vocabulary.csv'):
        super().__init__(variable_filename)

        self.in_memory_db = None
        self.in_memory_db_attr = None
        self._observed_properties: Dict[str, ObservedProperty] = {}
        self._attribute_mappings: Dict[str, AttributeMapping] = {}
        self._datasources: Dict[str, DataSource] = {}

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        return self.in_memory_db is not None

    def _get_datasource(self, datasource_id) -> Optional[DataSource]:
        """
        Access a datasource via the datasource_id
        :param datasource_id: str, the datasource id
        :return: a :class:`basin3d.models.DataSource` object
        """
        return self._datasources.get(datasource_id, None)

    def _get_observed_property(self, basin3d_vocab) -> Optional[ObservedProperty]:
        """
        Access a single observed property

        :param basin3d_vocab: str, the observed property identifier
        :return: an :class:`basin3d.models.ObservedProperty` object
        """
        return self._observed_properties.get(basin3d_vocab, None)

    def _get_attribute_mapping(self, datasource_id, attr_type, basin3d_vocab, datasource_vocab, **kwargs) -> Optional[AttributeMapping]:
        """
        Access a single attribute mapping

        :param datasource_id: str, datasource identifier
        :param attr_type: str, attribute type
        :param basin3d_vocab: str, BASIN-3D vocabulary
        :param datasource_vocab: str, datasource vocabulary
        :return: a :class:`basin3d.models.AttributeMapping` object
        """
        return self._attribute_mappings.get(f'{datasource_id}-{attr_type}-{basin3d_vocab}-{datasource_vocab}', None)

    def find_observed_property(self, basin3d_vocab: str) -> Optional[ObservedProperty]:
        """
        Return the :class:`basin3d.models.ObservedProperty` object for the BASIN-3D vocabulary specified.

        :param basin3d_vocab: BASIN-3D vocabulary
        :return: a :class:`basin3d.models.ObservedProperty` object
        """

        if not self._observed_properties:
            msg = "Variable Store has not been initialized."
            logger.critical(msg)
            raise CatalogException(msg)

        return self._get_observed_property(basin3d_vocab)

    def find_observed_properties(self, basin3d_vocab: Optional[List[str]] = None) -> Iterator[Optional[ObservedProperty]]:
        """
        Report the observed_properties available based on the BASIN-3D vocabularies specified. If no BASIN-3D vocabularies are specified, then return all observed properties available.

        :param basin3d_vocab: list of the BASIN-3D observed properties
        :return: generator that yields :class:`basin3d.models.ObservedProperty` objects
        """

        if not self._observed_properties:
            msg = "Variable Store has not been initialized."
            logger.critical(msg)
            raise CatalogException(msg)

        opv: Optional[ObservedProperty]
        if basin3d_vocab is None:
            for opv in self._observed_properties.values():
                yield opv
        else:
            for b3d_vocab in basin3d_vocab:
                opv = self._get_observed_property(b3d_vocab)
                if opv is not None:
                    yield opv
                else:
                    logger.warning(f'BASIN-3D does not support variable {b3d_vocab}')

    def find_datasource_attribute_mapping(self, datasource_id: str, attr_type: str, datasource_vocab: str) -> Optional[AttributeMapping]:
        """
        Find the datasource attribute vocabulary to BASIN-3D mapping given a specific datasource_id, attr_type, and datasource attr_vocab.

        :param: datasource_id: the datasource identifier
        :param: attr_type: attribute type
        :param: datasource_vocab: the datasource attribute vocabulary
        :return: a :class:`basin3d.models.AttributeMapping` object
        """
        if self.in_memory_db_attr is None:
            msg = 'Attribute Store has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        # attr_type is search to accommodate compound mappings
        results = self.in_memory_db_attr.search(
            (query.datasource_vocab == datasource_vocab) & (query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))

        # Only one result should be returned. If so return it and all is good.
        if len(results) == 1:
            return self._get_attribute_mapping(**results[0])
        # If not, deal with results:
        # Case 1: more than one mapping for the datasource vocabulary. This should not happen. Raise an exception.
        elif len(results) > 1:
            error_msg = (f'More than one attribute mapping found for datasource vocab: "{datasource_vocab}" '
                         f'in datasource: "{datasource_id}". This should never happen.')
            logger.critical(error_msg)
            raise CatalogException(error_msg)

        # Case 2: no mapping was found. Return an empty AttributeMapping object
        msg = f'No mapping was found for datasource vocab: "{datasource_vocab}" in datasource: "{datasource_id}".'
        datasource = self._get_datasource(datasource_id)

        if datasource is None:
            datasource = DataSource()
            msg = f'No datasource was found for id "{datasource_id}".'
            logger.warning(msg)

        return AttributeMapping(attr_type=attr_type, basin3d_vocab=NO_MAPPING_TEXT, basin3d_desc=[],
                                datasource_vocab=datasource_vocab, datasource_desc=msg, datasource=datasource)

    def find_attribute_mappings(self, datasource_id: str = None, attr_type: str = None, attr_vocab: Union[str, List] = None,
                                from_basin3d: bool = False) -> Iterator[AttributeMapping]:
        """
        Find the list of attribute mappings given the specified fields.
        Exact matches are returned (see attr_vocab formats below for BASIN-3D vocab nuances).
        If no fields are specified, all registered attribute mappings will be returned.

        :param datasource_id: the datasource identifier
        :param attr_type: the attribute type
        :param attr_vocab: the attribute vocabulary, the formats are one of the following:
                           1) datasource vocab that is a complete vocab
                           2) BASIN-3D vocab that is a complete vocab for a given attr_type regardless if it is compound or not.
                           3) BASIN-3D vocab that is compound and fully specified for each attr_type either with the complete vocab or with wildcards.
        :param from_basin3d: boolean that says whether the attr_vocab is a BASIN-3D vocabulary. If not, then this a datasource vocabulary.
        :return: generator that yields :class:`basin3d.models.AttributeMapping` objects
        """
        catalog_messages: List[str] = []

        self._validate_find_attribute_mappings_arguments(datasource_id, attr_type)

        if self.in_memory_db_attr is None:
            msg = 'Attribute Store has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        # Set the attribute vocab list to track mappings not found
        specified_attr_vocab_not_found = []
        if attr_vocab:
            if isinstance(attr_vocab, str):
                attr_vocab = [attr_vocab]
            elif not isinstance(attr_vocab, List):
                raise CatalogException("attr_vocab must be a str or list")
            specified_attr_vocab_not_found = attr_vocab.copy()

        # Function for TinyDB search
        def is_in(x, attr_vocabs=attr_vocab, is_from_basin3d=from_basin3d):
            for a_vocab in attr_vocabs:
                # if attr_vocabs is datasource vocab(s)
                # OR attr_vocabs is BASIN-3D vocab(s) AND they have the compound mapping delimiter (i.e., it has : and possibly wildcards)
                # AND it is a fullmatch
                if (not is_from_basin3d or (is_from_basin3d and MAPPING_DELIMITER in a_vocab)) and re.fullmatch(a_vocab, x):
                    return True
                # if attr_vocabs is BASIN-3D vocab(s) and they do not have the compound mapping delimiter,
                # then match exactly for the individual attr_type components.
                # e.g. attr_vocabs = ['MAX', 'MIN'] and the mapping is compound e.g. x = 'Al:WATER:MAX'
                # re.search does not work here b/c of a case like OBSERVED_PROPERPTY variables TEMP:AIR and SONIC_TEMP:AIR,
                # in which a attr_vocab of ['TEMP'] would return both instead on just TEMP:AIR.
                # A general search would need to be handled differently.
                elif is_from_basin3d and MAPPING_DELIMITER not in a_vocab:
                    for x_vocab in x.split(MAPPING_DELIMITER):
                        if x_vocab == a_vocab:
                            return True
            return False

        # If no query parameters --> get all mapped attributes back for all registered plugins
        if not datasource_id and not attr_vocab and not attr_type:
            # return all mapped attributes possible possible
            for attr_mapping in self._attribute_mappings.values():
                yield attr_mapping
        # Otherwise search depends on the set of parameters provided
        else:
            if not datasource_id:
                if attr_type and attr_vocab and from_basin3d:
                    results = self.in_memory_db_attr.search((query.basin3d_vocab.test(is_in)) & (query.attr_type.search(attr_type)))
                elif attr_type and attr_vocab:
                    results = self.in_memory_db_attr.search((query.datasource_vocab.test(is_in)) & (query.attr_type.search(attr_type)))
                elif not attr_type and from_basin3d:
                    results = self.in_memory_db_attr.search(query.basin3d_vocab.test(is_in))
                elif not attr_type:
                    results = self.in_memory_db_attr.search(query.datasource_vocab.test(is_in))
                else:
                    results = self.in_memory_db_attr.search(query.attr_type.search(attr_type))

            # Returns all possible attributes for a data source
            elif not attr_type and not attr_vocab:
                results = self.in_memory_db_attr.search(query.datasource_id == datasource_id)

            # Returns all attributes for a data source and attribute type
            elif not attr_vocab:
                results = self.in_memory_db_attr.search((query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))

            # Return mappings if only the datasource and attr_vocab are specified.
            elif not attr_type:
                if from_basin3d:
                    results = self.in_memory_db_attr.search((query.basin3d_vocab.test(is_in)) & (query.datasource_id == datasource_id))
                else:
                    results = self.in_memory_db_attr.search((query.datasource_vocab.test(is_in)) & (query.datasource_id == datasource_id))

            # Finally, all parameters specified:
            # Convert from BASIN-3D to DataSource variable name
            elif from_basin3d:
                results = self.in_memory_db_attr.search(
                    (query.basin3d_vocab.test(is_in)) & (query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))

            # Convert from DataSource variable name to BASIN-3D
            else:
                results = self.in_memory_db_attr.search(
                    (query.datasource_vocab.test(is_in)) & (query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))

            # Yield the results
            attr_map: Optional[AttributeMapping]
            for r in results:
                attr_map = self._get_attribute_mapping(**r)

                # if AttributeMapping is not found: THIS SHOULD NEVER HAPPEN.
                if attr_map is None:
                    logger.error('AttributeMapping not found given database search results. THIS SHOULD NEVER HAPPPEN.')
                    continue

                # If attr_vocab was specified, track what was found for the datasource
                if attr_vocab:
                    vocabs = [attr_map.datasource_vocab]
                    if from_basin3d:
                        vocabs = attr_map.basin3d_vocab.split(MAPPING_DELIMITER)

                    # look for it in the specified vocabs, remove it from the not found list
                    for vocab in vocabs:
                        try:
                            idx = specified_attr_vocab_not_found.index(vocab)
                            specified_attr_vocab_not_found.pop(idx)
                        except ValueError:
                            pass

                # yield the AttributeMapping
                yield attr_map

            # If any specified attr_vocabs were not found, add info messages
            if specified_attr_vocab_not_found:
                attr_vocab_text = ', '.join(specified_attr_vocab_not_found)
                attr_vocab_source = 'datasource'
                if from_basin3d:
                    attr_vocab_source = 'BASIN-3D'
                if not results:
                    # if no results,
                    msg = (f'No attribute mappings found for specified parameters: datasource id = "{datasource_id}", '
                           f'attribute type = "{attr_type}", {attr_vocab_source} vocabularies: {attr_vocab_text}.')
                else:
                    msg = (f'No attribute mappings found for the following {attr_vocab_source} vocabularies: {attr_vocab_text}. '
                           f'Note: specified datasource id = {datasource_id} and attribute type = {attr_type}')
                logger.info(msg)
                catalog_messages.append(msg)

            return StopIteration(catalog_messages)

    def _init_catalog(self):
        """
        Initialize the catalog database

        :return:
        """
        # Note: op are held in dictionary in this TinyDB
        from tinydb import TinyDB
        from tinydb.storages import MemoryStorage
        self.in_memory_db = TinyDB(storage=MemoryStorage)
        self.in_memory_db_attr = self.in_memory_db.table('attr')
        self.in_memory_db_attr.truncate()

    def _insert(self, record):
        """
        :param record:
        """
        if self.in_memory_db is not None:
            if isinstance(record, ObservedProperty):
                self._observed_properties[record.basin3d_vocab] = record
            elif isinstance(record, AttributeMapping):
                key = f'{record.datasource.id}-{record.attr_type}-{record.basin3d_vocab}-{record.datasource_vocab}'
                logger.debug(key)
                self._attribute_mappings[key] = record
                self.in_memory_db_attr.insert({'datasource_id': record.datasource.id,
                                               'attr_type': record.attr_type,
                                               'basin3d_vocab': record.basin3d_vocab,
                                               'basin3d_desc': record.basin3d_desc,
                                               'datasource_vocab': record.datasource_vocab,
                                               'datasource_desc': record.datasource_desc, })
            elif isinstance(record, DataSource):
                self._datasources[record.id] = record
        else:
            msg = 'Could not insert record. Catalog not initialized.'
            logger.critical(msg)
            raise CatalogException(msg)
