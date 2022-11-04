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
from basin3d.core import monitor
from dataclasses import dataclass
from itertools import product
from importlib import resources
from inspect import getmodule
from string import whitespace
from typing import Dict, Iterator, List, Optional, Union

from basin3d.core.schema.enum import MAPPING_DELIMITER, NO_MAPPING_TEXT, MappedAttributeEnum, set_mapped_attribute_enum_type
from basin3d.core.schema.query import QueryBase
from basin3d.core.models import DataSource, AttributeMapping, ObservedProperty

logger = monitor.get_logger(__name__)


class CatalogException(Exception):
    pass


def verify_attr_type(attr_type: str) -> str:
    """
    Helper method to translate query and model parameters (lower case and sometimes plural)
    to proper attribute types (upper case and singular) in the catalog

    :param attr_type: str, attribute type, single only
    :return: str, attribute type for give parameter with format confirmed and/or modified
    """

    attr_type = attr_type.upper()

    if attr_type == 'OBSERVED_PROPERTY_VARIABLE' or attr_type == 'OBSERVED_PROPERTY_VARIABLES':
        attr_type = 'OBSERVED_PROPERTY'

    return attr_type


def verify_query_param(attr_type: str, is_query=True) -> str:
    """
    Helper method to translate attribute types (UPPER CASE) in the catalog to query parameters (lower case and sometimes plural)

    :param attr_type: str, attribute type to translate
    :param is_query: boolean, True = is a subclass of QueryBase
    :return: str, parameter for given attribute with format confirmed and/or modified
    """
    if attr_type == 'OBSERVED_PROPERTY':
        attr_type = 'OBSERVED_PROPERTY_VARIABLE'

        if is_query:
            attr_type = f'{attr_type}S'

    return attr_type.lower()


class CatalogBase:

    def __init__(self, variable_filename: str = 'basin3d_observed_property_variables_vocabulary.csv'):
        self.variable_dir = 'basin3d.data'
        self.variable_filename = variable_filename

    @dataclass
    class CompoundMapping:
        """
        Helper model to handle compound attribute mapping

        Attributes:
            - *attr_type:* a single attribute type that is part of the associated compound mapping, e.g. STATISTIC, RESULT_QUALITY, OBSERVED_PROPERTY
            - *compound_mapping:* the compound mapping
            - *datasource:* the datasource containing the compound mapping
        """
        attr_type: str  # e.g. OBSERVED_PROPERTY
        compound_mapping: str  # e.g. OBSERVED_PROPERTY:SAMPLING_MEDIUM
        datasource: DataSource = DataSource()

        def __str__(self):
            return self.__unicode__()

        def __unicode__(self):
            return self.compound_mapping

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
                        f'Could not retrieve plugin_id. Check that plugin is configured / registered properly.')
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

    def _get_compound_mapping(self, datasource_id, attr_type, compound_mapping):
        """

        :param datasource_id:
        :param attr_type:
        :param compound_mapping:
        :return:
        """
        raise NotImplementedError

    def _find_compound_mapping(self, datasource_id, attr_type):
        """

        :param datasource_id:
        :param attr_type:
        :return:
        """
        raise NotImplementedError

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        raise NotImplementedError

    def find_observed_property(self, basin3d_vocab) -> Optional[ObservedProperty]:
        """

        :param basin3d_vocab:
        :return:
        """
        raise NotImplementedError

    def find_observed_properties(self, basin3d_vocab=None) -> Iterator[Optional[ObservedProperty]]:
        """

        :param basin3d_vocab:
        :return:
        """
        raise NotImplementedError

    def find_attribute_mapping(self, datasource_id, attr_type, attr_vocab) -> Optional[AttributeMapping]:
        """

        :param datasource_id:
        :param attr_type:
        :param attr_vocab:
        :return:
        """
        raise NotImplementedError

    def find_attribute_mappings(self, datasource, attr_type, attr_vocab, from_basin3d) -> Iterator[AttributeMapping]:
        """

        :param datasource:
        :param attr_type:
        :param attr_vocab:
        :param from_basin3d:
        :return:
        """
        raise NotImplementedError

    def find_datasource_vocab(self, datasource_id, attr_type, attr_vocab, b3d_query):
        """

        :param datasource_id:
        :param attr_type:
        :param attr_vocab:
        :param b3d_query:
        :return:
        """
        raise NotImplementedError

    def find_compound_mapping_attributes(self, datasource_id, attr_type, include_specified_type=False) -> list:
        """

        :param datasource_id:
        :param attr_type:
        :param include_specified_type:
        :return:
        """
        raise NotImplementedError

    def find_compound_mappings(self, datasource_id: str) -> list:
        """

        :param datasource_id:
        :return:
        """
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

            # get a set ready to collect the attribute types for unique compound mappings
            compound_mappings = set()

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

                # if the mapping is compound collect it for later parsing
                if MAPPING_DELIMITER in attr_type:
                    compound_mappings.add(attr_type)

        for compound_mapping in compound_mappings:
            attrs = compound_mapping.split(MAPPING_DELIMITER)
            for attr in attrs:
                cm = CatalogBase.CompoundMapping(
                    compound_mapping=compound_mapping,
                    attr_type=attr,
                    datasource=datasource)
                self._insert(cm)


class CatalogTinyDb(CatalogBase):

    def __init__(self, variable_filename: str = 'basin3d_observed_property_variables_vocabulary.csv'):
        super().__init__(variable_filename)

        self.in_memory_db = None
        self.in_memory_db_attr = None
        self.in_memory_db_cm = None
        self._observed_properties: Dict[str, ObservedProperty] = {}
        self._attribute_mappings: Dict[str, AttributeMapping] = {}
        self._compound_mapping: Dict[str, CatalogBase.CompoundMapping] = {}
        self._datasources: Dict[str, DataSource] = {}

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        return self.in_memory_db is not None

    def _get_datasource(self, datasource_id) -> Optional[DataSource]:
        """
        Access a datasource via the datasource_id
        :param datasource_id: str, the datasource id
        :return: a `basin3d.models.DataSource` object
        """
        return self._datasources.get(datasource_id, None)

    def _get_observed_property(self, basin3d_vocab) -> Optional[ObservedProperty]:
        """
        Access a single observed property

        :param basin3d_vocab: str, the observed property identifier
        :return: an `basin3d.models.ObservedProperty` object
        """
        return self._observed_properties.get(basin3d_vocab, None)

    def _get_attribute_mapping(self, datasource_id, attr_type, basin3d_vocab, datasource_vocab, **kwargs) -> Optional[AttributeMapping]:
        """
        Access a single attribute mapping

        :param datasource_id: str, datasource identifier
        :param attr_type: str, attribute type
        :param basin3d_vocab: str, BASIN-3D vocabulary
        :param datasource_vocab: str, datasource vocabulary
        :return: a `basin3d.models.AttributeMapping` object
        """
        return self._attribute_mappings.get(f'{datasource_id}-{attr_type}-{basin3d_vocab}-{datasource_vocab}', None)

    def _get_compound_mapping(self, datasource_id, attr_type, compound_mapping) -> Optional[CatalogBase.CompoundMapping]:
        """
        Access a single compound mapping

        :param datasource_id: str, the datasource identifier
        :param attr_type: str, single attribute type
        :param compound_mapping: str, compound mapping for attr_type
        :return: a `basin3d.catalog.CatalogBase.CompoundMapping` object
        """
        return self._compound_mapping.get(f'{datasource_id}-{attr_type}-{compound_mapping}', None)

    def _find_compound_mapping(self, datasource_id, attr_type) -> Optional[CatalogBase.CompoundMapping]:
        """
        Get the compound mapping for the specified attr_type

        :param datasource_id: datasource identifier
        :param attr_type: attribute type
        :return: a `basin3d.catalog.CatalogBase.CompoundMapping` object
        """
        if self.in_memory_db_cm is None:
            msg = 'Compound mapping database has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        attr_type = verify_attr_type(attr_type)

        results = self.in_memory_db_cm.search((query.datasource_id == datasource_id) & (query.attr_type == attr_type))

        if results:
            # there should only be one result
            return self._get_compound_mapping(**results[0])

        return None

    def find_observed_property(self, basin3d_vocab: str) -> Optional[ObservedProperty]:
        """
        Return the `basin3d.models.ObservedProperty` object for the BASIN-3D vocabulary specified.

        :param basin3d_vocab: BASIN-3D vocabulary
        :return: a `basin3d.models.ObservedProperty` object
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
        :return: generator that yields `basin3d.models.ObservedProperty` objects
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

    def find_attribute_mapping(self, datasource_id: str, attr_type: str, attr_vocab: str) -> Optional[AttributeMapping]:
        """
        Find the datasource attribute vocabulary to BASIN-3D mapping given a specific datasource_id, attr_type, and datasource attr_vocab.

        :param: datasource_id: the datasource identifier
        :param: attr_type: attribute type
        :param: attr_vocab: the attribute vocabulary; only datasource vocabulary currently supported
        :return: a `basin3d.models.AttributeMapping` object
        """
        if self.in_memory_db_attr is None:
            msg = 'Attribute Store has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        results = self.in_memory_db_attr.search(
            (query.datasource_vocab == attr_vocab) & (query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))
        basin3d_vocab = NO_MAPPING_TEXT
        datasource_vocab = attr_vocab

        if len(results) == 1:
            return self._get_attribute_mapping(**results[0])
        elif len(results) > 1:
            error_msg = (f'More than one attribute mapping found for datasource vocab: "{datasource_vocab}" '
                         f'in datasource: "{datasource_id}". This should never happen.')
            logger.critical(error_msg)
            raise CatalogException(error_msg)

        msg = f'No mapping was found for datasource vocab: "{datasource_vocab}" in datasource: "{datasource_id}".'
        datasource = self._get_datasource(datasource_id)

        if datasource is None:
            datasource = DataSource()
            msg = f'No datasource was found for id "{datasource_id}".'
            logger.warning(msg)

        return AttributeMapping(attr_type=attr_type, basin3d_vocab=basin3d_vocab, basin3d_desc=[],
                                datasource_vocab=datasource_vocab, datasource_desc=msg, datasource=datasource)

    def find_attribute_mappings(self, datasource_id: str = None, attr_type: str = None, attr_vocab: Union[str, List] = None,
                                from_basin3d: bool = False) -> Iterator[AttributeMapping]:
        """
        Find the list of attribute mappings given the specified fields. If no fields are specified, all registered attribute mappings will be returned.

        :param datasource_id: the datasource identifier
        :param attr_type: the attribute type
        :param attr_vocab:  the attribute vocabulary
        :param from_basin3d: boolean that says whether the attr_vocab is a BASIN-3D vocabulary. If not, then this a datasource vocabulary.
        :return: generator that yields `basin3d.models.AttributeMapping` objects
        """
        catalog_messages: List[str] = []

        if datasource_id and self._get_datasource(datasource_id) is None:
            msg = f'Specified datasource_id: "{datasource_id}" has not been registered.'
            logger.critical(msg)
            raise CatalogException(msg)

        if attr_type and attr_type not in MappedAttributeEnum.values():
            msg = f'"{attr_type}" is not an attribute type suppored by BASIN-3D.'
            logger.critical(msg)
            raise CatalogException(msg)

        if self.in_memory_db_attr is None:
            msg = 'Attribute Store has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        attr_vocab_list = []
        if attr_vocab:
            if isinstance(attr_vocab, str):
                attr_vocab = [attr_vocab]
            elif not isinstance(attr_vocab, List):
                raise CatalogException("attr_vocab must be a str or list")
            attr_vocab_list = attr_vocab.copy()

        def is_in(x, attr_vocabs=attr_vocab, is_from_basin3d=from_basin3d):
            if is_from_basin3d and MAPPING_DELIMITER in x:
                x_elements = x.split(MAPPING_DELIMITER)
                for x_element in x_elements:
                    if x_element in attr_vocabs:
                        return True
            else:
                return x in attr_vocabs

        # none for all 3 query parameters --> get all mapped variables back for all registered plugins
        if not datasource_id and not attr_vocab and not attr_type:
            # return all mapped attributes possible possible
            for attr_mapping in self._attribute_mappings.values():
                yield attr_mapping
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

            elif not attr_type and not attr_vocab:
                # This returns all possible attributes for a data source
                results = self.in_memory_db_attr.search(query.datasource_id == datasource_id)

            elif not attr_vocab:
                # Returns all attributes for a data source and attribute type
                results = self.in_memory_db_attr.search((query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))

            elif not attr_type:
                if from_basin3d:
                    results = self.in_memory_db_attr.search((query.basin3d_vocab.test(is_in)) & (query.datasource_id == datasource_id))
                else:
                    results = self.in_memory_db_attr.search((query.datasource_vocab.test(is_in)) & (query.datasource_id == datasource_id))

            elif from_basin3d:
                # Convert from BASIN-3D to DataSource variable name
                results = self.in_memory_db_attr.search(
                    (query.basin3d_vocab.test(is_in)) & (query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))

            else:
                # Convert from DataSource variable name to BASIN-3D
                results = self.in_memory_db_attr.search(
                    (query.datasource_vocab.test(is_in)) & (query.datasource_id == datasource_id) & (query.attr_type.search(attr_type)))

            # Yield the results
            attr_map: Optional[AttributeMapping]
            for r in results:
                attr_map = self._get_attribute_mapping(**r)
                if attr_map is None:
                    # ToDo: figure out if want to through an error here -- it should never happen
                    continue

                if attr_vocab:
                    vocabs = [attr_map.datasource_vocab]
                    if from_basin3d:
                        vocabs = attr_map.basin3d_vocab.split(MAPPING_DELIMITER)

                    for vocab in vocabs:
                        try:
                            idx = attr_vocab_list.index(vocab)
                            attr_vocab_list.pop(idx)
                        except ValueError:
                            pass

                yield attr_map

            if attr_vocab_list:
                attr_vocab_text = ', '.join(attr_vocab_list)
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

    def find_datasource_vocab(self, datasource_id: str, attr_type: str, basin3d_vocab: Union[str, list], b3d_query) -> list:
        """
        Find the datasource vocabulary(ies) for the specified datasource, attribute type, BASIN-3D vocabulary, and full query that may specify other attributes.
        Because multiple datasource vocabularies can be mapped to the same BASIN-3D vocabulary, the return is a list of the datasource vocabs.

        :param datasource_id: the datasource identifier
        :param attr_type: the attribute type
        :param basin3d_vocab: the BASIN-3D vocabulary
        :param b3d_query: either a QueryBase class or subclass object, or a dictionary
        :return: list of the datasource vocabularies
        """
        if self.in_memory_db_attr is None:
            msg = 'Attribute Store has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        # convert attr_type to the form in the database if necessary. e.g. OBSERVED_PROPERTIES --> OBSERVED_PROPERTY
        attr_type = verify_attr_type(attr_type)

        # is the attr_type part of a compound mapping?
        compound_mapping = self._find_compound_mapping(datasource_id, attr_type)

        b3d_vocab_combo_str = [basin3d_vocab]

        # if compound_mapping, find all the relevant value combos given the query
        if compound_mapping:
            compound_mapping_attrs = compound_mapping.compound_mapping  # e.g. OPV
            b3d_vocab_filter_lists = []  # list to hold lists of specified filters, one for each attr

            # loop thru each of the compound mapping attributes, build a list of lists of query combos
            for attr in compound_mapping_attrs.split(MAPPING_DELIMITER):
                attr_value = None

                # by default: match any number of characters excepting a new line for the attribute
                # replace this value below if a value for the attribute is specified in the query
                filter_values = ['.*']
                # if the attr is the attr_type, set the filter to the specified vocab
                if attr == attr_type:
                    filter_values = [basin3d_vocab]
                # if the other attribute is specified in the query, get the value
                elif issubclass(b3d_query.__class__, QueryBase) and hasattr(b3d_query, verify_query_param(attr, is_query=True)):
                    attr_value = getattr(b3d_query, verify_query_param(attr, is_query=True))
                elif isinstance(b3d_query, dict) and verify_query_param(attr) in b3d_query.keys():
                    attr_value = b3d_query.get(verify_query_param(attr))

                # if there is a value, replace the default value
                if attr_value:
                    filter_values = attr_value
                    # if the values are a str, change it to a list
                    if isinstance(attr_value, str):
                        filter_values = attr_value.split(',')

                # append the filter list to the main list
                b3d_vocab_filter_lists.append(filter_values)

            # create a list of sets contain the combinations of filter options for each attr
            b3d_vocab_combo_sets = list(product(*b3d_vocab_filter_lists))

            # change each set into a str for search
            b3d_vocab_combo_str = [MAPPING_DELIMITER.join(v) for v in b3d_vocab_combo_sets]

            # change the attr_type to the compound mapping str
            attr_type = compound_mapping_attrs

        ds_vocab = []
        no_match_list = []

        # Loop thru the list of vocabulary string combos to search the attribute mapping database
        for basin3d_vocab_str in b3d_vocab_combo_str:
            query_results = self.in_memory_db_attr.search(
                (query.datasource_id == datasource_id) & (query.attr_type == attr_type) & (query.basin3d_vocab.matches(basin3d_vocab_str)))
            if query_results:
                for qr in query_results:

                    # pop the datasource_desc without altering the restuls to find the attribute mapping
                    qr_copy = qr.copy()
                    qr_copy.pop('datasource_desc')

                    attr_mapping = self._get_attribute_mapping(**qr_copy)
                    if attr_mapping is not None:
                        ds_vocab.append(attr_mapping.datasource_vocab)
                        continue

            # if not result for the combo, add it to the no_match_list
            no_match_list.append(basin3d_vocab_str)

        if not ds_vocab:
            ds_vocab = [NO_MAPPING_TEXT]

        if no_match_list:
            logger.info(f'Datasource "{datasource_id}" did not have matches for attr_type "{attr_type}" and BASIN-3D vocab: {", ".join(no_match_list)}.')

        return ds_vocab

    def find_compound_mapping_attributes(self, datasource_id, attr_type, include_specified_type=False, is_query=False) -> list:
        """
        Return the attributes if attr_type is part of a compound mapping

        :param datasource_id: the datasource identifier
        :param attr_type: the attribute type
        :param include_specified_type: bool, True = include in the return the specified attr_type. False: return the other attribute types that are part of the compound mapping.
        :return: list of attributes in the compound mapping
        """
        if self.in_memory_db_cm is None:
            msg = 'Compound mapping database has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        attr_type = verify_attr_type(attr_type)
        results = self.in_memory_db_cm.search((query.datasource_id == datasource_id) & (query.compound_mapping.matches(attr_type)))

        attr_types = []
        for r in results:
            compound_attr_type = getattr(self._get_compound_mapping(**r), 'attr_type')
            if not include_specified_type and compound_attr_type == attr_type:
                continue
            if is_query:
                compound_attr_type = verify_query_param(compound_attr_type, is_query)
            attr_types.append(compound_attr_type)

        return attr_types

    def find_compound_mappings(self, datasource_id: str) -> list:
        """

        :param datasource_id:
        :return:
        """
        if self.in_memory_db_cm is None:
            msg = 'Compound mapping database has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        from tinydb import Query
        query = Query()

        compound_mappings = set()

        results = self.in_memory_db_cm.search((query.datasource_id == datasource_id))

        for r in results:
            compound_mapping = self._get_compound_mapping(**r)
            compound_mappings.add(compound_mapping.compound_mapping)

        return list(compound_mappings)

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
        self.in_memory_db_cm = self.in_memory_db.table('compound_mappings')
        self.in_memory_db_cm.truncate()

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
            elif isinstance(record, CatalogBase.CompoundMapping):
                key = f'{record.datasource.id}-{record.attr_type}-{record.compound_mapping}'
                logger.debug(key)
                self._compound_mapping[key] = record
                self.in_memory_db_cm.insert({'datasource_id': record.datasource.id,
                                             'attr_type': record.attr_type,
                                             'compound_mapping': record.compound_mapping, })
            elif isinstance(record, DataSource):
                self._datasources[record.id] = record
        else:
            msg = 'Could not insert record. Catalog not initialized.'
            logger.critical(msg)
            raise CatalogException(msg)
