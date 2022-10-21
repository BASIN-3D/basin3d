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
from basin3d.core.models import DataSource, AttributeMapping, ObservedProperty  # ObservedProperty

logger = monitor.get_logger(__name__)


class CatalogException(Exception):
    pass


def _verify_attr_type(attr_type: str) -> str:
    """

    :param attr_type:
    :return:
    """

    attr_type = attr_type.upper()

    if attr_type == 'OBSERVED_PROPERTY_VARIABLE' or attr_type == 'OBSERVED_PROPERTY_VARIABLES':
        attr_type = 'OBSERVED_PROPERTY'

    return attr_type


def _verify_query_var(attr_type: str, is_query=True) -> str:
    """

    :param attr_type:
    :return:
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

        :param str_value:
        :param enum_type:
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

            compound_mappings = set()
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
                        logger.warning(f'Attribute type {a_type} is not supported. Skipping mapping')
                        valid_type_vocab = False

                    if not b3d_desc:
                        logger.warning(f'{datasource.id}: basin3d_vocab {b3d_vocab} for attr_type {a_type} is not a valid BASIN-3D vocabulary. Skipping attribute mapping.')
                        valid_type_vocab = False

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
        self._observed_properties: Dict[str, ObservedProperty] = {}
        self._attribute_mappings: Dict[str, AttributeMapping] = {}
        self._compound_mapping: Dict[str, CatalogBase.CompoundMapping] = {}
        self._datasources: Dict[str, DataSource] = {}

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        return self.in_memory_db is not None

    def _get_datasource(self, datasource_id) -> Optional[DataSource]:
        """

        :param datasource_id:
        :return:
        """
        return self._datasources.get(datasource_id, None)

    def _get_observed_property(self, basin3d_vocab) -> Optional[ObservedProperty]:
        """
        Access a single observed property variable

        :param basin3d_vocab: the observed property variable identifier
        :return:
        """
        return self._observed_properties.get(basin3d_vocab, None)

    def _get_attribute_mapping(self, datasource_id, attr_type, basin3d_vocab, datasource_vocab, **kwargs) -> Optional[AttributeMapping]:
        """
        Access a single attribute mapping

        :param datasource_id:
        :param attr_type:
        :param basin3d_vocab:
        :param datasource_vocab:
        :return:
        """
        return self._attribute_mappings.get(f'{datasource_id}-{attr_type}-{basin3d_vocab}-{datasource_vocab}', None)

    def _get_compound_mapping(self, datasource_id, attr_type, compound_mapping) -> Optional[CatalogBase.CompoundMapping]:
        """

        :param datasource_id:
        :param attr_type:
        :param compound_mapping:
        :return:
        """
        return self._compound_mapping.get(f'{datasource_id}-{attr_type}-{compound_mapping}', None)

    # ToDo: check the return of None!
    def _find_compound_mapping(self, datasource_id, attr_type):
        """
        Get the compound mapping for the specified attr_type
        :param datasource_id:
        :param attr_type:
        :return:
        """
        if self.in_memory_db_cm is None:
            raise CatalogException("Compound mapping database has not been initialized")

        from tinydb import Query
        query = Query()

        attr_type = _verify_attr_type(attr_type)

        results = self.in_memory_db_cm.search((query.datasource_id == datasource_id) & (query.attr_type == attr_type))

        if results:
            return self._get_compound_mapping(**results[0])

        return None

    def find_observed_property(self, basin3d_vocab) -> Optional[ObservedProperty]:
        """
        Return the ObservedPropertyVariable object for the basin3d_vocab
        :param basin3d_vocab:
        :return: ObservedPropertyVariable
        """

        if not self._observed_properties:
            msg = "Variable Store has not been initialized."
            logger.critical(msg)
            raise CatalogException(msg)

        return self._get_observed_property(basin3d_vocab)

    def find_observed_properties(self, basin3d_vocab: Optional[List[str]] = None) -> Iterator[Optional[ObservedProperty]]:
        """

        :param basin3d_vocab:  The :class:`~basin3d.models.ObservedPropertyVariable`
             names to convert
        :type basin3d_vocab: iterable
        :return: ObservedPropertyVariable objects
        :rtype: iterable
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
        Convert the given datasource attribute vocabulary to BASIN-3D from :class:`~basin3d.models.DataSource`
        attribute.

        :param: datasource_id: the datasource identifier
        :param: attr_type: attribute type
        :param: attr_vocab:  the attribute vocabulary; only datasource vocabulary supported
        :return: A variable name
        :rtype: str
        """
        if self.in_memory_db_attr is None:
            raise CatalogException("Attribute Store has not been initialized")

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
        Convert the given list of attributes to either BASIN-3D from :class:`~basin3d.models.DataSource`
        attribute id or the other way around.

        :param datasource_id: the datasource identifier
        :param attr_type: the attribute type
        :param attr_vocab:  The :class:`~basin3d.models.AttributeMapping` names to convert
        :param from_basin3d: boolean that says whether the variable name is a
            BASIN-3D variable. If not, then this a datasource variable names.
        :return: list of variable names
        :rtype: iterable
        """
        catalog_messages: List[str] = []

        if datasource_id and self._get_datasource(datasource_id) is None:
            raise CatalogException(f'Specified datasource_id: "{datasource_id}" has not been registered.')

        if attr_type and attr_type not in MappedAttributeEnum.values():
            raise CatalogException(f'"{attr_type}" is not an attribute type suppored by BASIN-3D.')

        if self.in_memory_db_attr is None:
            raise CatalogException('Attribute Store has not been initialized')

        from tinydb import Query
        query = Query()

        # is_in = None
        attr_vocab_list = []
        if attr_vocab:
            if isinstance(attr_vocab, str):
                attr_vocab = [attr_vocab]
            elif not isinstance(attr_vocab, List):
                raise CatalogException("attr_vocab must be a str or list")
            # is_in = lambda x: x in attr_vocab
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

    def find_datasource_vocab(self, datasource_id, attr_type, basin3d_vocab, b3d_query) -> list:
        """

        :param datasource_id:
        :param attr_type:
        :param basin3d_vocab:
        :param b3d_query:
        :return:
        """
        if self.in_memory_db_attr is None:
            raise CatalogException("Compound mapping database has not been initialized")

        from tinydb import Query
        query = Query()

        # is the attr_type part of a compound mapping?
        attr_type = _verify_attr_type(attr_type)
        compound_mapping = self._find_compound_mapping(datasource_id, attr_type)

        # b3d_vocab_query = query.basin3d_vocab.fragment(basin3d_vocab)
        b3d_vocab_combo_str = [basin3d_vocab]

        # if so, find all the relevant value combos given the query
        if compound_mapping:
            compound_mapping_attrs = compound_mapping.compound_mapping  # e.g. OPV
            b3d_vocab_filter_lists = []  # list to hold lists of specified filters, one for each attr

            # ToDo: refigure out this logic and write some tests for it.
            # loop thru each of the compound mapping attributes
            for attr in compound_mapping_attrs.split(MAPPING_DELIMITER):
                # by default: match any number of characters excepting a new line for the attribute
                filter_values = ['.*']
                # if the attr is the attr_type, set the filter to the specified vocab
                if attr == attr_type:
                    filter_values = [basin3d_vocab]
                # if the other attribute is specified in the query
                elif _verify_query_var(attr, is_query=True) in b3d_query.get_mapped_fields():
                    # get the filter values
                    attr_value = getattr(b3d_query, _verify_query_var(attr_type, is_query=True))
                    if not attr_value:
                        filter_values = attr_value
                        # if the values are a str, change it to a list
                        if isinstance(attr_value, str):
                            # filter_values = filter_values.split(',')
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
        for basin3d_vocab_str in b3d_vocab_combo_str:
            query_results = self.in_memory_db_attr.search(
                (query.datasource_id == datasource_id) & (query.attr_type == attr_type) & (query.basin3d_vocab.matches(basin3d_vocab_str)))
            if query_results:
                for qr in query_results:
                    qr_copy = qr.copy()
                    qr_copy.pop('datasource_desc')
                    attr_mapping = self._get_attribute_mapping(**qr_copy)
                    if attr_mapping is not None:
                        ds_vocab.append(attr_mapping.datasource_vocab)

        if not ds_vocab:
            ds_vocab = [NO_MAPPING_TEXT]

        return ds_vocab

    def find_compound_mapping_attributes(self, datasource_id, attr_type, include_specified_type=False) -> list:
        """
        Return the attributes if attr_type is part of a compound mapping
        :param datasource_id:
        :param attr_type:
        :param include_specified_type: bool
        :return:
        """
        if self.in_memory_db_cm is None:
            raise CatalogException("Compound mapping database has not been initialized")

        from tinydb import Query
        query = Query()

        attr_type = _verify_attr_type(attr_type)
        results = self.in_memory_db_cm.search((query.datasource_id == datasource_id) & (query.compound_mapping.matches(attr_type)))

        attr_types = []
        for r in results:
            compound_attr_type = getattr(self._get_compound_mapping(**r), 'attr_type')
            if not include_specified_type and compound_attr_type == attr_type:
                continue
            attr_types.append(compound_attr_type)

        return attr_types

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
            # elif isinstance(record, ObservedProperty):
            #     self._observed_properties[
            #         f"{record.datasource.id}-{record.observed_property_variable.basin3d_id}-{record.datasource_variable}"] = record
            #     self.in_memory_db_op.insert({'datasource_id': record.datasource.id,
            #                                  'datasource_variable_id': record.datasource_variable,
            #                                  'basin3d_id': record.observed_property_variable.basin3d_id, })
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
            raise CatalogException(f'Could not insert record.  Catalog not initialize')
