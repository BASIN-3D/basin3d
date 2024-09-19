"""

.. currentmodule:: basin3d.core.sqlalchemy_models

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` catalog classes
:module author: Valerie C. Hendrix <vchendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""
import csv
from sqlalchemy.exc import SQLAlchemyError, NoResultFound, IntegrityError

import basin3d
from basin3d.core import monitor, models
from importlib import resources
from inspect import getmodule
from string import whitespace
from typing import Iterator, List, Optional, Union

from basin3d.core.schema.enum import MAPPING_DELIMITER, NO_MAPPING_TEXT, MappedAttributeEnum, \
    set_mapped_attribute_enum_type, BaseEnum
from basin3d.core import sqlalchemy_models

logger = monitor.get_logger(__name__)
"""The logger for the catalog"""

DATASOURCE_NONE = models.DataSource(name="", location="", id_prefix="", id="", credentials={})
"""The default DataSource object when a DataSource is not found"""


class CatalogException(Exception):
    """The exception class for the Catalog"""
    pass


class CatalogBase:

    def __init__(self, variable_filename: str = 'basin3d_observed_property_vocabulary.csv'):
        """
        Initialize the catalog

        :param variable_filename: the filename of the observed property vocabulary
        """
        self.variable_dir = 'basin3d.data'  # location of the variable file
        self.variable_filename = variable_filename  # observed property vocabulary filename
        self._plugin_ids: List[str] = []  # list of plugin ids (updated on initialization)

    def initialize(self, plugin_list: list):
        """
        Initialize the catalog. This method should be called before any other method.


        :param plugin_list: list of plugins
        :return: tinyDB object
        """
        if not self.is_initialized():
            logger.debug(f"Initializing {self.__class__.__name__} metadata catalog ")

            def _get_plugin_id(plugin):
                """Get the plugin id from the plugin"""
                try:
                    if isinstance(plugin, str):
                        return basin3d.core.plugin.PluginMount.plugins[plugin].get_meta().id
                    return plugin.get_meta().id
                except Exception:
                    logger.error(
                        'Could not retrieve plugin_id. Check that plugin is configured / registered properly.')
                    raise CatalogException

            self._plugin_ids = plugin_list and [_get_plugin_id(plugin) for plugin in plugin_list if plugin] or []

            # initiate db
            self._init_catalog(plugin_ids=self._plugin_ids)

            # generate variable store
            self._gen_variable_store()

            # Load plugins, get the mapping file, and insert into the catalog datastore
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

            logger.info(f"Initialized {self.__class__.__name__} metadata catalog ")

    # ---------------------------------
    # Methods to be over-witten by children classes

    def _init_catalog(self, **kwargs):
        """
        Initialize the catalog database

        """
        raise NotImplementedError

    def _insert(self, record):
        """
        Insert the record

        :param record: the record to insert
        """
        raise NotImplementedError

    def _get_datasource(self, datasource_id) -> Optional[basin3d.core.models.DataSource]:
        """
        Access a single datasource

        :param datasource_id: the datasource_identifier
        """
        raise NotImplementedError

    def _get_observed_property(self, basin3d_vocab) -> Optional[basin3d.core.models.ObservedProperty]:
        """
        Access a single observed property variable

        :param basin3d_vocab: the observed property variable identifier
        """
        raise NotImplementedError

    def _get_attribute_mapping(self, datasource_id, attr_type, basin3d_vocab, datasource_vocab, **kwargs) -> Optional[
            basin3d.core.models.AttributeMapping]:
        """
        Access a single attribute mapping

        :param datasource_id: the datasource identifier, e.g., plugin_id
        :param attr_type: the attribute type, e.g., STATISTIC
        :param basin3d_vocab: the BASIN-3D vocabulary, e.g., Max
        :param datasource_vocab: the datasource vocabulary, e.g., max
        :return: the attribute mapping
        """
        raise NotImplementedError

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""
        raise NotImplementedError

    def find_observed_property(self, basin3d_vocab) -> Optional[basin3d.core.models.ObservedProperty]:
        """
        Return the :class:`basin3d.models.ObservedProperty` object for the BASIN-3D vocabulary specified.

        :param basin3d_vocab: BASIN-3D vocabulary
        :return: a :class:`basin3d.models.ObservedProperty` object
        """
        raise NotImplementedError

    def find_observed_properties(self, basin3d_vocab=None) -> Iterator[Optional[basin3d.core.models.ObservedProperty]]:
        """
        Report the observed_properties available based on the BASIN-3D vocabularies specified. If no BASIN-3D vocabularies are specified, then return all observed properties available.

        :param basin3d_vocab: list of the BASIN-3D observed properties
        :return: generator that yields :class:`basin3d.models.ObservedProperty` objects
        """
        raise NotImplementedError

    def find_datasource_attribute_mapping(self, datasource_id, attr_type, attr_vocab) -> Optional[
            basin3d.core.models.AttributeMapping]:
        """
        Find the datasource attribute vocabulary to BASIN-3D mapping given a specific datasource_id, attr_type, and datasource attr_vocab.

        :param: datasource_id: the datasource identifier
        :param: attr_type: attribute type
        :param: datasource_vocab: the datasource attribute vocabulary
        :return: a :class:`basin3d.models.AttributeMapping` object
        """
        raise NotImplementedError

    def find_attribute_mappings(self, datasource, attr_type, attr_vocab, from_basin3d) -> Iterator[
            basin3d.core.models.AttributeMapping]:
        """
        Find the list of attribute mappings given the specified fields.
        Exact matches are returned (see attr_vocab formats below for BASIN-3D vocab nuances).
        If no fields are specified, all registered attribute mappings will be returned.

        :param datasource: the datasource object
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
        Generate a variable store. Loads this from the provided vocabulary CSV file.

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

                observed_property_variable = basin3d.core.models.ObservedProperty(
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

    def _process_plugin_attr_mapping(self, plugin, filename: str, datasource: basin3d.core.models.DataSource):
        """
        Process the plugin attribute mapping file

        :param plugin: the plugin, e.g., the plugin object
        :param filename: the filename of the mapping file, e.g., 'plugin_mapping.csv'
        :param datasource: the datasource object
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
                for a_type, b3d_vocab in zip(attr_type.split(MAPPING_DELIMITER),
                                             basin3d_vocab.split(MAPPING_DELIMITER)):
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
                        logger.warning(
                            f'Datasource {datasource.id}: Attribute type {a_type} is not supported. Skipping mapping.')
                        valid_type_vocab = False
                        break

                    if not b3d_desc:
                        logger.warning(
                            f'Datasource {datasource.id}: basin3d_vocab {b3d_vocab} for attr_type {a_type} '
                            f'is not a valid BASIN-3D vocabulary. Skipping attribute mapping.')
                        valid_type_vocab = False
                        break

                    basin3d_desc.append(b3d_desc)

                if not valid_type_vocab:
                    continue

                attr_mapping = basin3d.core.models.AttributeMapping(
                    attr_type=attr_type,
                    basin3d_vocab=basin3d_vocab,
                    basin3d_desc=basin3d_desc,
                    datasource_vocab=datasource_vocab,
                    datasource_desc=datasource_desc,
                    datasource=datasource)

                self._insert(attr_mapping)
                logger.debug(f"{datasource.id}: Mapped {attr_type} {datasource_vocab} to {basin3d_vocab}")


class CatalogSqlAlchemy(CatalogBase):

    def __init__(self, variable_filename: str = 'basin3d_observed_property_vocabulary.csv'):
        super().__init__(variable_filename)

        # Clear the database, on initialization
        basin3d.core.sqlalchemy_models.clear_database()

    def is_initialized(self) -> bool:
        """Has the catalog been initialized?"""

        session = sqlalchemy_models.Session()

        try:
            datasources = session.query(sqlalchemy_models.DataSource).count()
            if isinstance(datasources, int):
                return datasources > 0
            logger.debug('Catalog not initialized')
            return False
        except SQLAlchemyError as e:
            logger.error(f'SQLAlchemy error: {e}')
            return False
        finally:
            session.close()

    def _convert_observed_property(self, sqlalchemy_opv) -> Optional[basin3d.core.models.ObservedProperty]:
        """
        Convert SQL Alchemy observed property variable to basin3d

        :param sqlalchemy_opv:
        :return: basin3d observed property
        """

        return basin3d.core.models.ObservedProperty(
            basin3d_vocab=sqlalchemy_opv.basin3d_vocab,
            full_name=sqlalchemy_opv.full_name,
            categories=sqlalchemy_opv.categories.split(","),
            units=sqlalchemy_opv.units
        )

    def _convert_attribute_mapping(self, sqlalchemy_am) -> Optional[basin3d.core.models.AttributeMapping]:
        """
        Convert SQL Alchemy attribute_mapping

        :param sqlalchemy_am: the sqlalchemy attribute mapping
        :return: the basin3d attribute mapping
        """

        attr_type_list = sqlalchemy_am.attr_type.split(MAPPING_DELIMITER)

        basin3d_desc_list = []
        if isinstance(sqlalchemy_am.basin3d_desc, list):
            basin3d_desc_list = sqlalchemy_am.basin3d_desc

        basin3d_desc = []

        for attr_type, desc in zip(attr_type_list, basin3d_desc_list):
            if attr_type == MappedAttributeEnum.OBSERVED_PROPERTY.value:
                op = basin3d.core.models.ObservedProperty(
                    basin3d_vocab=desc.get('basin3d_vocab'),
                    full_name=desc.get('full_name'),
                    categories=desc.get('categories'),
                    units=desc.get('units')
                )
                basin3d_desc.append(op)
            elif attr_type in MappedAttributeEnum.values():
                attr_enum_class = set_mapped_attribute_enum_type(attr_type)
                attr_type_enum = getattr(attr_enum_class, desc)
                basin3d_desc.append(attr_type_enum)
            else:
                basin3d_desc.append(desc)

        return basin3d.core.models.AttributeMapping(
            attr_type=sqlalchemy_am.attr_type,
            basin3d_vocab=sqlalchemy_am.basin3d_vocab,
            basin3d_desc=basin3d_desc,
            datasource_vocab=sqlalchemy_am.datasource_vocab,
            datasource_desc=sqlalchemy_am.datasource_desc,
            datasource=basin3d.core.models.DataSource(id=sqlalchemy_am.datasource.plugin_id,
                                                      name=sqlalchemy_am.datasource.name,
                                                      location=sqlalchemy_am.datasource.location,
                                                      id_prefix=sqlalchemy_am.datasource.id_prefix, )
        )

    def _convert_basin3d_attr_mapping_basin3d_desc(self, basin3d_desc: list) -> list:
        """
        Convert the basin3d_desc to a JSON ready format

        :param basin3d_desc: the basin3d_desc, a list of observed properties, enums, or strings
        :return: a list of JSON ready basin3d_desc
        """
        json_ready_basin3d_desc = []

        for desc in basin3d_desc:
            if isinstance(desc, basin3d.core.models.ObservedProperty):
                json_ready_basin3d_desc.append(desc.to_dict())
            elif isinstance(desc, BaseEnum):
                json_ready_basin3d_desc.append(desc.value)
            else:
                json_ready_basin3d_desc.append(desc)

        return json_ready_basin3d_desc

    def _get_observed_property(self, basin3d_vocab) -> Optional[basin3d.core.models.ObservedProperty]:
        """
        Access a single observed property variable

        :param basin3d_vocab: the observed property name
        :return: the observed property, or None if not found
        """
        session = sqlalchemy_models.Session()

        try:
            opv = session.query(sqlalchemy_models.ObservedProperty).filter_by(basin3d_vocab=basin3d_vocab).one_or_none()
            if opv:
                return self._convert_observed_property(opv)
            return None
        except SQLAlchemyError as e:
            if not isinstance(e, NoResultFound):
                raise e
            return None
        finally:
            session.close()

    def _get_attribute_mapping(self, datasource_id, attr_type, basin3d_vocab, datasource_vocab, **kwargs) -> Optional[
            basin3d.core.models.AttributeMapping]:
        """
        Access a single attribute mapping. If not found, return None.

        :param datasource_id: the datasource identifier, e.g., plugin_id
        :param attr_type: the attribute type, e.g., STATISTIC
        :param basin3d_vocab: the BASIN-3D vocabulary, e.g., Max
        :param datasource_vocab: the datasource vocabulary, e.g., max
        :param kwargs: additional keyword arguments. Not used.

        :return: the attribute mapping, or None if not found
        """
        if not self.is_initialized():
            raise CatalogException("Datasource catalog has not been initialized")

        session = sqlalchemy_models.Session()

        try:
            opv = session.query(sqlalchemy_models.AttributeMapping).filter_by(
                datasource_id=datasource_id, attr_type=attr_type, basin3d_vocab=basin3d_vocab,
                datasource_vocab=datasource_vocab).one_or_none()
            if opv:
                return self._convert_attribute_mapping(opv)
            return None
        except SQLAlchemyError as e:
            if not isinstance(e, NoResultFound):
                raise e
            return None
        finally:
            session.close()

    def find_observed_property(self, basin3d_vocab) -> Optional[basin3d.core.models.ObservedProperty]:
        """
        Return the :class:`basin3d.models.ObservedProperty` object for the BASIN-3D vocabulary specified.

        :param basin3d_vocab: BASIN-3D vocabulary
        :return: a :class:`basin3d.models.ObservedProperty` object
        """
        if not self.is_initialized():
            msg = "Variable Store has not been initialized."
            logger.critical(msg)
            raise CatalogException(msg)

        return self._get_observed_property(basin3d_vocab)

    def find_observed_properties(self, basin3d_vocab: Optional[List[str]] = None) -> Iterator[
            Optional[basin3d.core.models.ObservedProperty]]:
        """
        Report the observed_properties available based on the BASIN-3D vocabularies specified. If no BASIN-3D vocabularies are specified, then return all observed properties available.

        :param basin3d_vocab: list of the BASIN-3D observed properties
        :return: generator that yields :class:`basin3d.models.ObservedProperty` objects
        """
        if not self.is_initialized():
            msg = "Variable Store has not been initialized."
            logger.critical(msg)
            raise CatalogException(msg)

        session = sqlalchemy_models.Session()

        try:
            if not basin3d_vocab:
                for opv in session.query(sqlalchemy_models.ObservedProperty).all():
                    yield self._convert_observed_property(opv)
            else:
                for b3d_vocab in basin3d_vocab:
                    b3d_opv: Optional[basin3d.core.models.ObservedProperty] = self._get_observed_property(b3d_vocab)
                    if b3d_opv is not None:
                        yield b3d_opv
                    else:
                        logger.warning(f'BASIN-3D does not support variable {b3d_vocab}')
        finally:
            session.close()

    def find_datasource_attribute_mapping(self, datasource_id: str, attr_type: str, datasource_vocab: str) -> Optional[
            basin3d.core.models.AttributeMapping]:
        """
        Find the datasource attribute vocabulary to BASIN-3D mapping given a specific datasource_id, attr_type, and datasource attr_vocab.

        :param datasource_id: the datasource identifier
        :param attr_type: attribute type
        :param datasource_vocab: the datasource attribute vocabulary
        :return: a :class:`basin3d.models.AttributeMapping` object

        """
        if not self.is_initialized():
            msg = 'Attribute Store has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        session = sqlalchemy_models.Session()

        msg = (f'No mapping was found for attr: "{attr_type}" and for datasource vocab: "{datasource_vocab}" '
               f'in datasource: "{datasource_id}".')

        try:
            datasource = session.query(sqlalchemy_models.DataSource).filter_by(plugin_id=datasource_id).one_or_none()
            opv = None
            query_params = {}
            if datasource is None:
                msg = f'No datasource was found for id "{datasource_id}".'
                basin3d_datasource = DATASOURCE_NONE
            else:
                # Set up the search parameters
                query_params = {
                    'datasource_id': datasource.id,
                    'datasource_vocab': datasource_vocab
                }
                basin3d_datasource = \
                    basin3d.core.models.DataSource(
                        id=datasource.plugin_id, name=datasource.name, location=datasource.location, id_prefix=datasource.id_prefix)  # type: ignore

            # Set up empty AttributeMapping in case where mapping is not found or another error occurs
            attr_mapping = basin3d.core.models.AttributeMapping(attr_type=attr_type, basin3d_vocab=NO_MAPPING_TEXT,
                                                                basin3d_desc=[],
                                                                datasource_vocab=datasource_vocab, datasource_desc=msg,
                                                                datasource=basin3d_datasource)
            if datasource:
                opv = session.query(sqlalchemy_models.AttributeMapping).filter_by(**query_params).filter(
                    sqlalchemy_models.AttributeMapping.attr_type.contains(attr_type)).one_or_none()
            if opv is None:
                return attr_mapping
            return self._convert_attribute_mapping(opv)
        except SQLAlchemyError as e:
            if not isinstance(e, NoResultFound):
                raise e
            return attr_mapping
        finally:
            session.close()

    def find_attribute_mappings(self, datasource_id: str = None, attr_type: str = None,
                                attr_vocab: Union[str, List] = None,
                                from_basin3d: bool = False) -> Iterator[basin3d.core.models.AttributeMapping]:
        """
        Find the list of attribute mappings given the specified fields. Exact matches are returned
        (see attr_vocab formats below for BASIN-3D vocab nuances).
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

        if not self.is_initialized():
            msg = 'Attribute Store has not been initialized.'
            logger.critical(msg)
            raise CatalogException(msg)

        def construct_attr_vocab_query(attr_vocab_list, is_from_basin3d):
            from sqlalchemy import or_
            query = []
            for a_vocab in attr_vocab_list:
                if not is_from_basin3d:
                    query.append(sqlalchemy_models.AttributeMapping.datasource_vocab == a_vocab)
                elif MAPPING_DELIMITER in a_vocab:
                    query.append(sqlalchemy_models.AttributeMapping.basin3d_vocab.op('regexp')(a_vocab))
                else:
                    query.append(or_(
                        sqlalchemy_models.AttributeMapping.basin3d_vocab == a_vocab,
                        sqlalchemy_models.AttributeMapping.basin3d_vocab.op('regexp')(f'.*:{a_vocab}'),
                        sqlalchemy_models.AttributeMapping.basin3d_vocab.op('regexp')(f'{a_vocab}:.*'),
                        sqlalchemy_models.AttributeMapping.basin3d_vocab.op('regexp')(f'.*:{a_vocab}:.*')
                    ))
            return or_(*query)

        session = sqlalchemy_models.Session()
        query_params = []

        if datasource_id is not None:
            try:
                ds = session.query(sqlalchemy_models.DataSource).filter_by(plugin_id=datasource_id).one_or_none()
                if ds is None:
                    logger.warning(
                        f'No datasource for datasource_id {datasource_id} was found. Check plugin initialization')
                    raise CatalogException(f'No datasource for datasource_id {datasource_id} was found.')
                else:
                    query_params.append(sqlalchemy_models.AttributeMapping.datasource_id == ds.id)
            except SQLAlchemyError as e:
                if not isinstance(e, NoResultFound):
                    raise CatalogException(e)

        if attr_type is not None:
            if attr_type not in MappedAttributeEnum.values():
                logger.warning(f'Attribute type {attr_type} is invalid')
                raise CatalogException(f'Attribute type {attr_type} is invalid')
            else:
                query_params.append(sqlalchemy_models.AttributeMapping.attr_type.contains(attr_type))

        if attr_vocab:
            if isinstance(attr_vocab, str):
                attr_vocab = [attr_vocab]
            elif not isinstance(attr_vocab, list):
                raise CatalogException("attr_vocab must be a str or list")
            attr_vocab_query = construct_attr_vocab_query(attr_vocab, from_basin3d)
            query_params.append(attr_vocab_query)

        try:
            attr_mappings = session.query(sqlalchemy_models.AttributeMapping).filter(*query_params).all()
            vocab_source_type = 'datasource' if not from_basin3d else 'BASIN-3D'

            if not attr_mappings:
                logger.info(
                    f'No attribute mappings found for specified parameters: datasource id = '
                    f'"{datasource_id}", attribute type = "{attr_type}", {vocab_source_type} '
                    f'vocabularies: {attr_vocab and ",".join(attr_vocab) or None}.')
            elif attr_vocab and len(attr_mappings) != len(attr_vocab):
                # Find missing vocab in attr_mappings
                if from_basin3d:
                    not_found_attr_vocab = [vocab for vocab in attr_vocab if
                                            vocab not in [am.basin3d_vocab for am in attr_mappings]]
                else:
                    not_found_attr_vocab = [vocab for vocab in attr_vocab if
                                            vocab not in [am.datasource_vocab for am in attr_mappings]]
                logger.warning(
                    f'No attribute mappings found for the following {vocab_source_type} '
                    f'vocabularies: {",".join(not_found_attr_vocab)}. Note: specified datasource id = {datasource_id} and attribute type = {attr_type}.')
        except SQLAlchemyError as e:
            if not isinstance(e, NoResultFound):
                raise e

        for attr_mapping in attr_mappings:
            value = self._convert_attribute_mapping(attr_mapping)
            if value:
                yield value

        session.close()

    def _init_catalog(self, **kwargs):
        """
        Initialize the catalog database
        """
        if not self.is_initialized():
            session = sqlalchemy_models.Session()

            from basin3d.core.plugin import PluginMount
            for name, plugin in PluginMount.plugins.items():

                # Were the plugins passed in? If so, only load the plugins that are in the list
                if ("plugin_ids" in kwargs and plugin.get_meta().id in kwargs[
                   "plugin_ids"]) or "plugin_ids" not in kwargs or kwargs["plugin_ids"] == []:
                    module_name = plugin.__module__
                    class_name = plugin.__name__

                    logger.info("Loading Plugin = {}.{}".format(module_name, class_name))

                    try:
                        datasource = session.query(sqlalchemy_models.DataSource).filter_by(
                            plugin_id=plugin.get_meta().id).one_or_none()
                        if datasource is None:
                            logger.info("Registering NEW Data Source Plugin '{}.{}'".format(module_name, class_name))
                            datasource = sqlalchemy_models.DataSource()
                            if hasattr(plugin.get_meta(), "connection_class"):
                                datasource.credentials = plugin.get_meta().connection_class.get_credentials_format()

                        # Update the datasource
                        datasource.plugin_id = plugin.get_meta().id
                        datasource.name = plugin.get_meta().name
                        datasource.location = plugin.get_meta().location
                        datasource.id_prefix = plugin.get_meta().id_prefix
                        datasource.plugin_module = module_name
                        datasource.plugin_class = class_name
                        session.add(datasource)
                        session.commit()
                        logger.info("Updated Data Source '{}'".format(plugin.get_meta().id))
                    except SQLAlchemyError as e:
                        session.rollback()
                        logger.error(f"Error loading plugin {module_name}.{class_name}: {e}")
                    finally:
                        session.close()

    def _insert(self, record):
        """
        Insert the record. This method is used to insert the record into the database.


        :param record: the record to insert
        """
        if self.is_initialized():
            session = sqlalchemy_models.Session()
            try:
                if isinstance(record, basin3d.core.models.ObservedProperty):
                    try:
                        p = sqlalchemy_models.ObservedProperty(
                            basin3d_vocab=record.basin3d_vocab,
                            full_name=record.full_name,
                            categories=",".join(record.categories),  # type: ignore
                            units=record.units
                        )
                        session.add(p)
                        session.commit()
                        logger.info(f'inserted {record.basin3d_vocab}')
                    except IntegrityError as ie:
                        # This object has already been loaded
                        session.rollback()
                        logger.debug(f'Integrity error for OP: {ie}')
                    except Exception as e:
                        session.rollback()
                        logger.warning(f"Error Registering ObservedProperty '{record.basin3d_vocab}': {str(e)}")

                elif isinstance(record, basin3d.core.models.AttributeMapping):
                    try:
                        ds_name = session.query(sqlalchemy_models.DataSource).filter_by(
                            plugin_id=record.datasource.id).one_or_none()
                        if ds_name is None:
                            raise ValueError(f"DataSource with id {record.datasource.plugin_id} not found")

                        record_basin3d_desc = self._convert_basin3d_attr_mapping_basin3d_desc(record.basin3d_desc)

                        p = sqlalchemy_models.AttributeMapping(
                            datasource=ds_name,
                            attr_type=record.attr_type,
                            basin3d_vocab=record.basin3d_vocab,
                            basin3d_desc=record_basin3d_desc,
                            datasource_vocab=record.datasource_vocab,
                            datasource_desc=record.datasource_desc
                        )
                        session.add(p)
                        session.commit()
                        logger.info(f'inserted {record.datasource_vocab} mapping attribute')
                    except IntegrityError:
                        # This object has already been loaded
                        session.rollback()
                        logger.info(f'Warning: skipping AttributeMapping "{record.basin3d_vocab}". Already loaded.')
                    except Exception as e:
                        session.rollback()
                        logger.info(f'Error Registering AttributeMapping "{record.basin3d_vocab}": {str(e)}')
                elif isinstance(record, basin3d.core.models.DataSource):
                    pass  # Do nothing and don't error out
                else:
                    msg = f"Record type {type(record)} not supported for insertion into a {self.__class__.__name__}"
                    logger.critical(msg)
                    raise CatalogException(msg)
            finally:
                session.close()
        else:
            msg = 'Could not insert record. Catalog not initialized.'
            logger.critical(msg)
            raise CatalogException(msg)
