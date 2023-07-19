"""

.. currentmodule:: basin3d.core.translate

:synopsis: The BASIN-3D Translate functionality
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""

from itertools import product, repeat
from typing import List, Optional, Union

from basin3d.core import monitor
from basin3d.core.schema.enum import MAPPING_DELIMITER, NO_MAPPING_TEXT
from basin3d.core.schema.query import QueryBase, QueryMeasurementTimeseriesTVP, QueryMonitoringFeature


logger = monitor.get_logger(__name__)


def _clean_query(translated_query: QueryBase) -> QueryBase:
    """
    Remove any NOT_SUPPORTED translations

    :param translated_query: the translated query that may have NOT_SUPPORTED translation results
    :return: cleaned query that has all NOT_SUPPORTED translations removed
    """
    for attr in translated_query.mapped_fields:
        attr_value = getattr(translated_query, attr)
        if attr_value and isinstance(attr_value, list):
            clean_list = [val for val in attr_value if val != NO_MAPPING_TEXT]
            unique_list = list(set(clean_list))
            setattr(translated_query, attr, unique_list)
        elif attr_value and attr_value == NO_MAPPING_TEXT:
            setattr(translated_query, attr, None)
    return translated_query


def _get_attr_types_in_compound_mappings(plugin_access) -> list:
    """
    Get all the compound mappings for a datasource if any exist

    :param plugin_access: plugin access
    :return: list of attribute types that are part of a compound mapping.
    """
    compound_mapping_attrs = set()

    attr_mapping_iterator = plugin_access.get_attribute_mappings()

    for attr_mapping in attr_mapping_iterator:
        if MAPPING_DELIMITER in attr_mapping.attr_type:
            compound_mapping_attrs.add(attr_mapping.attr_type)

    return list(compound_mapping_attrs)


def _get_attr_type_if_compound_mapping(plugin_access, attr_type: str) -> Optional[str]:
    """
    Return the compound attr_type str if the specified attr_type is part of a compound_mapping

    :param plugin_access:
    :param attr_type:
    :return: str if attr mapping is part of compound mapping
    """
    compound_mapping_str = None
    attr_type = attr_type.upper()
    attr_mapping_iterator = plugin_access.get_attribute_mappings(attr_type=attr_type)

    # look at the first element returned if there is one
    for attr_mapping in attr_mapping_iterator:
        if MAPPING_DELIMITER in attr_mapping.attr_type:
            compound_mapping_str = attr_mapping.attr_type
        # only need to look at the first attribute mapping returned
        break

    return compound_mapping_str


def _get_single_attr_types_in_compound_mappings(plugin_access, attr_type: str, include_specified_type: bool = False) -> list:
    """
    Return the attributes if attr_type is part of a compound mapping

    :param attr_type: the attribute type
    :param include_specified_type: bool, True = include in the return the specified attr_type. False: return the other attribute types that are part of the compound mapping.
    :return: list of attributes in the compound mapping
    """

    compound_mapping_attrs: List[str] = []

    compound_mapping_str = _get_attr_type_if_compound_mapping(plugin_access, attr_type)

    if not compound_mapping_str:
        return compound_mapping_attrs

    for attr in compound_mapping_str.split(MAPPING_DELIMITER):
        if attr == attr_type.upper() and not include_specified_type:
            continue
        compound_mapping_attrs.append(attr)

    return compound_mapping_attrs


def _is_translated_query_valid(datasource_id, query, translated_query) -> Optional[bool]:
    """
    Determine if the translated query is valid. A valid translated query has at least one datasoure vocabulary
    for any query field that was supecified (i.e., with a BASIN-3D vocab)

    :param datasource_id: the datasource id
    :param query: the original query
    :param translated_query: the translated query
    :return: boolean (True = valid translated query, False = invalid translated query) or None (translated query could not be assessed)
    """
    for field_type, field_list in zip(['mapped', 'prefixed'], [query.mapped_fields, query.prefixed_fields]):
        # loop thru kwargs
        for attr in field_list:
            translated_attr_value = getattr(translated_query, attr)
            b3d_attr_value = getattr(query, attr)
            msg_prefix = ''
            if field_type == 'mapped':
                msg_prefix = 'No vocabulary found for attribute {attr} with values: {b3d_attr_value}.'
            if isinstance(b3d_attr_value, list):
                b3d_attr_value = ', '.join(b3d_attr_value)
            if translated_attr_value and isinstance(translated_attr_value, list):
                # if list and all of list == NOT_SUPPORTED, False
                if all([x == NO_MAPPING_TEXT for x in translated_attr_value]):
                    logger.warning(f'Translated query for datasource {datasource_id} is invalid.{msg_prefix}')
                    return False
            elif translated_attr_value and isinstance(translated_attr_value, str):
                # if single NOT_SUPPORTED, False
                if translated_attr_value == NO_MAPPING_TEXT:
                    logger.warning(f'Translated query for datasource {datasource_id} is invalid.{msg_prefix}')
                    return False
            elif translated_attr_value:
                logger.warning(
                    f'Translated query for datasource {datasource_id} cannot be assessed. Translated value for {attr} is not expected type.')
                return None
    return True


def _order_mapped_fields(plugin_access, query_mapped_fields):
    """
    Order the mapped fields according to the order of the attributes in any compound mappings. And then handle the rest of the mappings.

    :param plugin_access:
    :param query_mapped_fields:
    :return:
    """
    query_mapped_fields_ordered = []

    # get list of compound mappings if any
    compound_mappings = _get_attr_types_in_compound_mappings(plugin_access)

    # If there are compound mappings...
    if compound_mappings:
        cm_fields = []
        # Split up the compound mappings, preserving the order of the attributes as specified in the plugin mapping file.
        # The order only matters relative to the individual compound mapping.
        for cm in compound_mappings:
            cm_attrs = cm.split(MAPPING_DELIMITER)
            cm_fields.extend([cm_attr.lower() for cm_attr in cm_attrs])
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


def _translate_mapped_query_attrs(plugin_access, query: Union[QueryMeasurementTimeseriesTVP, QueryMonitoringFeature]) -> QueryBase:
    """
    Translation functionality
    """
    query_mapped_fields = query.mapped_fields.copy()

    # if there are no mapped fields, return the query as is.
    if not query_mapped_fields:
        return query

    # order the query fields by any compound attributes
    query_mapped_fields_ordered = _order_mapped_fields(plugin_access, query_mapped_fields)

    for attr in query_mapped_fields_ordered:
        # if the attribute is specified, proceed to translate it
        # NOTE: looking in the translated_query which is mutable. As the translation occurs, translated query fields may change
        #       and the if statement may have different values for a given field during the loop.
        if getattr(query, attr):
            b3d_vocab = getattr(query, attr)

            if isinstance(b3d_vocab, str):
                ds_vocab = _translate_to_datasource_vocab(plugin_access, attr.upper(), b3d_vocab, query)
            else:
                ds_vocab = []
                for b3d_value in b3d_vocab:
                    # handle multiple values returned
                    ds_vocab.extend(_translate_to_datasource_vocab(plugin_access, attr.upper(), b3d_value, query))
            setattr(query, attr, ds_vocab)

            # look up whether the attr is part of a compound mapping
            compound_attrs = _get_single_attr_types_in_compound_mappings(plugin_access, attr)
            # if so: for any compound attrs, clear out the values in the synthesized query b/c search needs to be done on the coupled datasource_vocab
            for compound_attr in compound_attrs:
                setattr(query, compound_attr.lower(), None)

    # NOTE: always returns list for each mapped attr b/c multiple datasource vocab can be mapped to a single BASIN-3D vocab.
    return query


def _translate_prefixed_query_attrs(plugin_access, query: Union[QueryMeasurementTimeseriesTVP, QueryMonitoringFeature]) -> QueryBase:
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

    for attr in query.prefixed_fields:
        attr_value = getattr(query, attr)
        if attr_value:

            # if the value is a string
            if isinstance(attr_value, str):
                translated_value = extract_id(attr_value)
                if translated_value == attr_value:
                    translated_value = NO_MAPPING_TEXT

            # otherwise assume it is a list
            else:
                translated_value = [extract_id(x) for x in attr_value if x.startswith("{}-".format(id_prefix))]

            setattr(query, attr, translated_value)

    return query


def _translate_to_datasource_vocab(plugin_access, attr_type: str, basin3d_vocab: str, b3d_query) -> list:
    """
    Find the datasource vocabulary(ies) for the specified datasource, attribute type, BASIN-3D vocabulary, and full query that may specify other attributes.
    Because multiple datasource vocabularies can be mapped to the same BASIN-3D vocabulary, the return is a list of the datasource vocabs.

    :param plugin_access: plugin access
    :param attr_type: the attribute type
    :param basin3d_vocab: the BASIN-3D vocabulary
    :param b3d_query: either a QueryBase class or subclass object, or a dictionary
    :return: list of the datasource vocabularies
    """
    # convert attr_type to uppercase
    attr_type = attr_type.upper()

    # is the attr_type part of a compound mapping?
    compound_mapping_attrs = _get_single_attr_types_in_compound_mappings(plugin_access, attr_type, include_specified_type=True)

    b3d_vocab_combo_str = [basin3d_vocab]

    # if compound_mapping, find all the relevant value combos given the query
    if compound_mapping_attrs:
        b3d_vocab_filter_lists = []  # list to hold lists of specified filters, one for each attr

        # loop thru each of the compound mapping attributes, build a list of lists of query combos
        for attr in compound_mapping_attrs:
            attr_value = None

            # by default: match any number of characters excepting a new line for the attribute
            # replace this value below if a value for the attribute is specified in the query
            filter_values: List[str] = ['.*']
            # if the attr is the attr_type, set the filter to the specified vocab
            if attr == attr_type:
                filter_values = [basin3d_vocab]
            elif issubclass(b3d_query.__class__, QueryBase) and hasattr(b3d_query, attr.lower()):
                attr_value = getattr(b3d_query, attr.lower())
            elif isinstance(b3d_query, dict) and attr.lower() in b3d_query.keys():
                attr_value = b3d_query.get(attr.lower())

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

    ds_vocab: List[str] = []
    no_match_list: List[str] = []

    # Loop thru the list of vocabulary string combos to search the attribute mapping database
    for basin3d_vocab_str in b3d_vocab_combo_str:
        results_iterator = plugin_access.get_attribute_mappings(attr_type=attr_type,
                                                                attr_vocab=basin3d_vocab_str, from_basin3d=True)
        query_results = []

        # there may be more than one datasource variable mapped to the specified BASIN-3D vocab
        # Loop thru the results and collect all of them.
        for qr in results_iterator:
            query_results.append(qr.datasource_vocab)

        # If there were indeed mappings, add them to the main ds_vocab list and move on
        if query_results:
            ds_vocab.extend(query_results)
            continue

        # if not result for the combo, add it to the no_match_list
        no_match_list.append(basin3d_vocab_str)

    if not ds_vocab:
        ds_vocab = [NO_MAPPING_TEXT]

    if no_match_list:
        if compound_mapping_attrs:
            attr_type = ':'.join(compound_mapping_attrs)
        logger.info(f'Datasource "{plugin_access.datasource.id}" did not have matches for attr_type '
                    f'{attr_type} and BASIN-3D vocab {", ".join(no_match_list)}.')

    return ds_vocab


def get_datasource_mapped_attribute(plugin_access, attr_type, datasource_vocab):
    """
    Get the :class:`basin3d.core.models.MappedAttribute` object(s) for the specified attribute type and datasource attribute vocab(s)

    :param plugin_access: plugin_access
    :param attr_type: attribute type
    :param datasource_vocab: datasource attribute vocabulary
    :return: a single or list of `basin3d.core.models.MappedAttribute` objects
    """

    if isinstance(datasource_vocab, str):
        return plugin_access.get_datasource_attribute_mapping(attr_type, datasource_vocab)

    elif isinstance(datasource_vocab, list):
        return list(map(plugin_access.get_datasource_attribute_mapping, repeat(attr_type), datasource_vocab))


def translate_attributes(plugin_access, mapped_attrs, **kwargs):
    """
    Translate datasource vocabularies to BASIN-3D vocabularies via
    :class:`basin3d.core.models.AttributeMapping` objects specified in the datasource plugin
    for specified model attributes (i.e., those that have mappings).
    This function is used by data model objects during object creation. See :class:`basin3d.core.models`.

    :param plugin_access: plugin_access
    :param mapped_attrs: a model's mapped attributes
    :param kwargs: the full set of a model's attributes
    :return: kwargs: the model's attributes, including the translated attributes
    """

    # copy the kwargs be able to loop thru the original while modifying the actual for compound mappings
    kwargs_orig = kwargs.copy()

    for attr in mapped_attrs:
        if attr in kwargs_orig:
            datasource_vocab = kwargs[attr]
            attr_mapping = get_datasource_mapped_attribute(plugin_access, attr_type=attr.upper(), datasource_vocab=datasource_vocab)
            kwargs[attr] = attr_mapping

            # If the attr is part of a compound mapping and the compound attr is not part of the kwargs, set it.
            cm_attrs = _get_single_attr_types_in_compound_mappings(plugin_access, attr)
            if cm_attrs:
                for cm_attr in cm_attrs:
                    if cm_attr.lower() not in kwargs:
                        cm_attr_mapping = get_datasource_mapped_attribute(plugin_access, attr_type=cm_attr.upper(), datasource_vocab=datasource_vocab)
                        kwargs[cm_attr.lower()] = cm_attr_mapping

    return kwargs


def translate_query(plugin_access, query: Union[QueryMeasurementTimeseriesTVP, QueryMonitoringFeature]) -> QueryBase:
    """
    Translate BASIN-3D vocabulary specified in a query to the datasource vocabularies defined by :class:`basin3d.core.models.AttributeMapping` objects specified in the datasource plugin.

    :param plugin_access: plugin access
    :param query: query to be translated
    :return: translated query as a :class:`basin3d.core.schema.query.QueryBase` object
    """
    translated_query = query.copy()
    _translate_mapped_query_attrs(plugin_access, translated_query)
    _translate_prefixed_query_attrs(plugin_access, translated_query)
    is_valid_translated_query = _is_translated_query_valid(plugin_access.datasource.id, query, translated_query)

    if is_valid_translated_query:
        translated_query.is_valid_translated_query = is_valid_translated_query
        _clean_query(translated_query)

    return translated_query
