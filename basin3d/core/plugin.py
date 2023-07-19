"""

.. currentmodule:: basin3d.core.plugin

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` plugin classes
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top


"""
from types import MethodType
from typing import Dict

from basin3d.core import monitor
from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import DataSource
from basin3d.core.schema.enum import FeatureTypeEnum

logger = monitor.get_logger(__name__)


def get_feature_type(feature_type, return_format="enum"):
    """
    Return the feature type if exists in the request
    :param feature_type:
    :param return_format: "enum" (default) = the FeatureTypeEnum enum,
    otherwise return the text version
    :return: the feature_type in the format specified, None if none exists
    """
    if feature_type:
        for k, v in FeatureTypeEnum.__members__.items():
            ft = v.lower()
            if ft == feature_type.lower():
                if return_format == "enum":
                    return k
                else:
                    return v
    return None


class DataSourcePluginAccess:
    """
    Metaclass for DataSource plugin views.  The should be registered in a subclass of
    :class:`basin3d.plugins.DataSourcePluginPoint` in attribute `plugin_access_classes`.
    """
    def __init__(self, datasource: DataSource, catalog: CatalogTinyDb):
        """

        :param datasource: The data source object
        :param catalog: The Plugin Catalog
        """
        self._datasource = datasource
        self._catalog = catalog

    @property
    def datasource(self):
        return self._datasource

    def get_datasource_attribute_mapping(self, attr_type, attr_vocab):
        """
        Get attribute mapping for the specified attribute type and datasource attribute vocab

        :param attr_type: attribute type
        :param attr_vocab: datasource attribute vocabulary
        :return: a `basin3d.models.AttributeMapping` object
        """
        return self._catalog.find_datasource_attribute_mapping(self.datasource.id, attr_type, attr_vocab)

    def get_attribute_mappings(self, attr_type=None, attr_vocab=None, from_basin3d=False):
        """
        General purpose search for attribute mappings from datasource or BASIN-3D vocabularies.
        Returns an iterator that yields `basin3d.models.MappedAttribute` objects that match the criteria
        :param attr_type:
        :param attr_vocab:
        :param from_basin3d:
        :return:
        """
        return self._catalog.find_attribute_mappings(self.datasource.id, attr_type, attr_vocab, from_basin3d)


def basin3d_plugin(cls):
    """Register a BASIN-3D plugin"""

    # TODO add any validation here for class structure
    PluginMount.plugins[cls.get_meta().id] = cls
    return cls


def _basin3d_plugin_access(plugin_class, synthesis_model_class, access_type):
    """Decorator for registering model access"""

    def _inner(func):

        plugin_access_classes = getattr(plugin_class, 'plugin_access_classes', None)
        if not plugin_access_classes:
            setattr(plugin_class, 'plugin_access_classes', [])
            plugin_access_classes = getattr(plugin_class, 'plugin_access_classes')

        class_name = f"{plugin_class.__module__}.{plugin_class.get_meta().id}{synthesis_model_class.__name__}Access"

        new_class = None
        for plugin_access in plugin_access_classes:
            if plugin_access.__name__ in class_name:
                new_class = plugin_access

        if new_class:
            setattr(new_class, access_type, MethodType(func, new_class))
        else:
            new_class = type(class_name, (DataSourcePluginAccess,),
                             {"synthesis_model_class": synthesis_model_class, access_type: func})
            plugin_access_classes.append(new_class)

        return func

    return _inner


class DataSourcePluginPoint:
    """
    Base class for DataSourcePlugins.
    """

    def __init__(self, catalog: CatalogTinyDb):
        """
        Instantiate the plugin with the catalog
        :param catalog:
        """

        meta = self.get_meta()
        datasource_id = getattr(meta, 'id')
        self.datasource = DataSource(id=datasource_id,
                                     id_prefix=self.get_id_prefix(),
                                     name=self.get_name(),
                                     location=self.get_location(),
                                     credentials={})

        self.access_classes = {}
        plugin_access_classes = getattr(self, 'plugin_access_classes', None)
        if plugin_access_classes:
            for access_class in plugin_access_classes:
                access_point = access_class(self.get_datasource(), catalog)
                self.access_classes[access_point.synthesis_model_class] = access_point

    @classmethod
    def get_meta(cls):
        """
        Gets the DataSourceMeta internal class that should be defined by subclasses.
        Raises an error if it is not found
        :return:
        """
        meta = getattr(cls, 'DataSourceMeta', None)

        if not meta:
            raise ValueError("Must define inner class DatasSourceMeta for {}".format(cls))
        return meta

    def get_datasource(self):
        """
        Get the `basin3d.models.DataSource` record for the subclass of this Plugin.
        # :return:
        """
        return self.datasource

    def get_plugin_access(self):
        """
        Get the defined plugin_access_classes from the subclass.  These should be defined in
        `DataSourceMeta.plugin_view_subclass`. If not, an error is thrown
        :return:
        """
        return self.access_classes

    @classmethod
    def get_id_prefix(cls):
        """
        Get the defined id prefix
        """
        meta = cls.get_meta()
        return getattr(meta, 'id_prefix')

    @classmethod
    def get_name(cls):
        """
        Get the defined id prefix
        """
        meta = cls.get_meta()
        return getattr(meta, 'name')

    @classmethod
    def get_location(cls):
        """
        Get the defined id prefix
        """
        meta = cls.get_meta()
        return getattr(meta, 'location')

    @classmethod
    def get_feature_types(cls):
        """
        Get the defined feature types
        :return:
        """
        return getattr(cls, 'feature_types', None)


class PluginMount(type):
    """
    The idea for the Simple Plugin Framework comes from a post
    by Marty Alchin on January 10, 2008 about Django

    See: http://martyalchin.com/2008/jan/10/simple-plugin-framework/

    it is under CC-BY-SA 3.0 US License
    (https://creativecommons.org/licenses/by-sa/3.0/us/)

    Plugin classes that extend this will register themselves as
    soon as they are loaded

    """
    plugins: Dict[DataSourcePluginPoint, str] = {}
