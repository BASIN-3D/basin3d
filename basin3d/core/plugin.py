"""
`basin3d.core.plugin`
*********************

.. currentmodule:: basin3d.core.plugin

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` plugin classes
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top


"""
import logging
from types import MethodType
from typing import Dict

from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import DataSource
from basin3d.core.schema.enum import FeatureTypeEnum

logger = logging.getLogger(__name__)


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

    def get_observed_property(self, variable_name: str):
        """
        Convert the given name to either BASIN-3D from :class:`~basin3d.models.DataSource`
        variable name or the other way around.

        :param variable_name:  The :class:`~basin3d.models.ObservedPropertyVariable`
             name to convert
        :return: An observed property variable name
        :rtype: str
        """

        return self._catalog.find_observed_property(self.datasource.id, variable_name)

    def get_observed_properties(self, variable_names):
        """
        Convert the given name to either BASIN-3D from :class:`~basin3d.models.DataSource`
        variable name or the other way around.

        :param variable_names:  The :class:`~basin3d.models.ObservedPropertyVariable`
             name to convert
        :return: An observerd proeprty variable name
        :rtype: str
        """

        return self._catalog.find_observed_properties(self.datasource.id, variable_names)

    def get_observed_property_variable(self, variable_name, from_basin3d=False):
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

        return self._catalog.find_observed_property_variable(self.datasource.id, variable_name, from_basin3d)

    def get_observed_property_variables(self, variable_names=None, from_basin3d=False):
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
        return self._catalog.find_observed_property_variables(self.datasource.id, variable_names, from_basin3d)


def basin3d_plugin(cls):
    """Register a BASIN-3D plugin"""

    # TODO add any validation here for class structure
    PluginMount.plugins[cls.get_meta().id] = cls
    return cls


def basin3d_plugin_access(plugin_class, synthesis_model_class, access_type):
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

