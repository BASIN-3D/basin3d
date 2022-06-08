from unittest.mock import Mock

import pytest

import basin3d
from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import MonitoringFeature
from basin3d.core.plugin import get_feature_type
from basin3d.core.schema.enum import FeatureTypeEnum
from basin3d.core.synthesis import DataSourceModelIterator
from basin3d.synthesis import register


def test_plugin_metadata():
    """Import the custom plugin and check the metadata"""
    from tests.testplugins import alpha
    assert alpha.AlphaSourcePlugin.get_meta().id == 'Alpha'
    assert alpha.AlphaSourcePlugin.get_id_prefix() == 'A'
    assert alpha.AlphaSourcePlugin.get_meta().location == 'https://asource.foo/'


def test_plugin_attribs():
    """Import the plugin and check the configuration information"""
    from tests.testplugins import alpha
    assert alpha.AlphaSourcePlugin.get_feature_types() == ['REGION', 'POINT', 'TREE']


def test_plugin_access_objects():
    """Test the plugin and check the access objects available"""
    from tests.testplugins import alpha
    assert isinstance(alpha.AlphaSourcePlugin(CatalogTinyDb()).get_plugin_access(), dict)
    plugin_access_objects = alpha.AlphaSourcePlugin(CatalogTinyDb()).get_plugin_access()
    assert list(plugin_access_objects.keys()) == [
        basin3d.core.models.MeasurementTimeseriesTVPObservation,
        basin3d.core.models.MonitoringFeature]

    assert plugin_access_objects[
               basin3d.core.models.MonitoringFeature].__class__.__name__ == 'tests.testplugins.alpha.AlphaMonitoringFeatureAccess'
    assert plugin_access_objects[basin3d.core.models.MeasurementTimeseriesTVPObservation].__class__.__name__ == \
           'tests.testplugins.alpha.AlphaMeasurementTimeseriesTVPObservationAccess'


def test_plugin_incomplete():
    """Import the incomplete plugin and should throw ValueError"""

    with pytest.raises(ValueError):
        from tests.testplugins import empty
        empty.IncompleteSourcePlugin.get_meta()


def test_plugin_incomplete_meta():
    """Import the incomplete plugin and try to get the id_prefix ValueError"""
    with pytest.raises(AttributeError):
        from tests.testplugins import incomplete_meta
        incomplete_meta.IncompleteMetaSourcePlugin.get_id_prefix()


@pytest.mark.xfail()
def test_plugin_direct(monkeypatch):
    """Import the alpha plugin and try to call the api directly"""
    from tests.testplugins import alpha

    monkeypatch.setattr(alpha.AlphaSourcePlugin, 'get_datasource', Mock())
    assert alpha.AlphaSourcePlugin().direct("path") is None


def test_plugin_views(monkeypatch):
    """Import the alpha plugin and try to call the api directly"""
    from tests.testplugins import alpha

    monkeypatch.setattr(alpha.AlphaSourcePlugin, 'get_datasource', Mock())
    plugin_views = alpha.AlphaSourcePlugin(CatalogTinyDb()).get_plugin_access()
    assert isinstance(plugin_views, dict)
    assert basin3d.core.models.MeasurementTimeseriesTVPObservation in plugin_views
    assert basin3d.core.models.MonitoringFeature in plugin_views


@pytest.mark.parametrize("feature_name, return_format, result", [("FOO", None, None),
                                                                 ("REGION", "enum", FeatureTypeEnum.REGION),
                                                                 ("REGION", None, "REGION")],
                         ids=["nonexistent", "return_enum", "return_string"])
def test_get_feature_type(feature_name, return_format, result):
    """Test get the feature type"""

    feature_type = get_feature_type(feature_name, return_format)
    assert result == feature_type


@pytest.mark.parametrize("plugin, messages, synthesis_call, synthesis_args", [
    # ErrorSource.MF
    ("tests.testplugins.plugin_error.ErrorSourcePlugin",
     [{'msg': 'Unexpected Error(Exception): This is a list_monitoring_features exception', 'level': 'ERROR', 'where': ['Error', 'MonitoringFeature']}],
     "monitoring_features",
     {}),
    # ErrorSource.TVP
    ("tests.testplugins.plugin_error.ErrorSourcePlugin",
     [{'level': 'ERROR', 'msg': 'Unexpected Error(Exception): This is a find_measurement_timeseries_tvp_observations error', 'where': ['Error', 'MeasurementTimeseriesTVPObservation']}],
     "measurement_timeseries_tvp_observations",
     {'start_date': '2019-10-01', 'observed_property_variables': ["Ag"], 'monitoring_features': ['E-region']}),
    # AlphaSource.MF
    ("tests.testplugins.alpha.AlphaSourcePlugin",
     [{'level': 'ERROR', 'msg': 'DataSource not not found for id E-1234', 'where': None}],
     "monitoring_features",
     {"id": 'E-1234'}),
    # USGS.MR

    ("basin3d.plugins.usgs.USGSDataSourcePlugin",
     [{'level': 'ERROR', 'msg': 'DataSource not not found for id E-1234', 'where': None}],
     "monitoring_features",
     {"id": 'E-1234'}),
    # NoPluginViews.TVP
    ("tests.testplugins.no_plugin_views.NoPluginViewsPlugin",
     [{'level': 'WARN', 'msg': 'Plugin view does not exist', 'where': ['NoPluginView', 'MeasurementTimeseriesTVPObservation']}],
     "measurement_timeseries_tvp_observations",
     {'start_date': '2019-10-01', 'observed_property_variables': ["Ca"], 'monitoring_features': ['NPV-region']}),
    # NoPluginViews.MF
    ("tests.testplugins.no_plugin_views.NoPluginViewsPlugin",
     [{'level': 'WARN', 'msg': 'Plugin view does not exist', 'where': ['NoPluginView', 'MonitoringFeature']}],
     "monitoring_features",
     {'id': 'NPV-01'}),
    # AlphaSource.TVP
    ("tests.testplugins.alpha.AlphaSourcePlugin",
     [{'level': 'WARN', 'msg': 'Synthesis generated warnings but they are in the wrong format', 'where': ['Alpha', 'MeasurementTimeseriesTVPObservation']}],
     "measurement_timeseries_tvp_observations",
     {'start_date': '2019-10-01', 'observed_property_variables': ["Ag"], 'monitoring_features': ['A-region']}),
    ],
    ids=["ErrorSource.MF", "ErrorSource.TVP", "AlphaSource.MF", "USGS.MR", "NoPluginViews.TVP", "NoPluginViews.MF", "AlphaSource.TVP"])
def test_plugin_exceptions(plugin, messages, synthesis_call, synthesis_args):
    """Test that basin3d handles unexpected exceptions"""

    synthesizer = register([plugin])
    result = getattr(synthesizer, synthesis_call)(**synthesis_args)

    if isinstance(result, DataSourceModelIterator):
        for _ in result:
            pass
        synthesis_response = result.synthesis_response
    else:
        synthesis_response = result

    syn_response = synthesis_response.dict()
    assert syn_response["messages"] == messages
