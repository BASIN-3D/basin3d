from unittest.mock import Mock

import pytest

import basin3d
from basin3d.core.catalog import CatalogTinyDb
from basin3d.core.models import MonitoringFeature
from basin3d.core.plugin import get_feature_type
from basin3d.core.schema.enum import FeatureTypeEnum


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
