import pytest

from basin3d.core.models import DataSource, MeasurementTimeseriesTVPObservation
from basin3d.plugins.usgs import USGSMeasurementTimeseriesTVPObservationAccess


@pytest.fixture
def datasource(name='Alpha', location='https://asource.foo/', id_prefix='A'):
    """
    Create a DataSource object
    """
    return DataSource(id=name, name=name, id_prefix=id_prefix, location=location)


@pytest.fixture
def plugin_access_alpha():
    """
    Create a DataSourcePluginAccess object
    """
    from tests.testplugins import alpha
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    plugins = [alpha.AlphaSourcePlugin(catalog)]
    catalog.initialize(plugins)
    return plugins[0].access_classes[MeasurementTimeseriesTVPObservation]


@pytest.fixture
def plugin_access():
    """
    Create a DataSourcePluginAccess object
    """
    from basin3d.plugins import usgs
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    plugins = [usgs.USGSDataSourcePlugin(catalog)]
    catalog.initialize(plugins)
    return USGSMeasurementTimeseriesTVPObservationAccess(plugins[0].datasource, catalog)
