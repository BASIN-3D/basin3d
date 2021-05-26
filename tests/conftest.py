import pytest

from basin3d.core.models import DataSource, MeasurementTimeseriesTVPObservation
from basin3d.plugins.usgs import USGSMeasurementTimeseriesTVPObservationAccess

def pytest_addoption(parser):
    """
    Add execution options to the commandline
    :param parser:
    :return:
    """

    for marker in ["integration"]:
        parser.addoption(
            f"--run{marker}", action="store_true", default=False, help=f"run {marker} tests"
        )


def pytest_configure(config):
    """
    Add pytext init lines
    :param config:
    :return:
    """
    config.addinivalue_line("markers", "integration: Mark test as integration.")

def pytest_collection_modifyitems(config, items):
    """
    Modify the tests to skip integration unless specified
    :param config:
    :param items:
    :return:
    """
    markers_skip = {}

    # Determine if any markers need to be skipped.
    for marker in ["integration"]:
        if not config.getoption(f"--run{marker}"):
            markers_skip[marker] = pytest.mark.skip(reason=f"need --run{marker} option to run")

    # filter out the marked tests by default
    if len(markers_skip):
        for item in items:
            for marker, skip in markers_skip.items():
                if marker in item.keywords:
                    item.add_marker(skip)


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
