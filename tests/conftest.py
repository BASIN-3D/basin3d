import pytest


def pytest_addoption(parser):
    """
    Add execution options to the commandline
    :param parser:
    :return:
    """

    parser.addoption(
        "--runintegration", action="store_true", default=False, help=f"run integration tests"
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

    # Determine if any markers need to be skipped.
    if config.getoption("--runintegration"):
        # --runintegration given in cli: do not skip integration tests
        return

    markers_skip_integration = pytest.mark.skip(reason="need --runintegration option to run")

    for item in items:
        if "integration" in item.keywords:
            item.add_marker(markers_skip_integration)


@pytest.fixture
def datasource(name='Alpha', location='https://asource.foo/', id_prefix='A'):
    """
    Create a DataSource object
    """
    from basin3d.core.models import DataSource
    return DataSource(id=name, name=name, id_prefix=id_prefix, location=location)


@pytest.fixture
def plugin_access_alpha():
    """
    Create a DataSourcePluginAccess object
    """
    from basin3d.core.models import MeasurementTimeseriesTVPObservation

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
    from basin3d.plugins.usgs import USGSMeasurementTimeseriesTVPObservationAccess

    from basin3d.plugins import usgs
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    plugins = [usgs.USGSDataSourcePlugin(catalog)]
    catalog.initialize(plugins)
    return USGSMeasurementTimeseriesTVPObservationAccess(plugins[0].datasource, catalog)
