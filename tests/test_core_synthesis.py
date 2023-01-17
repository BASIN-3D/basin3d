import pytest

from basin3d.core.models import DataSource
from basin3d.core.plugin import DataSourcePluginAccess

from tests.testplugins import alpha


@pytest.fixture
def alpha_plugin_access():
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')

    return DataSourcePluginAccess(alpha_ds, catalog)
