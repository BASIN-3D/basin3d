import pytest

from basin3d.core.models import DataSource
from basin3d.core.plugin import DataSourcePluginAccess
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP
from basin3d.core.synthesis import MeasurementTimeseriesTVPObservationAccess

from tests.testplugins import alpha


@pytest.fixture
def alpha_plugin_access():
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')

    return DataSourcePluginAccess(alpha_ds, catalog)


@pytest.mark.parametrize('input_query, expected_result',
                         [
                          # change-to-day
                          ({'datasource': ['A'], 'monitoring_feature': ['bar', 'base'], 'observed_property':['FOO', 'BAR'],
                            'start_date': '2021-01-01', 'aggregation_duration': 'MONTH'}, ['DAY']),
                          # NONE
                          ({'datasource': ['A'], 'monitoring_feature': ['bar', 'base'], 'observed_property':['FOO', 'BAR'],
                            'start_date': '2021-01-01', 'aggregation_duration': 'NONE'}, ['NONE']),
                          # not-specified
                          ({'datasource': ['A'], 'monitoring_feature': ['bar', 'base'], 'observed_property':['FOO', 'BAR'],
                            'start_date': '2021-01-01'}, ['DAY']),
                          ], ids=['change-to-day', 'NONE', 'not-specified'])
def test_measurement_timeseries_TVP_observation_access_synthesize_query(input_query, expected_result, alpha_plugin_access):
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')
    alpha_access = MeasurementTimeseriesTVPObservationAccess(alpha_ds, catalog)

    query = QueryMeasurementTimeseriesTVP(**input_query)

    result = alpha_access.synthesize_query(alpha_plugin_access, query)

    assert result.aggregation_duration == expected_result
