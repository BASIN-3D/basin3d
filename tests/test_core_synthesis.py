import pytest

from basin3d.core.models import DataSource, MonitoringFeature
from basin3d.core.plugin import DataSourcePluginAccess
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.synthesis import MeasurementTimeseriesTVPObservationAccess, MonitoringFeatureAccess, SynthesisResponse

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


def test_monitoring_feature_retrieve_no_id():
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')
    # cheating and assigning a datasource to the plugin field as it won't be used
    alpha_access = MonitoringFeatureAccess({'A': alpha_ds}, catalog)

    result = alpha_access.retrieve(query=QueryMonitoringFeature())

    assert isinstance(result, SynthesisResponse)
    assert result.data is None
    assert result.messages[0].msg == 'query.id field is missing and is required for monitoring feature request by id.'
