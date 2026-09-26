import pytest

from basin3d.core.models import DataSource, MonitoringFeature
from basin3d.core.plugin import DataSourcePluginAccess, PluginIteratorResult
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.synthesis import (DataSourceModelIterator, MeasurementTimeseriesTVPObservationAccess,
                                     MonitoringFeatureAccess, SynthesisResponse)

from tests.testplugins import alpha


@pytest.fixture
def alpha_plugin_access():
    from basin3d.core.catalog import CatalogSqlAlchemy
    catalog = CatalogSqlAlchemy()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')

    return DataSourcePluginAccess(alpha_ds, catalog)


@pytest.mark.parametrize('input_query, expected_result',
                         [
                          # change-to-day
                          ({'datasource': ['A'], 'monitoring_feature': ['bar', 'base'], 'observed_property':['FOO', 'BAR'],
                            'start_date': '2021-01-01', 'aggregation_duration': 'HOUR'}, ['NOT_SUPPORTED']),
                          # NONE
                          ({'datasource': ['A'], 'monitoring_feature': ['bar', 'base'], 'observed_property':['FOO', 'BAR'],
                            'start_date': '2021-01-01', 'aggregation_duration': 'NONE'}, ['NONE']),
                          # not-specified
                          ({'datasource': ['A'], 'monitoring_feature': ['bar', 'base'], 'observed_property':['FOO', 'BAR'],
                            'start_date': '2021-01-01'}, ['DAY']),
                          ], ids=['HOUR-not-supported', 'NONE', 'not-specified'])
def test_measurement_timeseries_TVP_observation_access_synthesize_query(input_query, expected_result, alpha_plugin_access):
    from basin3d.core.catalog import CatalogSqlAlchemy
    catalog = CatalogSqlAlchemy()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')
    alpha_access = MeasurementTimeseriesTVPObservationAccess(alpha_ds, catalog)

    query = QueryMeasurementTimeseriesTVP(**input_query)

    result = alpha_access.synthesize_query(alpha_plugin_access, query)

    assert result.aggregation_duration == expected_result


def test_monitoring_feature_retrieve_no_id():
    from basin3d.core.catalog import CatalogSqlAlchemy
    catalog = CatalogSqlAlchemy()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')
    # cheating and assigning a datasource to the plugin field as it won't be used
    alpha_access = MonitoringFeatureAccess({'A': alpha_ds}, catalog)

    result = alpha_access.retrieve(query=QueryMonitoringFeature())

    assert isinstance(result, SynthesisResponse)
    assert result.data is None
    assert result.messages[0].msg == 'query.id field is missing and is required for monitoring feature request by id.'


def test_iterator_collects_plugin_citations(alpha_plugin_access):
    query = QueryMonitoringFeature()
    iterator = DataSourceModelIterator(query, MonitoringFeatureAccess({}, alpha_plugin_access._catalog))

    def plugin_iterator():
        return StopIteration(PluginIteratorResult(['plugin warning'], ['https://alpha.alpha/citation']))
        yield  # pragma: no cover

    iterator._model_access_iterator = plugin_iterator()

    with pytest.raises(StopIteration):
        next(iterator)

    assert [message.msg for message in iterator.synthesis_response.messages] == ['plugin warning']
    assert iterator.synthesis_response.citations == ['https://alpha.alpha/citation']


def test_alpha_measurement_region_early_exit_returns_malformed_result(alpha_plugin_access):
    plugin_access = alpha.AlphaMeasurementTimeseriesTVPObservationAccess(
        alpha_plugin_access._datasource, alpha_plugin_access._catalog)
    query = QueryMeasurementTimeseriesTVP(
        monitoring_feature=['region'], observed_property=['ACT'], start_date='2016-02-01')

    plugin_iterator = plugin_access.list(query)
    with pytest.raises(StopIteration) as stop_iteration:
        next(plugin_iterator)

    plugin_result = stop_iteration.value.value
    assert isinstance(plugin_result, StopIteration)
    assert plugin_result.args[0] == {"message": "FOO"}
