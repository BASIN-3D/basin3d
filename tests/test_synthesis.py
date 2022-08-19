import datetime

import pytest

import basin3d
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.synthesis import DataSourceModelIterator
from basin3d.synthesis import register, SynthesisException


def test_register_error(monkeypatch):
    """Test when there are no plugins"""

    monkeypatch.setattr(basin3d.synthesis.PluginMount, "plugins", {})

    from basin3d.synthesis import register
    pytest.raises(SynthesisException, register)


def test_register_implicit(monkeypatch):
    """Test default registration"""

    from basin3d.plugins import usgs
    monkeypatch.setattr(basin3d.synthesis.PluginMount, "plugins", {
        usgs.USGSDataSourcePlugin.get_meta().id: usgs.USGSDataSourcePlugin
    })

    from basin3d.synthesis import register
    synthesizer = register()

    datasources = synthesizer.datasources
    assert len(datasources) == 1

    assert datasources[0].id_prefix == usgs.USGSDataSourcePlugin.get_id_prefix()
    assert datasources[0].id == 'USGS'
    assert datasources[0].location == 'https://waterservices.usgs.gov/nwis/'



def test_register():
    """Test basic plugin registration"""

    from basin3d.synthesis import register
    synthesizer = register(["basin3d.plugins.usgs.USGSDataSourcePlugin",
                            "tests.testplugins.alpha.AlphaSourcePlugin"])

    datasources = synthesizer.datasources
    assert len(datasources) == 2

    from basin3d.plugins import usgs
    from tests.testplugins import alpha
    assert datasources[0].id_prefix == usgs.USGSDataSourcePlugin.get_id_prefix()
    assert datasources[0].id == 'USGS'
    assert datasources[0].location == 'https://waterservices.usgs.gov/nwis/'

    assert datasources[1].id_prefix == alpha.AlphaSourcePlugin.get_id_prefix()
    assert datasources[1].id == 'Alpha'
    assert datasources[1].location == 'https://asource.foo/'


@pytest.mark.parametrize("query", [{"id": "A-123"}, {"id": "A-123", "feature_type": "region"}],
                         ids=['not-found', 'too-many-for-id'])
def test_monitoring_feature_not_found(query):
    """Test not found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    pytest.raises(Exception, synthesizer.monitoring_features, **query)


def test_monitoring_features_found():
    """Test  found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    monitoring_featurues = synthesizer.monitoring_features()
    if isinstance(monitoring_featurues, DataSourceModelIterator):
        count = 0
        assert monitoring_featurues.synthesis_response is not None
        assert monitoring_featurues.synthesis_response.dict() == {'data': None,
                                                                  'messages': [],
                                                                  'query': {'datasource': None,
                                                                            'feature_type': None,
                                                                            'monitoring_features': None,
                                                                            'parent_features': None}}
        assert isinstance(monitoring_featurues.synthesis_response.query, QueryMonitoringFeature)

        for mf in monitoring_featurues:
            count += 1

        assert monitoring_featurues.synthesis_response is not None
        assert monitoring_featurues.synthesis_response.dict() == {'data': None,
                                                                  'messages': [{'level': 'WARN',
                                                                                'msg': 'message1',
                                                                                'where': ['Alpha',
                                                                                          'MonitoringFeature']},
                                                                               {'level': 'WARN',
                                                                                'msg': 'message2',
                                                                                'where': ['Alpha',
                                                                                          'MonitoringFeature']},
                                                                               {'level': 'WARN',
                                                                                'msg': 'message3',
                                                                                'where': ['Alpha',
                                                                                          'MonitoringFeature']}],
                                                                  'query': {'datasource': None,
                                                                            'feature_type': None,
                                                                            'monitoring_features': None,
                                                                            'parent_features': None}}

        assert count == 2
    else:
        assert monitoring_featurues is not None


@pytest.mark.parametrize("query", [{"id": "A-123"}, {"id": "A-123", "feature_type": "region"}],
                         ids=['not-found', 'too-many-for-id'])
def test_measurement_timeseries_tvp_observation_errors(query):
    """Test not found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    pytest.raises(Exception, synthesizer.measurement_timeseries_tvp_observations, **query)


def test_measurement_timeseries_tvp_observations_count():
    """Test  found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_features=['test'], observed_property_variables=['test'], start_date='2016-02-01')
    if isinstance(measurement_timeseries_tvp_observations, DataSourceModelIterator):
        count = 0
        assert measurement_timeseries_tvp_observations.synthesis_response is not None
        assert measurement_timeseries_tvp_observations.synthesis_response.query is not None
        assert isinstance(measurement_timeseries_tvp_observations.synthesis_response.query,
                          QueryMeasurementTimeseriesTVP)
        assert measurement_timeseries_tvp_observations.synthesis_response.query.monitoring_features == ['test']
        assert measurement_timeseries_tvp_observations.synthesis_response.query.observed_property_variables == ['test']
        assert measurement_timeseries_tvp_observations.synthesis_response.query.start_date == datetime.date(2016, 2, 1)

        for mf in measurement_timeseries_tvp_observations:
            count += 1

        assert count == 4
    else:
        assert measurement_timeseries_tvp_observations is not None


@pytest.mark.parametrize("plugins, query, expected_count",
                         [(['basin3d.plugins.usgs.USGSDataSourcePlugin'], {}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'USGS'}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha'}, 4),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'FOO'}, 0)
                          ],
                         ids=['USGS-only', 'USGS-plus', 'Alpha-plus', 'Bad-DataSource'])
def test_observed_properties(plugins, query, expected_count):
    """Test observed properties search"""

    synthesizer = register(plugins)
    observed_properties = synthesizer.observed_properties(**query)

    # TODO are there other things to test?
    count = 0
    for op in observed_properties:
        print(op)
        count += 1

    assert count == expected_count


@pytest.mark.parametrize("plugins, query, expected_count",
                         [(['basin3d.plugins.usgs.USGSDataSourcePlugin'], {}, 168),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'USGS'}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha'}, 4),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'FOO'}, 0)
                          ],
                         ids=['USGS-only', 'USGS-plus', 'Alpha-plus', 'Bad-DataSource'])
def test_observed_property_variables(plugins, query, expected_count):
    """ Test observed property variables """

    synthesizer = register(plugins)
    observed_properties = synthesizer.observed_property_variables(**query)

    # TODO are there other things to test?
    count = 0
    for op in observed_properties:
        print(op)
        count += 1

    assert count == expected_count
