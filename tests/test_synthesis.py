import datetime
import pytest

import basin3d
from basin3d.core.catalog import CatalogException
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.synthesis import DataSourceModelIterator
from basin3d.synthesis import register, SynthesisException, SynthesisResponse


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


@pytest.mark.parametrize("query, expected_monitoring_feature_id, msg",
                         [({"id": "A-123"}, None, None),
                          ({"id": "A-1", "feature_type": "region"}, None, None),
                          ({"id": "A-1", 'datasource': 'A'}, "A-1", None),
                          ({"id": "A-1"}, "A-1", None),
                          ({"id": "A-1", "monitoring_feature": "A-123"}, "A-1",
                           'Monitoring Feature query has both id A-1 and monitoring_feature A-123 specified. Removing monitoring_feature and using id.'),
                          ({"id": "A-123", "monitoring_feature": "A-1"}, None,
                           'Monitoring Feature query has both id A-123 and monitoring_feature A-1 specified. Removing monitoring_feature and using id.'),
                          ],
                         ids=['not-found', 'wrong-feature-type', 'point-datasource', 'point', 'with-mf-valid', 'with-mf-invalid'])
def test_monitoring_features_by_id(query, expected_monitoring_feature_id, msg):
    """Test query of single monitoring feature using id """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    result = synthesizer.monitoring_features(**query)

    assert isinstance(result, SynthesisResponse)

    if expected_monitoring_feature_id:
        assert result.data.id == expected_monitoring_feature_id
    else:
        assert result.data is None

    if msg is not None:
        assert result.messages[0].msg == msg


def test_monitoring_features():
    """Test monitoring features"""

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features()
    if isinstance(monitoring_features, DataSourceModelIterator):
        count = 0
        assert monitoring_features.synthesis_response is not None
        assert monitoring_features.synthesis_response.dict() == {'data': None,
                                                                 'messages': [],
                                                                 'query': {'datasource': None,
                                                                           'feature_type': None,
                                                                           'id': None,
                                                                           'is_valid_translated_query': None,
                                                                           'monitoring_feature': None,
                                                                           'parent_feature': None}}
        assert isinstance(monitoring_features.synthesis_response.query, QueryMonitoringFeature)

        for mf in monitoring_features:
            count += 1

        assert monitoring_features.synthesis_response is not None
        assert monitoring_features.synthesis_response.dict() == {'data': None,
                                                                 'messages': [{'level': 'WARN',
                                                                               'msg': 'message1',
                                                                               'where': ['Alpha', 'MonitoringFeature']},
                                                                              {'level': 'WARN',
                                                                               'msg': 'message2',
                                                                               'where': ['Alpha', 'MonitoringFeature']},
                                                                              {'level': 'WARN',
                                                                               'msg': 'message3',
                                                                               'where': ['Alpha', 'MonitoringFeature']}],
                                                                 'query': {'datasource': None,
                                                                           'feature_type': None,
                                                                           'id': None,
                                                                           'is_valid_translated_query': None,
                                                                           'monitoring_feature': None,
                                                                           'parent_feature': None}}

        assert count == 1
    else:
        assert monitoring_features is not None


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
        monitoring_feature=['A-3'], observed_property=['Al'], start_date='2016-02-01')
    if isinstance(measurement_timeseries_tvp_observations, DataSourceModelIterator):
        count = 0
        assert measurement_timeseries_tvp_observations.synthesis_response is not None
        assert measurement_timeseries_tvp_observations.synthesis_response.query is not None
        assert isinstance(measurement_timeseries_tvp_observations.synthesis_response.query,
                          QueryMeasurementTimeseriesTVP)
        assert measurement_timeseries_tvp_observations.synthesis_response.query.monitoring_feature == ['A-3']
        assert measurement_timeseries_tvp_observations.synthesis_response.query.observed_property == ['Al']
        assert measurement_timeseries_tvp_observations.synthesis_response.query.start_date == datetime.date(2016, 2, 1)

        for mf in measurement_timeseries_tvp_observations:
            count += 1

        assert count == 1
    else:
        assert measurement_timeseries_tvp_observations is not None


@pytest.mark.parametrize("plugins, query, expected_count",
                         [(['basin3d.plugins.usgs.USGSDataSourcePlugin'], {}, 53),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin', 'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'USGS'}, 53),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin', 'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha'}, 14),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin', 'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha', 'attr_type': 'OBSERVED_PROPERTY'}, 6),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin', 'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha', 'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': ['Ag']}, 1),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin', 'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha', 'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': ['Ag'], 'from_basin3d': True}, 2),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin', 'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'FOO'}, -1)
                          ],
                         ids=['USGS-only', 'USGS-plus', 'Alpha-plus', 'Alpha-OP', 'Alpha-OP-Ag-datasource', 'Alpha-OP-Ag-basin3d', 'Bad-DataSource'])
def test_attribute_mappings(plugins, query, expected_count):
    """Test attribute_mappings search"""

    synthesizer = register(plugins)
    results = synthesizer.attribute_mappings(**query)

    if expected_count > 0:
        count = 0
        for attr_mapping in results:
            count += 1

        assert count == expected_count

    else:
        with pytest.raises(CatalogException):
            for attr_mapping in results:
                pass


def test_observed_properties():
    """Test observed_properties method"""
    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    results = synthesizer.observed_properties()

    count = 0
    for op in results:
        count += 1

    assert count == 168
