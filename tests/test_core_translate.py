import logging
import pytest

from basin3d.core import translate
from basin3d.core.models import DataSource
from basin3d.core.plugin import DataSourcePluginAccess
from basin3d.core.schema.enum import NO_MAPPING_TEXT
from basin3d.core.schema.query import QueryMonitoringFeature, QueryMeasurementTimeseriesTVP
from tests.testplugins import alpha, complexmap


def alpha_plugin_access():
    """
    Create a DataSourcePluginAccess object for the Alpha plugin
    """
    from basin3d.core.catalog import CatalogSqlAlchemy
    catalog = CatalogSqlAlchemy()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')

    return DataSourcePluginAccess(alpha_ds, catalog)


def complex_plugin_access():
    """
    Create a DataSourcePluginAccess object for the Complexmap plugin
    """
    from basin3d.core.catalog import CatalogSqlAlchemy
    catalog = CatalogSqlAlchemy()
    catalog.initialize([p(catalog) for p in [complexmap.ComplexmapSourcePlugin]])
    complex_ds = DataSource(id='Complexmap', name='Complexmap', id_prefix='C', location='https://asource.foo/')

    return DataSourcePluginAccess(complex_ds, catalog)


# ToDo: add test for triple compound mapping
@pytest.mark.parametrize(
    'attr_type, basin3d_vocab, basin3d_query, expected_results, expected_msgs',
    # non-compound
    [('statistic', 'MEAN', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['ACT'], start_date='2020-01-01', statistic=['MEAN']),
      ['mean'], []),
     # non-compound-no-match
     ('statistic', 'ESTIMATED', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['ACT'], start_date='2020-01-01', statistic=['MEAN']),
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type STATISTIC and BASIN-3D vocab ESTIMATED.']),
     # compound-simple_query
     ('observed_property', 'ACT', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['ACT'], start_date='2020-01-01'),
      ['Acetate'], []),
     # compound-simple_query-multimap
     ('observed_property', 'Al', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Al'], start_date='2020-01-01'),
      ['Al', 'Aluminum'], []),
     # compound-compound_query-multimap
     ('observed_property', 'Al', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Al'], start_date='2020-01-01', sampling_medium=['WATER']),
      ['Al', 'Aluminum'], []),
     # compound-compound_query
     ('observed_property', 'Ag', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Ag'], start_date='2020-01-01', sampling_medium=['WATER']),
      ['Ag'], []),
     # compound-compound_query-no_compound_match
     ('observed_property', 'Al', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Al'], start_date='2020-01-01', sampling_medium=['GAS']),
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type OBSERVED_PROPERTY:SAMPLING_MEDIUM and BASIN-3D vocab Al:GAS.']),
     # compound-compound_query_lists
     ('observed_property', 'Ag', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Ag', 'Al'], start_date='2020-01-01', sampling_medium=['WATER', 'GAS']),
      ['Ag', 'Ag_gas'], []),
     # compound-compound_query_no_match
     ('observed_property', 'Hg', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Ag', 'Al', 'Hg'], start_date='2020-01-01'),
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type OBSERVED_PROPERTY:SAMPLING_MEDIUM and BASIN-3D vocab Hg:.*.']),
     # non-compound_non-query-class
     ('statistic', 'MEAN', {'statistic': 'MEAN'}, ['mean'], []),
     # compound-simple_query_non-query-class
     ('observed_property', 'ACT', {'observed_property': 'ACT'}, ['Acetate'], []),
     # compound-compound_query_non-query-class-lists
     ('observed_property', 'Ag', {'observed_property': ['Ag'], 'sampling_medium': ['WATER']}, ['Ag'], []),
     # compound_non-query-class_str-value
     ('observed_property', 'Ag', {'observed_property': 'Ag'}, ['Ag', 'Ag_gas'], []),
     ],
    ids=['non-compound', 'non-compound-no-match', 'compound-simple_query', 'compound-simple_query-multimap',
         'compound-compound_query-multimap', 'compound-compound_query', 'compound-compound_query-no_compound_match',
         'compound-compound_query_lists', 'compound-compound_query_no_match',
         'non-compound_non-query-class', 'compound-simple_query_non-query-class',
         'compound-compound_query_non-query-class-lists', 'compound_non-query-class_str-value'])
def test_find_datasource_vocab(caplog, attr_type, basin3d_vocab, basin3d_query, expected_results, expected_msgs):
    caplog.set_level(logging.INFO)
    caplog.clear()

    results = translate._translate_to_datasource_vocab(alpha_plugin_access(), attr_type, basin3d_vocab, basin3d_query)
    assert sorted(results) == sorted(expected_results)

    if expected_msgs:
        log_msgs = [rec.message for rec in caplog.records]
        for msg in expected_msgs:
            assert msg in log_msgs


@pytest.mark.parametrize('ds, query, expected_results, set_transformed_attr',
                         [
                          # single-compound
                          ('Alpha',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas'], sampling_medium=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # both-compound
                          ('Alpha',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], sampling_medium=['GAS'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag_gas'], sampling_medium=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # single-compound+non-compound
                          ('Alpha',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], statistic=['MEAN'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas'], statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY'], 'statistic': ['mean']}),
                          # not_supported
                          ('Alpha',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], statistic=['INSTANT'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas'], statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY'], 'statistic': [NO_MAPPING_TEXT]}),
                          # not_supported-opv
                          ('Alpha',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'RDC'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas', NO_MAPPING_TEXT], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # multi-mapped
                          ('Alpha',
                           QueryMeasurementTimeseriesTVP(observed_property=['Al'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Al', 'Aluminum'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # multi-mapped-complex
                          ('Alpha',
                           QueryMeasurementTimeseriesTVP(observed_property=['Al'], sampling_medium=['WATER'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Al', 'Aluminum'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # complex-single-compound
                          ('Complexmap',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Mean Ag', 'Min Ag_gas'], sampling_medium=None, statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # complex-two-compound
                          ('Complexmap',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], sampling_medium=['GAS'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Min Ag_gas'], sampling_medium=None, statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # complex-other-two-compound
                          ('Complexmap',
                           QueryMeasurementTimeseriesTVP(observed_property=['Al'], statistic=['MEAN'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Mean Al', 'Mean Aluminum'], sampling_medium=None, statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # complex-all-compound
                          ('Complexmap',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], sampling_medium=['GAS'], statistic=['MIN'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Min Ag_gas'], sampling_medium=None, statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # complex-all-compound-no-mapping
                          ('Complexmap',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag'], sampling_medium=['WATER'], statistic=['MIN'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=[NO_MAPPING_TEXT], sampling_medium=None, statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # complex-all-compound-multiple-values
                          ('Complexmap',
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Al'], sampling_medium=['WATER', 'GAS'], statistic=['MIN', 'MEAN'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Mean Ag', 'Min Ag_gas', 'Minimum Aluminum', 'Mean Al', 'Mean Aluminum'], sampling_medium=None, statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          ],
                         ids=['single-compound', 'both-compound', 'single-compound+non-compound', 'not_supported', 'not_supported-opv', 'multi-mapped', 'multi-mapped-complex',
                              'complex-single-compound', 'complex-two-compound', 'complex-other-two-compound', 'complex-all-compound', 'complex-all-compound-no-mapping',
                              'complex-all-compound-multiple-values'])
def test_translator_translate_mapped_query_attrs(ds, query, expected_results, set_transformed_attr):
    plugin_access = ds == "Complexmap" and complex_plugin_access() or alpha_plugin_access()
    results = translate._translate_mapped_query_attrs(plugin_access, query)
    for attr, value in set_transformed_attr.items():
        setattr(expected_results, attr, value)
    assert results == expected_results


def test_translator_order_mapped_fields():
    ordered_fields = translate._order_mapped_fields(alpha_plugin_access(), ['statistic', 'aggregation_duration', 'sampling_medium', 'observed_property'])
    assert ordered_fields == ['observed_property', 'sampling_medium', 'statistic', 'aggregation_duration']


@pytest.mark.parametrize('query, set_translated_attr, expected_result',
                         [(QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {}, True),
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'observed_property': [NO_MAPPING_TEXT]}, False),
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'observed_property': ['Ag', NO_MAPPING_TEXT]}, True),
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'start_date': NO_MAPPING_TEXT}, True),
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': NO_MAPPING_TEXT}, False),
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': {'key', 'value'}}, None),
                          ],
                         ids=['valid', 'invalid_only-list-not_supported', 'valid-2', 'valid-not-mapped-field',
                              'invalid-single-not-supported', 'invalid_attr_value_type'])
def test_translator_is_translated_query_valid(query, set_translated_attr, expected_result):
    translated_query = query.copy()
    for attr, value in set_translated_attr.items():
        setattr(translated_query, attr, value)
    assert translate._is_translated_query_valid('Alpha', query, translated_query) is expected_result


@pytest.mark.parametrize("query, set_translated_attr",
                         [(QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237'], observed_property=['Ag'], start_date='2019-01-01'),
                           {'monitoring_feature': ['9237']}),
                          (QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237', 'R-8e3838', 'A-00000'], observed_property=['Ag'], start_date='2019-01-01'),
                           {'monitoring_feature': ['9237', '00000']}),
                          (QueryMeasurementTimeseriesTVP(monitoring_feature=['F-9237'], observed_property=['Ag'], start_date='2019-01-01'),
                           {'monitoring_feature': []}),
                          (QueryMonitoringFeature(monitoring_feature=['A-9237', 'R-8e3838'], parent_feature=['A-00000'], id='A-345aa'),
                           {'monitoring_feature': ['9237'], 'parent_feature': ['00000'], 'id': '345aa'}),
                          ],
                         ids=["single", "multiple", "none", "monitoring_feature_query"])
def test_translator_prefixed_query_attrs(query, set_translated_attr):
    """Filtering of query arguments"""
    translated_query = query.copy()
    for attr, value in set_translated_attr.items():
        setattr(translated_query, attr, value)
    assert translate._translate_prefixed_query_attrs(alpha_plugin_access(), query) == translated_query


@pytest.mark.parametrize('query, set_translated_attr, set_cleaned_query_attr',
                         [(QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237'], observed_property=['Ag'], start_date='2019-01-01'),
                           {}, {}),
                          (QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237'], observed_property=['Ag', NO_MAPPING_TEXT], start_date='2019-01-01'),
                           {}, {'observed_property': ['Ag']}),
                          (QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237'], observed_property=[NO_MAPPING_TEXT, NO_MAPPING_TEXT], start_date='2019-01-01'),
                           {}, {'observed_property': []}),
                          (QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237'], observed_property=['Ag'], start_date='2019-01-01'),
                           {'observed_property': NO_MAPPING_TEXT}, {'observed_property': None}),
                          ],
                         ids=['no-change', 'rm-one-list', 'rm-all-list', 'rm-string'])
def test_translator_clean_query(query, set_translated_attr, set_cleaned_query_attr):
    translated_query = query.copy()
    cleaned_query = query.copy()
    for attr, value in set_translated_attr.items():
        setattr(translated_query, attr, value)
    for attr, value in set_cleaned_query_attr.items():
        setattr(cleaned_query, attr, value)
    assert translate._clean_query(translated_query) == cleaned_query
