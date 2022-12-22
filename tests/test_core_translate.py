import logging
import pytest

from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP

# catalog.find_datasource_vocab (TinyDB)
# ToDo: add test for triple compound mapping
@pytest.mark.parametrize(
    'attr_type, basin3d_vocab, basin3d_query, expected_results, expected_msgs',
    # non-compound
    [('statistic', 'MEAN', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['ACT'], start_date='2020-01-01', statistic=['MEAN']),
      ['mean'], []),
     # non-compound-no-match
     ('statistic', 'ESTIMATED', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['ACT'], start_date='2020-01-01', statistic=['MEAN']),
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type "STATISTIC" and BASIN-3D vocab: ESTIMATED.']),
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
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type "OBSERVED_PROPERTY:SAMPLING_MEDIUM" and BASIN-3D vocab: Al:GAS.']),
     # compound-compound_query_lists
     ('observed_property', 'Ag', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Ag', 'Al'], start_date='2020-01-01', sampling_medium=['WATER', 'GAS']),
      ['Ag', 'Ag_gas'], []),
     # compound-compound_query_no_match
     ('observed_property', 'Hg', QueryMeasurementTimeseriesTVP(monitoring_feature=['A-1'], observed_property=['Ag', 'Al', 'Hg'], start_date='2020-01-01'),
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type "OBSERVED_PROPERTY:SAMPLING_MEDIUM" and BASIN-3D vocab: Hg:.*.']),
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

    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    caplog.clear()

    results = catalog.find_datasource_vocab('Alpha', attr_type, basin3d_vocab, basin3d_query)
    assert sorted(results) == sorted(expected_results)

    if expected_msgs:
        log_msgs = [rec.message for rec in caplog.records]
        for msg in expected_msgs:
            assert msg in log_msgs
