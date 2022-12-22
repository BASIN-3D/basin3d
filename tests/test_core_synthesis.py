import pytest

from basin3d.core.models import DataSource
from basin3d.core.plugin import DataSourcePluginAccess
from basin3d.core.schema.enum import NO_MAPPING_TEXT
from basin3d.core.schema.query import QueryMonitoringFeature, QueryMeasurementTimeseriesTVP
from basin3d.core.synthesis import TranslatorMixin
from tests.testplugins import alpha


@pytest.fixture
def alpha_plugin_access():
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')

    return DataSourcePluginAccess(alpha_ds, catalog)


@pytest.mark.parametrize('query, expected_results, set_transformed_attr',
                         # single-compound
                         [(QueryMeasurementTimeseriesTVP(observed_property=['Ag'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas'], sampling_medium=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # both-compound
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], sampling_medium=['GAS'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag_gas'], sampling_medium=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # single-compound+non-compound
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], statistic=['MEAN'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas'], statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY'], 'statistic': ['mean']}),
                          # not_supported
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag'], statistic=['INSTANT'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas'], statistic=None, start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY'], 'statistic': [NO_MAPPING_TEXT]}),
                          # not_supported-opv
                          (QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'RDC'], start_date='2019-01-01', monitoring_feature=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property=['Ag', 'Ag_gas', NO_MAPPING_TEXT], start_date='2019-01-01', monitoring_feature=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          ],
                         ids=['single-compound', 'both-compound', 'single-compound+non-compound', 'not_supported', 'not_supported-opv'])
def test_translator_translate_mapped_query_attrs(alpha_plugin_access, query, expected_results, set_transformed_attr):
    translator = TranslatorMixin()
    results = translator.translate_mapped_query_attrs(alpha_plugin_access, query)
    for attr, value in set_transformed_attr.items():
        setattr(expected_results, attr, value)
    assert results == expected_results


def test_translator_order_mapped_fields(alpha_plugin_access):
    translator = TranslatorMixin()
    ordered_fields = translator._order_mapped_fields(alpha_plugin_access, ['statistic', 'aggregation_duration', 'sampling_medium', 'observed_property'])
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
    translator = TranslatorMixin()
    translated_query = query.copy()
    for attr, value in set_translated_attr.items():
        setattr(translated_query, attr, value)
    assert translator.is_translated_query_valid('Alpha', query, translated_query) is expected_result


@pytest.mark.parametrize("query, set_translated_attr",
                         [(QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237'], observed_property=['Ag'], start_date='2019-01-01'),
                           {'monitoring_feature': ['9237']}),
                          (QueryMeasurementTimeseriesTVP(monitoring_feature=['A-9237', 'R-8e3838', 'A-00000'], observed_property=['Ag'], start_date='2019-01-01'),
                           {'monitoring_feature': ['9237', '00000']}),
                          (QueryMeasurementTimeseriesTVP(monitoring_feature=['F-9237'], observed_property=['Ag'], start_date='2019-01-01'),
                           {'monitoring_feature': []}),
                          (QueryMonitoringFeature(monitoring_feature=['A-9237', 'R-8e3838'], parent_feature=['A-00000']),
                           {'monitoring_feature': ['9237'], 'parent_feature': ['00000']}),
                          ],
                         ids=["single", "multiple", "none", "monitoring_feature_query"])
def test_translator_prefixed_query_attrs(alpha_plugin_access, query, set_translated_attr):
    """Filtering of query arguments"""
    translator = TranslatorMixin()
    translated_query = query.copy()
    for attr, value in set_translated_attr.items():
        setattr(translated_query, attr, value)
    assert translator.translate_prefixed_query_attrs(alpha_plugin_access, query) == translated_query


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
    translator = TranslatorMixin()
    translated_query = query.copy()
    cleaned_query = query.copy()
    for attr, value in set_translated_attr.items():
        setattr(translated_query, attr, value)
    for attr, value in set_cleaned_query_attr.items():
        setattr(cleaned_query, attr, value)
    assert translator.clean_query(translated_query) == cleaned_query
