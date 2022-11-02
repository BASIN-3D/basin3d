import pytest
import datetime

from basin3d.core.models import DataSource
from basin3d.core.plugin import DataSourcePluginAccess
from basin3d.core.schema.query import QueryBase, QueryMeasurementTimeseriesTVP
from basin3d.core.synthesis import _synthesize_query_identifiers, TranslatorMixin
from tests.testplugins import alpha


@pytest.fixture
def alpha_plugin_access():
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])
    alpha_ds = DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/')

    return DataSourcePluginAccess(alpha_ds, catalog)


@pytest.mark.parametrize("values, filtered_params",
                         [(["F-9237"], ['9237']),
                          (["F-9237", "R-8e38e8", "F-00000"], ['9237', '00000']),
                          ("R-9237", [])],
                         ids=["single", "multiple", "none"])
def test_extract_query_param_ids(values, filtered_params):
    """Filtering of query arguments"""

    synthesized_values = _synthesize_query_identifiers(values, "F")
    assert synthesized_values == filtered_params


@pytest.mark.parametrize('query, expected_results, set_transformed_attr',
                         # single-compound
                         [(QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], start_date='2019-01-01', monitoring_features=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag', 'Ag_gas'], sampling_medium=None, start_date='2019-01-01', monitoring_features=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # both-compound
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], sampling_medium=['GAS'], start_date='2019-01-01', monitoring_features=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag_gas'], sampling_medium=None, start_date='2019-01-01', monitoring_features=['A-3']),
                           {'aggregation_duration': ['DAY']}),
                          # single-compound+non-compound
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], statistic=['MEAN'], start_date='2019-01-01', monitoring_features=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag', 'Ag_gas'], statistic=None, start_date='2019-01-01', monitoring_features=['A-3']),
                           {'aggregation_duration': ['DAY'], 'statistic': ['mean']}),
                          # not_supported
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], statistic=['INSTANT'], start_date='2019-01-01', monitoring_features=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag', 'Ag_gas'], statistic=None, start_date='2019-01-01', monitoring_features=['A-3']),
                           {'aggregation_duration': ['DAY'], 'statistic': ['NOT_SUPPORTED']}),
                          # not_supported-opv
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag', 'RDC'], start_date='2019-01-01', monitoring_features=['A-3']),
                           QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag', 'Ag_gas', 'NOT_SUPPORTED'], start_date='2019-01-01', monitoring_features=['A-3']),
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
    ordered_fields = translator._order_mapped_fields(alpha_plugin_access, ['statistic', 'aggregation_duration', 'sampling_medium', 'observed_property_variables'])
    assert ordered_fields == ['observed_property_variables', 'sampling_medium', 'statistic', 'aggregation_duration']


@pytest.mark.parametrize('query, set_translated_attr, expected_result',
                         [(QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], start_date='2019-01-01', monitoring_features=['A-3']),
                           {}, True),
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], start_date='2019-01-01', monitoring_features=['A-3']),
                           {'observed_property_variables': ['NOT_SUPPORTED']}, False),
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], start_date='2019-01-01', monitoring_features=['A-3']),
                           {'observed_property_variables': ['Ag', 'NOT_SUPPORTED']}, True),
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], start_date='2019-01-01', monitoring_features=['A-3']),
                           {'start_date': 'NOT_SUPPORTED'}, True),
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], start_date='2019-01-01', monitoring_features=['A-3']),
                           {'aggregation_duration': 'NOT_SUPPORTED'}, False),
                          (QueryMeasurementTimeseriesTVP(observed_property_variables=['Ag'], start_date='2019-01-01', monitoring_features=['A-3']),
                           {'aggregation_duration': {'key', 'value'}}, None),
                          ],
                         ids=['valid', 'invalid_only-list-not_supported', 'valid-2', 'valid-not-mapped-field',
                              'invalid-single-not-supported', 'invalid_attr_value_type'])
def test_translator_is_translated_query_valid(alpha_plugin_access, query, set_translated_attr, expected_result):
    translator = TranslatorMixin()
    translated_query = query.copy()
    for attr, value in set_translated_attr.items():
        setattr(translated_query, attr, value)
    assert translator.is_translated_query_valid(query, translated_query) is expected_result