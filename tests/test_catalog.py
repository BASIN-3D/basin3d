import logging
from unittest.mock import Mock

import pytest
from tinydb import TinyDB

from basin3d.core.catalog import CatalogException
from basin3d.core.models import DataSource, ObservedProperty, AttributeMapping
from basin3d.core.schema.enum import SamplingMediumEnum, StatisticEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP
from tests.testplugins import alpha
from basin3d.plugins import usgs

log = logging.Logger(__name__)


# ---------------------------------------
# Helper and mock functions

def mock_plugin_get_meta():
    class PluginMeta:
        id = 'Alpha'
        name = 'Alpha'
        location = 'https://asource.foo/'
        id_prefix = 'A'

    return PluginMeta


def mock_init_catalog(dummyself):
    test_db_filepath = './tests/resources/test.json'
    test_catalog = TinyDB(test_db_filepath)
    test_catalog.truncate()
    return test_catalog


def get_datasource_from_id(id, id_prefix, location='test.location'):
    """Helper file to turn id into datasource"""
    return DataSource(id=id, name=id, id_prefix=id_prefix, location=location)


# ---------------------------------------
# Fixtures


@pytest.fixture
def test_catalog():
    return mock_init_catalog('dummyself')


@pytest.fixture
def catalog(monkeypatch):
    from basin3d.core.catalog import CatalogTinyDb

    catalog = CatalogTinyDb()
    catalog.plugin_dir = 'tests.testplugins'

    catalog.initialize([])

    return catalog


# ---------------------------------------
# Tests
# ********* ORDER of tests matters!! see comments
# ********* The catalog is only first created in the test_create_catalog function below.

# CompoundMapping model (catalogBASE)
def test_compound_mapping_model():
    from basin3d.core.catalog import CatalogBase

    catalog = CatalogBase()
    datasource = get_datasource_from_id('Alpha', 'A')
    compound_mapping = catalog.CompoundMapping(attr_type='TEST',
                                               compound_mapping='TEST:TEST',
                                               datasource=datasource)
    assert compound_mapping.attr_type == 'TEST'
    assert compound_mapping.compound_mapping == 'TEST:TEST'
    assert compound_mapping.datasource == datasource


# _get_attribute_enum (catalogBASE)
def test_get_attribute_enum():
    from basin3d.core.catalog import CatalogBase
    catalog = CatalogBase()
    assert catalog._get_attribute_enum('WATER', SamplingMediumEnum) == SamplingMediumEnum.WATER
    assert catalog._get_attribute_enum('FOO', SamplingMediumEnum) is None

# ToDo: Think about outcomes of logic
# ToDo: see if can resolve datasource vs datasource_id arguments


@pytest.mark.parametrize("plugins", [(["foo"]), [Mock()]])
def test_catalog_error(plugins):
    """Test Catalog Error"""
    from basin3d.core.catalog import CatalogTinyDb, CatalogException
    catalog = CatalogTinyDb()
    pytest.raises(CatalogException, catalog.initialize, plugins)


# Test with different variable files.
# Last test uses the default basin3d hydrology variables which is needed for rest of tests.
def test_gen_basin3d_variable_store(catalog, caplog):
    caplog.set_level(logging.INFO, logger=__name__)
    catalog.variable_dir = 'tests.resources'

    caplog.clear()
    catalog.variable_filename = 'basin3d_variables_duplicate.csv'
    catalog._gen_variable_store()
    assert catalog._get_observed_property('Br').to_dict() == ObservedProperty(
        basin3d_vocab='Br', full_name='Bromide (Br)',
        categories=['Biogeochemistry', 'Anions'], units='mM').to_dict()
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Duplicate BASIN-3D variable Br found. Skipping duplicate.' in log_msgs

    catalog.variable_filename = 'basin3d_variables_wrong_header.csv'
    from basin3d.core.catalog import CatalogException
    with pytest.raises(CatalogException):
        catalog._gen_variable_store()

    # KEEP this test last: it resets the variable file.
    catalog.variable_filename = 'basin3d_observed_property_variables_vocabulary.csv'
    catalog.variable_dir = 'basin3d.data'
    catalog._gen_variable_store()
    assert catalog._get_observed_property('ACT') == ObservedProperty(
        basin3d_vocab='ACT', full_name='Acetate (CH3COO)',
        categories=['Biogeochemistry', 'Anions'], units='mM')
    assert catalog._get_observed_property('heya') is None


# catalog.find_observed_property (TinyDB)
def test_find_observed_property(catalog):
    assert catalog.find_observed_property('ACT') == ObservedProperty(basin3d_vocab='ACT',
                                                                     full_name='Acetate (CH3COO)',
                                                                     categories=['Biogeochemistry' ,'Anions'],
                                                                     units='mM')
    assert catalog.find_observed_property('FOO') is None


def test_not_initialized_errors(caplog):
    from basin3d.core.catalog import CatalogTinyDb, CatalogException
    new_catalog = CatalogTinyDb()

    caplog.clear()
    with pytest.raises(CatalogException):
        new_catalog._insert('Foo')
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Could not insert record. Catalog not initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        new_catalog.find_observed_property('FOO')
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Variable Store has not been initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        test_gen = new_catalog.find_observed_properties()
        for op in test_gen:
            pass
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Variable Store has not been initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        new_catalog._find_compound_mapping('Foo', 'FOO')
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Compound mapping database has not been initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        new_catalog.find_compound_mappings('Foo')
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Compound mapping database has not been initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        new_catalog.find_attribute_mapping('Foo', 'FOO', 'Foo')
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Attribute Store has not been initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        test_gen = new_catalog.find_attribute_mappings()
        for attr_map in test_gen:
            pass
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Attribute Store has not been initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        new_catalog.find_datasource_vocab('Foo', 'FOO', 'Foo', {})
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Attribute Store has not been initialized.' in log_msgs

    caplog.clear()
    del log_msgs
    with pytest.raises(CatalogException):
        new_catalog.find_compound_mapping_attributes('Foo', 'FOO')
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Compound mapping database has not been initialized.' in log_msgs


def test_find_observed_properties(caplog, catalog):
    """Test the observed property search works"""
    caplog.set_level(logging.INFO)

    # all observed_properties are returned
    assert ['ACT', 'Br', 'Cl', 'DIN', 'DTN', 'F', 'NO3', 'NO2', 'PO4', 'SO4', 'S2', 'S2O3', 'HCO3', 'DIC', 'DOC',
            'NPOC', 'POC', 'TIC', 'TOC', 'd13C', 'CO2_d13C_soilgas', 'CH4_d13C_soilgas', 'd15N', 'N2O_d15N_soilgas',
            'CO2_d18O_soilgas', 'N2O_d18O_soilgas', 'SO4_d34S', 'U235', 'H2O_d18O', 'H2O_dD', 'NH4', 'Ca', 'Mg', 'K',
            'Si', 'Na', 'MICRO_COV', 'MGO1', 'MGO2', 'MGO3', 'GENE_TRAN', 'MICRO_PEP', 'MICRO_PLFA', 'NH3', 'CO2', 'H2',
            'H2S', 'CH4', 'N2', 'N2O', 'O2', 'Al', 'Sb', 'As', 'Ba', 'Be', 'B', 'Cd', 'Cs', 'Co', 'Cu', 'Eu', 'Ge',
            'Fe2', 'Hg', 'Pb', 'Li', 'Mn2', 'Mo', 'Ni', 'Rb', 'Se', 'Ag', 'Sr', 'Th', 'Sn', 'Ti', 'Cr', 'FeT', 'Mn',
            'P', 'U', 'V', 'Zn', 'Zr', 'AT', 'ALB', 'APA', 'APR', 'DP', 'ET', 'HI', 'PPT', 'PPT_TOT_5', 'PPT_TOT_10',
            'PPT_TOT_60', 'PPT_TOT_DAY', 'UV_IN', 'UV_REF', 'NIR_IN', 'NIR_REF', 'PAR_IN', 'PAR_REF', 'LWIR_IN',
            'LWIR_REF', 'BB_IN', 'BB_REF', 'RH', 'SD', 'SWE', 'SDEN', 'ST', 'PPT_SF', 'SRAD', 'WBT', 'W_DIR', 'W_GS',
            'W_SPD', 'W_CH', 'ERT', 'CEC', 'EXAFS', 'SEQ', 'XRD', 'XRF', 'XANES', 'HCND', 'SED_SIZE', 'HCND_Sat',
            'Porosity', 'SBD', 'SYD', 'HCND_Unsat', 'RET_CUR', 'LSE', 'GWF', 'Well logs', 'SAT', 'SDE', 'SMO', 'STM',
            'SWP', 'SEC', 'FDOM', 'DO', 'EC', 'SC', 'SAL', 'GWL', 'PH', 'ORP', 'RDC', 'SWL', 'WLH', 'WLE', 'WT', 'TDS',
            'TSS', 'TRB', 'STO_RES', 'LAI', 'PLT_HT', 'PAI', 'PFT', 'SAP', 'RGB', 'GCC', 'NDVI'] == [i.basin3d_vocab for
                                                                                                     i in
                                                                                                     catalog.find_observed_properties()]

    # specified valid observed properties returned
    assert ['ACT', 'Br'] == [i.basin3d_vocab for i in catalog.find_observed_properties(['ACT', 'Br'])]

    # on invalid observed properties is not returned and warning message generated.
    caplog.clear()
    assert ['ACT', 'Br'] == [i.basin3d_vocab for i in catalog.find_observed_properties(['ACT', 'FOO', 'Br'])]
    log_msgs = [rec.message for rec in caplog.records]
    assert 'BASIN-3D does not support variable FOO' in log_msgs


def test_process_plugin_attr_mapping(catalog, caplog):
    """Test a bad mapping file with the wrong header"""
    from tests.testplugins import alpha
    from basin3d.core.catalog import CatalogException
    with pytest.raises(CatalogException):
        catalog._process_plugin_attr_mapping(
            alpha.AlphaSourcePlugin(catalog), filename='mapping_alpha_wrong_header.csv', datasource=DataSource())

    caplog.set_level(logging.INFO)
    caplog.clear()
    from tests.testplugins import beta
    catalog._process_plugin_attr_mapping(beta.BetaSourcePlugin(catalog), filename='beta_mapping.csv', datasource=DataSource(id='Beta'))
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Datasource Beta: Attribute type STAT is not supported. Skipping mapping.' in log_msgs
    assert 'Datasource Beta: basin3d_vocab MAXIMUM for attr_type STATISTIC is not a valid BASIN-3D vocabulary. Skipping attribute mapping.' in log_msgs


@pytest.mark.parametrize("plugins, query, expected",
                         # Wrong-plugin-inititalized
                         [([usgs.USGSDataSourcePlugin],
                           {'datasource_id': 'Alpha', 'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'ACT'},
                           AttributeMapping(
                               attr_type='OBSERVED_PROPERTY',
                               basin3d_vocab='NOT_SUPPORTED',
                               basin3d_desc=[],
                               datasource_vocab='ACT',
                               datasource_desc='No datasource was found for id "Alpha".',
                               datasource=DataSource())
                           ),
                          # USGS
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'datasource_id': 'USGS', 'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': '00095'},
                           AttributeMapping(attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
                                            basin3d_vocab='SC:WATER',
                                            basin3d_desc=[ObservedProperty(basin3d_vocab='SC', full_name='Specific Conductance (SC)',
                                                                           categories=['Hydrogeology', 'Water Physical/Quality Parameters'], units='uS/cm'),
                                                          SamplingMediumEnum.WATER],
                                            datasource_vocab='00095',
                                            datasource_desc='Specific conductance, water, unfiltered, microsiemens per centimeter at 25 degrees Celsius',
                                            datasource=DataSource(id='USGS', name='USGS', id_prefix='USGS',
                                                                  location='https://waterservices.usgs.gov/nwis/', credentials={}))
                           ),
                          # Alpha
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'datasource_id': 'Alpha', 'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'Acetate'},
                           AttributeMapping(attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
                                            basin3d_vocab='ACT:WATER',
                                            basin3d_desc=[ObservedProperty(basin3d_vocab='ACT', full_name='Acetate (CH3COO)', categories=['Biogeochemistry', 'Anions'], units='mM'),
                                                          SamplingMediumEnum.WATER],
                                            datasource_vocab='Acetate',
                                            datasource_desc='acetate',
                                            datasource=DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/', credentials={}))
                           ),
                          # Bad-DataSource
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'datasource_id': 'FOO', 'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'Acetate'},
                           AttributeMapping(attr_type='OBSERVED_PROPERTY',
                                            basin3d_vocab='NOT_SUPPORTED',
                                            basin3d_desc=[],
                                            datasource_vocab='Acetate',
                                            datasource_desc='No datasource was found for id "FOO".',
                                            datasource=DataSource())
                           )
                          ],
                         ids=['Wrong-plugin-initialized', 'USGS', 'Alpha', 'Bad-DataSource'])
def test_find_attribute_mapping(plugins, query, expected):
    """ Test attribute mapping """
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in plugins])

    attribute_mapping = catalog.find_attribute_mapping(**query)
    assert attribute_mapping == expected


# catalog.find_attribute_mappings (TinyDB)
@pytest.mark.parametrize('plugins, query, expected_count, expected_list, expected_msg',
                         # ds_id-attr_type-attr_vocab-from_basin3d--USGS-OBSERVED_PROPERTY-Hg--compound
                         [
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'datasource_id': 'USGS', 'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'Hg', 'from_basin3d': True}, 1,
                           [AttributeMapping(attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
                                             basin3d_vocab='Hg:WATER',
                                             basin3d_desc=[ObservedProperty(basin3d_vocab='Hg', full_name='Mercury (Hg)', categories=['Biogeochemistry', 'Trace elements'], units='mg/L'),
                                                           SamplingMediumEnum.WATER],
                                             datasource_vocab='50287',
                                             datasource_desc='Mercury, water, filtered, nanograms per liter',
                                             datasource=DataSource(id='USGS', name='USGS', id_prefix='USGS', location='https://waterservices.usgs.gov/nwis/', credentials={}))],
                           []),
                          # datasource_id--Alpha
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Alpha'}, 13, [], []),
                          # no-params
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {}, 66, [], []),
                          # ds_id-attr_type-Alpha-STATISTIC
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Alpha', 'attr_type': 'STATISTIC'}, 3, [], []),
                          # ds_id-attr_type-attr_vocab--Alpha-STATISTIC-mean
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Alpha', 'attr_type': 'STATISTIC', 'attr_vocab': 'mean'}, 1, [], []),
                          # ds_id-attr_type_attr_vocab-from_basin3d--Alpha-STATISTIC-MEAN
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Alpha', 'attr_type': 'STATISTIC', 'attr_vocab': 'MEAN', 'from_basin3d': True}, 1, [], []),
                          # ds_id-attr_vocab--Alpha-mean
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Alpha', 'attr_vocab': 'mean'}, 1,
                           [AttributeMapping(attr_type='STATISTIC', basin3d_vocab='MEAN', datasource_vocab='mean',
                                             datasource_desc='', basin3d_desc=[StatisticEnum.MEAN],
                                             datasource=DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/', credentials={}))],
                           []),
                          # ds_id-attr_vocab-from_basin3d--Alpha-MEAN
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Alpha', 'attr_vocab': 'MEAN', 'from_basin3d': True}, 1,
                           [AttributeMapping(attr_type='STATISTIC', basin3d_vocab='MEAN', datasource_vocab='mean',
                                             datasource_desc='', basin3d_desc=[StatisticEnum.MEAN],
                                             datasource=DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/', credentials={}))],
                           []),
                          # attr_type_attr_vocab--STATISTIC-mean
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_type': 'STATISTIC', 'attr_vocab': 'mean'}, 1,
                           [AttributeMapping(attr_type='STATISTIC', basin3d_vocab='MEAN', datasource_vocab='mean',
                                             datasource_desc='', basin3d_desc=[StatisticEnum.MEAN],
                                             datasource=DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/', credentials={}))],
                           []),
                          # attr_type_attr_vocab-from_basin3d--STATISTIC-MEAN
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_type': 'STATISTIC', 'attr_vocab': 'MEAN', 'from_basin3d': True}, 2,
                           [AttributeMapping(attr_type='STATISTIC', basin3d_vocab='MEAN', datasource_vocab='mean',
                                             datasource_desc='', basin3d_desc=[StatisticEnum.MEAN],
                                             datasource=DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/', credentials={})),
                            AttributeMapping(attr_type='STATISTIC', basin3d_vocab='MEAN', datasource_vocab='00003',
                                             datasource_desc='', basin3d_desc=[StatisticEnum.MEAN],
                                             datasource=DataSource(id='USGS', name='USGS', id_prefix='USGS', location='https://waterservices.usgs.gov/nwis/', credentials={}))],
                           []),
                          # attr_type--AGGREGATION_TYPE
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_type': 'AGGREGATION_DURATION'}, 3, [], []),
                          # attr_vocab--Aluminum
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_vocab': 'Aluminum'}, 1, [], []),
                          # attr_vocab-from_basin3d--Al
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_vocab': 'Al', 'from_basin3d': True}, 3, [], []),
                          # attr_vocabs--Aluminum,Acetate
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_vocab': ['Aluminum', 'Acetate']}, 2, [], []),
                          # attr_vocabs-from_basin3d--ACT,Al
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_vocab': ['Al', 'ACT'], 'from_basin3d': True}, 4, [], []),
                          # attr_vocabs-some_bad--Aluminum,Acetate,Foo
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_vocab': ['Aluminum', 'Acetate', 'Foo']}, 2, [],
                           ['No attribute mappings found for the following datasource vocabularies: Foo. Note: specified datasource id = None and attribute type = None']),
                          # BAD-ds_id
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Foo'}, -1, [], []),
                          # BAD-attr_type
                          ([alpha.AlphaSourcePlugin], {'attr_type': 'BAD_ATTR_TYPE'}, -1, [], []),
                          # BAD-attr_vocab_type
                          ([alpha.AlphaSourcePlugin], {'attr_vocab': {'foo': 'foo'}}, -1, [], []),
                          # No-results
                          ([alpha.AlphaSourcePlugin], {'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'Hg'}, 0, [],
                           ['No attribute mappings found for specified parameters: datasource id = "None", attribute type = "OBSERVED_PROPERTY", datasource vocabularies: Hg.']),
                          # No-results-from_basin3d
                          ([alpha.AlphaSourcePlugin], {'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'Hg', 'from_basin3d': True}, 0, [],
                           ['No attribute mappings found for specified parameters: datasource id = "None", attribute type = "OBSERVED_PROPERTY", BASIN-3D vocabularies: Hg.'])
                         ],
                         ids=['USGS-from_basin3d', 'datasource_id-only-Alpha-all', 'no-params-ALL', 'ds_id-attr_type-Alpha-STATISTIC',
                              'ds_id-attr_type-attr_vocab--Alpha-STATISTIC-mean', 'ds_id-attr_type_attr_vocab-from_basin3d--Alpha-STATISTIC-MEAN',
                              'ds_id-attr_vocab--Alpha-mean', 'ds_id-attr_vocab-from_basin3d--Alpha-MEAN', 'attr_type_attr_vocab--STATISTIC-mean',
                              'attr_type_attr_vocab-from_basin3d--STATISTIC-MEAN', 'attr_type--AGGREGATION_TYPE', 'attr_vocab--Aluminum',
                              'attr_vocab-from_basin3d--Al', 'attr_vocabs--Aluminum-Acetate', 'attr_vocabs-from_basin3d--ACT-Al',
                              'attr_vocabs-some_bad--Aluminum-Acetate-Foo', 'BAD-ds_id', 'BAD-attr_type', 'BAD-attr_vocab_type', 'No-results', 'No-results-from_basin3d'])
def test_find_attribute_mappings(caplog, plugins, query, expected_count, expected_list, expected_msg):
    """ Test attribute mapping """
    caplog.set_level(logging.INFO)

    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in plugins])
    caplog.clear()

    attribute_mappings = catalog.find_attribute_mappings(**query)

    actual_list = []
    if expected_count >= 0:
        count = 0
        for attr_mapping in attribute_mappings:
            count += 1
            if expected_list:
                actual_list.append(attr_mapping)
        assert count == expected_count
        if expected_list:
            for expected_attr_mapping in expected_list:
                assert expected_attr_mapping in actual_list
        if expected_msg:
            log_msgs = [rec.message for rec in caplog.records]
            for msg in expected_msg:
                assert msg in log_msgs
    else:
        with pytest.raises(CatalogException):
            for attr_mapping in attribute_mappings:
                pass


# catalog.find_datasource_vocab (TinyDB)
# ToDo: add test for triple compound mapping
@pytest.mark.parametrize(
    'attr_type, basin3d_vocab, basin3d_query, expected_results, expected_msgs',
    # non-compound
    [('statistic', 'MEAN', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['ACT'], start_date='2020-01-01', statistic=['MEAN']),
      ['mean'], []),
     # non-compound-no-match
     ('statistic', 'ESTIMATED', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['ACT'], start_date='2020-01-01', statistic=['MEAN']),
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type "STATISTIC" and BASIN-3D vocab: ESTIMATED.']),
     # compound-simple_query
     ('observed_property_variables', 'ACT', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['ACT'], start_date='2020-01-01'),
      ['Acetate'], []),
     # compound-simple_query-multimap
     ('observed_property_variables', 'Al', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['Al'], start_date='2020-01-01'),
      ['Al', 'Aluminum'], []),
     # compound-compound_query-multimap
     ('observed_property_variables', 'Al', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['Al'], start_date='2020-01-01', sampling_medium=['WATER']),
      ['Al', 'Aluminum'], []),
     # compound-compound_query
     ('observed_property_variables', 'Ag', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['Ag'], start_date='2020-01-01', sampling_medium=['WATER']),
      ['Ag'], []),
     # compound-compound_query-no_compound_match
     ('observed_property_variables', 'Al', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['Al'], start_date='2020-01-01', sampling_medium=['GAS']),
      ['NOT_SUPPORTED'], ['Datasource "Alpha" did not have matches for attr_type "OBSERVED_PROPERTY:SAMPLING_MEDIUM" and BASIN-3D vocab: Al:GAS.']),
     # compound-compound_query_lists
     ('observed_property_variables', 'Ag', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['Ag', 'Al'], start_date='2020-01-01', sampling_medium=['WATER', 'GAS']),
      ['Ag', 'Ag_gas'], []),
     # compound-compound_query_no_match
     ('observed_property_variables', 'Hg', QueryMeasurementTimeseriesTVP(monitoring_features=['A-1'], observed_property_variables=['Ag', 'Al', 'Hg'], start_date='2020-01-01'),
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


@pytest.mark.parametrize('attr_type, expected_result',
                         [('OBSERVED_PROPERTY', 'OBSERVED_PROPERTY'),
                          ('OBSERVED_PROPERTY_VARIABLE', 'OBSERVED_PROPERTY'),
                          ('OBSERVED_PROPERTY_VARIABLES', 'OBSERVED_PROPERTY'),
                          ('foo', 'FOO')
                          ],
                         ids=['OP', 'OPV', 'OPVs', 'foo'])
def test_verify_attr_type(attr_type, expected_result):
    from basin3d.core.catalog import verify_attr_type
    assert verify_attr_type(attr_type) == expected_result


@pytest.mark.parametrize('query_var, is_query, expected_result',
                         [('OBSERVED_PROPERTY', [False], 'observed_property_variable'),
                          ('OBSERVED_PROPERTY', [True], 'observed_property_variables'),
                          ('FOO', [], 'foo'),
                          ('foo', [], 'foo')
                          ],
                         ids=['OP-False', 'OP-True', 'FOO', 'foo'])
def test_verify_query_var(query_var, is_query, expected_result):
    from basin3d.core.catalog import verify_query_param
    if is_query:
        assert verify_query_param(query_var, is_query[0]) == expected_result
    else:
        assert verify_query_param(query_var) == expected_result


# catalog.find_compound_mapping_attributes (TinyDB)
@pytest.mark.parametrize('datasource_id, attr_type, include_specified_type, expected_results',
                         [('Alpha', 'OBSERVED_PROPERTY', False, ['SAMPLING_MEDIUM']),
                          ('Alpha', 'OBSERVED_PROPERTY', True, ['SAMPLING_MEDIUM', 'OBSERVED_PROPERTY']),
                          ('Alpha', 'FOO', False, []),
                          ],
                         ids=['compound-other', 'compound-both', 'non-compound'])
def test_find_compound_mapping_attributes(datasource_id, attr_type, include_specified_type, expected_results):
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in [alpha.AlphaSourcePlugin]])

    if include_specified_type:
        results = catalog.find_compound_mapping_attributes(datasource_id, attr_type, include_specified_type)
    else:
        results = catalog.find_compound_mapping_attributes(datasource_id, attr_type)
    assert sorted(results) == sorted(expected_results)
