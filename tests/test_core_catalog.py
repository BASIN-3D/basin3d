import logging
from unittest.mock import Mock

import pytest
from tinydb import TinyDB

from basin3d.core.catalog import CatalogException
from basin3d.core.models import DataSource, ObservedProperty, AttributeMapping
from basin3d.core.schema.enum import SamplingMediumEnum, StatisticEnum
from tests.testplugins import alpha, complexmap
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

# _get_attribute_enum (catalogBASE)
def test_get_attribute_enum():
    from basin3d.core.catalog import CatalogBase
    catalog = CatalogBase()
    assert catalog._get_attribute_enum('WATER', SamplingMediumEnum) == SamplingMediumEnum.WATER
    assert catalog._get_attribute_enum('FOO', SamplingMediumEnum) is None


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
    catalog.variable_filename = 'basin3d_observed_property_vocabulary.csv'
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
        new_catalog.find_datasource_attribute_mapping('Foo', 'FOO', 'Foo')
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
        test_gen = new_catalog.find_attribute_mappings()
        for attr_map in test_gen:
            pass
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Attribute Store has not been initialized.' in log_msgs


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
                           {'datasource_id': 'Alpha', 'attr_type': 'OBSERVED_PROPERTY', 'datasource_vocab': 'ACT'},
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
                           {'datasource_id': 'USGS', 'attr_type': 'OBSERVED_PROPERTY', 'datasource_vocab': '00095'},
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
                           {'datasource_id': 'Alpha', 'attr_type': 'OBSERVED_PROPERTY', 'datasource_vocab': 'Acetate'},
                           AttributeMapping(attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
                                            basin3d_vocab='ACT:WATER',
                                            basin3d_desc=[ObservedProperty(basin3d_vocab='ACT', full_name='Acetate (CH3COO)', categories=['Biogeochemistry', 'Anions'], units='mM'),
                                                          SamplingMediumEnum.WATER],
                                            datasource_vocab='Acetate',
                                            datasource_desc='acetate',
                                            datasource=DataSource(id='Alpha', name='Alpha', id_prefix='A', location='https://asource.foo/', credentials={}))
                           ),
                          # Complexmap
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin, complexmap.ComplexmapSourcePlugin],
                           {'datasource_id': 'Complexmap', 'attr_type': 'OBSERVED_PROPERTY', 'datasource_vocab': 'Mean Al'},
                           AttributeMapping(attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM:STATISTIC',
                                            basin3d_vocab='Al:WATER:MEAN',
                                            basin3d_desc=[ObservedProperty(basin3d_vocab='Al', full_name='Aluminum (Al)', categories=['Biogeochemistry', 'Trace elements'], units='mg/L'),
                                                          SamplingMediumEnum.WATER, StatisticEnum.MEAN],
                                            datasource_vocab='Mean Al',
                                            datasource_desc='aluminum (Al) concentration in water',
                                            datasource=DataSource(id='Complexmap', name='Complexmap', id_prefix='C', location='https://asource.foo/', credentials={}))
                           ),
                          # Bad-DataSource
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'datasource_id': 'FOO', 'attr_type': 'OBSERVED_PROPERTY', 'datasource_vocab': 'Acetate'},
                           AttributeMapping(attr_type='OBSERVED_PROPERTY',
                                            basin3d_vocab='NOT_SUPPORTED',
                                            basin3d_desc=[],
                                            datasource_vocab='Acetate',
                                            datasource_desc='No datasource was found for id "FOO".',
                                            datasource=DataSource())
                           )
                          ],
                         ids=['Wrong-plugin-initialized', 'USGS', 'Alpha', 'Complexmap', 'Bad-DataSource'])
def test_find_datasource_attribute_mapping(plugins, query, expected):
    """ Test attribute mapping """
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in plugins])

    attribute_mapping = catalog.find_datasource_attribute_mapping(**query)
    assert attribute_mapping == expected


# currently these are the implemented uses:
# -- BASIN-3D vocab to datasource vocab:
#    -- fully specified: e.g. Al:WATER:MAX
#    -- with wildcard: e.g. one or more attribute is specified but not all: Al:WATER:.*
#    -- partially specified: e.g. OBSERVED_PROPERTY = Al for compound vocabs like Al:WATER:MAX (note partial specification may not be beginning text)
#    -- could be a list or a single BASIN-3D vocab. Note: Result is ALWAYS a list
# -- datasource vocab to BASIN-3D
#    -- single datasource vocab (with attr_type specified) with expected single BASIN-3D
# -- all mappings: to find complex mappings
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
                          # datasource_id-only-Alpha-all
                          ([alpha.AlphaSourcePlugin], {'datasource_id': 'Alpha'}, 14, [], []),
                          # no-params
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {}, 67, [], []),
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
                          # ds_id-attr_type_attr_vocab-from_basin3d-similar-vocab--USGS-RESULT_QUALITY
                          ([usgs.USGSDataSourcePlugin], {'datasource_id': 'USGS', 'attr_type': 'RESULT_QUALITY', 'attr_vocab': ['ESTIMATED', 'VALIDATED'], 'from_basin3d': True}, 3, [], []),
                          # attr_type--AGGREGATION_TYPE
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin], {'attr_type': 'AGGREGATION_DURATION'}, 4, [], []),
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
                           ['No attribute mappings found for specified parameters: datasource id = "None", attribute type = "OBSERVED_PROPERTY", BASIN-3D vocabularies: Hg.']),
                          # complexmap-attr_vocabs-from_basin3d--ACT,Al
                          ([complexmap.ComplexmapSourcePlugin], {'attr_vocab': ['Al', 'ACT'], 'from_basin3d': True}, 5, [], []),
                          # complexmap-attr_vocabs-wildcard-from_basin3d--Al
                          ([complexmap.ComplexmapSourcePlugin], {'attr_vocab': ['Al:.*:.*'], 'from_basin3d': True}, 3, [], []),
                          # complexmap-attr_vocab-Al
                          ([complexmap.ComplexmapSourcePlugin], {'attr_vocab': ['Mean Al']}, 1, [], []),
                          # complexmap-attr_vocab-bothAl
                          ([complexmap.ComplexmapSourcePlugin], {'attr_vocab': ['Mean Al', 'Mean Aluminum']}, 2, [], []),
                          # complexmap-attr_vocabs-single-wildcard-from_basin3d--Al
                          ([complexmap.ComplexmapSourcePlugin], {'attr_vocab': ['Al:.*:MEAN', 'Al:.*:MAX'], 'from_basin3d': True}, 2, [], []),
                          # complexmap-attr_type-attr_vocabs-from_basin3d--MEAN, MAX
                          ([complexmap.ComplexmapSourcePlugin], {'attr_type': 'STATISTIC', 'attr_vocab': ['MEAN', 'MAX'], 'from_basin3d': True}, 5, [], []),
                         ],
                         ids=['USGS-from_basin3d', 'datasource_id-only-Alpha-all', 'no-params-ALL', 'ds_id-attr_type-Alpha-STATISTIC',
                              'ds_id-attr_type-attr_vocab--Alpha-STATISTIC-mean', 'ds_id-attr_type_attr_vocab-from_basin3d--Alpha-STATISTIC-MEAN',
                              'ds_id-attr_vocab--Alpha-mean', 'ds_id-attr_vocab-from_basin3d--Alpha-MEAN', 'attr_type_attr_vocab--STATISTIC-mean',
                              'attr_type_attr_vocab-from_basin3d--STATISTIC-MEAN', 'ds_id-attr_type_attr_vocab-from_basin3d-similar-vocab', 'attr_type--AGGREGATION_TYPE', 'attr_vocab--Aluminum',
                              'attr_vocab-from_basin3d--Al', 'attr_vocabs--Aluminum-Acetate', 'attr_vocabs-from_basin3d--ACT-Al',
                              'attr_vocabs-some_bad--Aluminum-Acetate-Foo', 'BAD-ds_id', 'BAD-attr_type', 'BAD-attr_vocab_type', 'No-results', 'No-results-from_basin3d',
                              'complexmap-attr_vocabs-from_basin3d', 'complexmap-attr_vocabs-wildcard-from_basin3d', 'complexmap-attr_vocab-Al', 'complexmap-attr_vocab-bothAl',
                              'complexmap-attr_vocabs-single-wildcard-from_basin3d', 'complexmap-attr_type-attr_vocabs-from_basin3d'])
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
