import logging

import pytest
from tinydb import TinyDB

from basin3d.core.models import DataSource, ObservedProperty, AttributeMapping
from basin3d.core.schema.enum import SamplingMediumEnum
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

# ToDo: add following tests thinking about order
# CompoundMapping model (catalogBASE)
# _get_attribute_enum (catalogBASE)
# catalog.find_observed_property (TinyDB)
# catalog.find_attribute_mappings (TinyDB)
# catalog.find_datasource_vocab (TinyDB)
# catalog.find_compound_mapping_attributes (TinyDB)
#
# ToDo: Think about outcomes of logic
# ToDo: see if can resolve datasource vs datasource_id arguments

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


def test_process_plugin_attr_mapping(catalog):
    """Test a bad mapping file with the wrong header"""
    from tests.testplugins import alpha
    from basin3d.core.catalog import CatalogException
    with pytest.raises(CatalogException):
        catalog._process_plugin_attr_mapping(
            alpha.AlphaSourcePlugin(catalog), filename='mapping_alpha_wrong_header.csv', datasource=DataSource())


def test_get_observed_properties():
    """Test that all of the observed property variable are returned"""
    from tests.testplugins import alpha
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    plugins = [alpha.AlphaSourcePlugin(catalog)]
    catalog.initialize(plugins)
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


@pytest.mark.parametrize("plugins, query, datasource_id, expected",
                         # Wrong-plugin-inititalized
                         [([usgs.USGSDataSourcePlugin],
                           {'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'ACT'},
                           'Alpha',
                           AttributeMapping(
                               attr_type='OBSERVED_PROPERTY',
                               basin3d_vocab='NOT_SUPPORTED',
                               basin3d_desc=[],
                               datasource_vocab='ACT',
                               datasource_desc='no mapping was found for "ACT" in Alpha datasource',
                               datasource=DataSource(id='Alpha', name='Alpha', id_prefix='A',
                                                     location='test.location', credentials={}))
                           ),
                          # USGS
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': '00095'},
                           'USGS',
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
                          # USGS-from_basin3d
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'Hg', 'from_basin3d': True},
                           'USGS',
                           AttributeMapping(attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
                                            basin3d_vocab='Hg:WATER',
                                            basin3d_desc=[ObservedProperty(basin3d_vocab='Hg', full_name='Mercury (Hg)',
                                                                           categories=['Biogeochemistry', 'Trace elements'], units='mg/L'),
                                                          SamplingMediumEnum.WATER],
                                            datasource_vocab='50287',
                                            datasource_desc='Mercury, water, filtered, nanograms per liter',
                                            datasource=DataSource(id='USGS', name='USGS', id_prefix='USGS',
                                                                  location='https://waterservices.usgs.gov/nwis/', credentials={}))
                           ),
                          # Alpha-plus
                          ([usgs.USGSDataSourcePlugin, alpha.AlphaSourcePlugin],
                           {'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'Acetate'},
                           'Alpha',
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
                           {'attr_type': 'OBSERVED_PROPERTY', 'attr_vocab': 'ACT', 'from_basin3d': True},
                           'FOO',
                           AttributeMapping(attr_type='OBSERVED_PROPERTY',
                                            basin3d_vocab='ACT',
                                            basin3d_desc=[],
                                            datasource_vocab='NOT_SUPPORTED',
                                            datasource_desc='no mapping was found for "NOT_SUPPORTED" in FOO datasource',
                                            datasource=DataSource(id='FOO', name='FOO', id_prefix='FOO', location='test.location', credentials={}))
                           )
                          ],
                         ids=['Wrong-plugin-initialized', 'USGS', 'USGS-from_basin3d', 'Alpha', 'Bad-DataSource'])
def test_attribute_mapping(plugins, query, datasource_id, expected):
    """ Test observed property """
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in plugins])
    datasource = get_datasource_from_id(id=datasource_id, id_prefix='A' if datasource_id == 'Alpha' else datasource_id)
    query.update({'datasource': datasource})

    attribute_mapping = catalog.find_attribute_mapping(**query)
    # if expected is None:
    #     assert attribute_mapping == expected
    # else:
    assert attribute_mapping == expected
