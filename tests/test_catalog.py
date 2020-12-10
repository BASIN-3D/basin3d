import logging

import pytest
from tinydb import TinyDB

from basin3d.core.models import DataSource, ObservedPropertyVariable
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


# Test with different variable files.
# Last test uses the default basin3d hydrology variables which is needed for rest of tests.
def test_gen_basin3d_variable_store(catalog, caplog):
    caplog.set_level(logging.INFO, logger=__name__)
    catalog.variable_dir = 'tests.resources'

    caplog.clear()
    catalog.variable_filename = 'basin3d_variables_duplicate.csv'
    catalog._gen_variable_store()
    assert catalog._get_observed_property_variable('Br').to_dict() == ObservedPropertyVariable(
        basin3d_id='Br', full_name='Bromide (Br)',
        categories=['Biogeochemistry', 'Anions'], units='mM').to_dict()
    log_msgs = [rec.message for rec in caplog.records]
    assert 'Duplicate BASIN-3D variable Br found. Skipping duplicate.' in log_msgs

    catalog.variable_filename = 'basin3d_variables_wrong_header.csv'
    from basin3d.core.catalog import CatalogException
    with pytest.raises(CatalogException):
        catalog._gen_variable_store()

    # KEEP this test last: it resets the variable file.
    catalog.variable_filename = 'basin3d_variables_hydrology.csv'
    catalog.variable_dir = 'basin3d.data'
    catalog._gen_variable_store()
    assert catalog._get_observed_property_variable('ACT') == ObservedPropertyVariable(
        basin3d_id='ACT', full_name='Acetate (CH3COO)',
        categories=['Biogeochemistry', 'Anions'], units='mM')
    assert catalog._get_observed_property_variable('heya') is None


def test_process_plugin_variable_mapping(catalog):
    """Test a bad mapping file with the wrong header"""
    from tests.testplugins import alpha
    from basin3d.core.catalog import CatalogException
    with pytest.raises(CatalogException):
        catalog._process_plugin_variable_mapping(
            alpha.AlphaSourcePlugin(catalog), map_filename='mapping_alpha_wrong_header.csv', datasource=DataSource())


def test_get_observed_property_variables():
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
            'TSS', 'TRB', 'STO_RES', 'LAI', 'PLT_HT', 'PAI', 'PFT', 'SAP', 'RGB', 'GCC', 'NDVI'] == [i.basin3d_id for
                                                                                                     i in
                                                                                                     catalog.find_observed_property_variables()]

    # This test creates a catalog
    def test_create_catalog():
        from tests.testplugins import alpha
        from basin3d.core.catalog import CatalogTinyDb
        catalog = CatalogTinyDb()
        plugins = [alpha.AlphaSourcePlugin(catalog)]
        catalog.initialize(plugins)
        results = [{'basin3d_id': 'ACT',
                    'datasource': {'credentials': {},
                                   'id': 'Alpha',
                                   'id_prefix': 'A',
                                   'location': 'https://asource.foo/',
                                   'name': ''},
                    'datasource_id': 'Alpha',
                    'datasource_variable_id': 'Acetate',
                    'observed_property': {'datasource': {'credentials': {},
                                                         'id': 'Alpha',
                                                         'id_prefix': 'A',
                                                         'location': 'https://asource.foo/',
                                                         'name': 'Alpha'},
                                          'datasource_description': '',
                                          'datasource_variable': 'Acetate',
                                          'observed_property_variable': {'basin3d_id': 'ACT',
                                                                         'categories': ['Biogeochemistry',
                                                                                        'Anions'],
                                                                         'full_name': 'Acetate '
                                                                                      '(CH3COO)',
                                                                         'units': 'mM'},
                                          'sampling_medium': 'WATER'},
                    'observed_property_variable': {'basin3d_id': 'ACT',
                                                   'categories': ['Biogeochemistry', 'Anions'],
                                                   'full_name': 'Acetate (CH3COO)',
                                                   'units': 'mM'}},
                   {'basin3d_id': 'Ag',
                    'datasource': {'credentials': {},
                                   'id': 'Alpha',
                                   'id_prefix': 'A',
                                   'location': 'https://asource.foo/',
                                   'name': ''},
                    'datasource_id': 'Alpha',
                    'datasource_variable_id': 'Ag',
                    'observed_property': {'datasource': {'credentials': {},
                                                         'id': 'Alpha',
                                                         'id_prefix': 'A',
                                                         'location': 'https://asource.foo/',
                                                         'name': 'Alpha'},
                                          'datasource_description': '',
                                          'datasource_variable': 'Ag',
                                          'observed_property_variable': {'basin3d_id': 'Ag',
                                                                         'categories': ['Biogeochemistry',
                                                                                        'Trace '
                                                                                        'elements'],
                                                                         'full_name': 'Silver '
                                                                                      '(Ag)',
                                                                         'units': 'mg/L'},
                                          'sampling_medium': 'WATER'},
                    'observed_property_variable': {'basin3d_id': 'Ag',
                                                   'categories': ['Biogeochemistry',
                                                                  'Trace elements'],
                                                   'full_name': 'Silver (Ag)',
                                                   'units': 'mg/L'}},
                   {'datasource_id': 'Alpha', 'datasource': {},
                    'datasource_variable_id': 'Aluminum', 'observed_property': {},
                    'basin3d_id': 'Al', 'observed_property_variable': {}}]
        for item in catalog.in_memory_db:
            idx = item.doc_id - 1
            assert results[idx] == item


@pytest.mark.parametrize("plugins, query, expected",
                         [([usgs.USGSDataSourcePlugin],
                           {'variable_name': 'ACT', "datasource_id": 'Alpha'}, None),
                          ([usgs.USGSDataSourcePlugin,
                            alpha.AlphaSourcePlugin],
                           {"datasource_id": 'USGS', 'variable_name': 'Hg'}, {'datasource': {'credentials': {},
                                                                                             'id': 'USGS',
                                                                                             'id_prefix': 'USGS',
                                                                                             'location': 'https://waterservices.usgs.gov/nwis/',
                                                                                             'name': 'USGS'},
                                                                              'datasource_description': 'Mercury, water, filtered, nanograms per liter',
                                                                              'datasource_variable': '50287',
                                                                              'observed_property_variable': {
                                                                                  'basin3d_id': 'Hg',
                                                                                  'categories': ['Biogeochemistry',
                                                                                                 'Trace elements'],
                                                                                  'full_name': 'Mercury (Hg)',
                                                                                  'units': 'mg/L'},
                                                                              'sampling_medium': 'WATER'}),
                          ([usgs.USGSDataSourcePlugin,
                            alpha.AlphaSourcePlugin],
                           {"datasource_id": 'Alpha', 'variable_name': 'ACT'}, {'datasource': {'credentials': {},
                                                                                               'id': 'Alpha',
                                                                                               'id_prefix': 'A',
                                                                                               'location': 'https://asource.foo/',
                                                                                               'name': 'Alpha'},
                                                                                'datasource_description': '',
                                                                                'datasource_variable': 'Acetate',
                                                                                'observed_property_variable': {
                                                                                    'basin3d_id': 'ACT',
                                                                                    'categories': ['Biogeochemistry',
                                                                                                   'Anions'],
                                                                                    'full_name': 'Acetate (CH3COO)',
                                                                                    'units': 'mM'},
                                                                                'sampling_medium': 'WATER'}),
                          ([usgs.USGSDataSourcePlugin,
                            alpha.AlphaSourcePlugin], {"datasource_id": 'FOO', 'variable_name': 'ACT'},
                           None)
                          ],
                         ids=['Wrong-Alpha', 'USGS-plus', 'Alpha-plus', 'Bad-DataSource'])
def test_observed_property(plugins, query, expected):
    """ Test observed property """
    from basin3d.core.catalog import CatalogTinyDb
    catalog = CatalogTinyDb()
    catalog.initialize([p(catalog) for p in plugins])

    observed_property = catalog.find_observed_property(**query)
    if expected is None:
        assert observed_property == expected
    else:
        assert observed_property.to_dict() == expected
