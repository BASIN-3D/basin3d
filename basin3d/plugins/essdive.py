from enum import Enum
from dataclasses import dataclass
from datetime import date
import os
import pandas as pd
import pathlib
from typing import Dict, List, Optional, Iterator, Union, Tuple

from basin3d import monitor
from basin3d.core.schema.enum import FeatureTypeEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, DepthCoordinate, \
    GeographicCoordinate, HorizontalCoordinate, RepresentativeCoordinate, \
    MeasurementTimeseriesTVPObservation, MonitoringFeature, \
    TimeMetadataMixin, TimeValuePair, ResultListTVP
from basin3d.core.plugin import DataSourcePluginAccess, DataSourcePluginPoint, basin3d_plugin
from basin3d.core.types import SpatialSamplingShapes

logger = monitor.get_logger(__name__)

ESSDIVE_DATASETS_PATH = os.environ.get('ESSDIVE_DATASETS_PATH', None)


# ToDo: This should not be an enum class, rework in future.
class HydroRFTerms(Enum):
    # ESS-DIVE Hydrologic Monitoring Reporting Format
    # https://github.com/ess-dive-community/essdive-hydrologic-monitoring

    lat = 'Latitude'  # Required
    long = 'Longitude'  # Required
    depth = 'Depth'
    sensor_depth = 'Sensor_Depth'
    depth_reference = 'Depth_Reference'  # Look for depth units
    elevation = 'Elevation'
    sensor_elevation = 'Sensor_Elevation'
    elevation_reference = 'Elevation_Reference'  # Look for reference and datum
    loc_id = 'Site_ID'
    site_name = 'Site_Name'
    water_name = 'Water_Name'
    sensor_id = 'Sensor_ID'  # Not officially part of the terminology but is allowed in the HydroRF_Instructions.md
    loc_type = 'Site_Type'  # Search for river = HORIZONTAL_PATH, lake | groundwater = WATER_BODY, ecosystem = ECOSYSTEM, well = POINT
    utc_offset = 'UTC_Offset'
    install_method_id = 'InstallationMethod_ID'
    install_method_desc = 'InstallationMethod_Description'

    # time variables
    date_time = 'DateTime'
    date_time_start = 'DateTime_Start'
    date_time_end = 'DateTime_End'
    date_time_vars = [date_time, date_time_start, date_time_end]

    # observation_terms and units -- variables commented out are not supported yet.
    variables = {'Depth_To_Water': {'rf_options': ['meters', 'centimeters'], 'convert': 'to_m'},  # m
                 'Gage_Height': {'rf_options': ['meters', 'centimeters'], 'convert': 'to_m'},  # m
                 # 'Sensor_Depth': {'rf_options': ['meters', 'centimeters'], 'convert': 'to_m'},  # m
                 # 'Sensor_Elevation': {'rf_options': ['meters_above_mean_sea_level_NAVD88'], 'convert': None},
                 # 'Sensor_Pressure_Vented': {'rf_options': ['kilopascals', 'meters_of_H2O'], 'convert': 'tbd'},
                 # 'Sensor_Pressure_Unvented': {'rf_options': ['kilopascals', 'meters_of_H2O'], 'convert': 'tbd'},
                 'Water_Depth': {'rf_options': ['meters', 'centimeters'], 'convert': 'to_m'},  # m
                 'Water_Surface_Elevation': {'rf_options': ['meters_above_mean_sea_level_NAVD88'], 'convert': None},
                 'Electrical_Conductivity': {'rf_options': ['microsiemens_per_centimeter', 'millisiemens_per_centimeter'], 'convert': 'to_micro'},  # uS/cm
                 'Specific_Conductance': {'rf_options': ['microsiemens_per_centimeter', 'millisiemens_per_centimeter'], 'convert': 'to_micro'},  # uS/cm
                 'pH': {'rf_options': ['pH'], 'convert': None},
                 'Dissolved_Oxygen': {'rf_options': ['milligrams_per_liter'], 'convert': None},
                 # 'Dissolved_Oxygen_Saturation': {'rf_options': ['percent_saturation'], 'convert': None},
                 'Water_Temperature': {'rf_options': ['degree_celsius'], 'convert': None}}

    # location terms
    location_terms = [lat, long, depth, depth_reference, elevation, elevation_reference, loc_id,
                      site_name, water_name, sensor_id, loc_type, utc_offset, install_method_id]
    loc_numeric_type = [lat, long, depth, elevation]

    # unique location terms
    # following approach in which a unique location is at the surface for the monitoring_feature (mf),
    #    adding depths / elevation to mf desc when the information is easily accessible (i.e., in the locations files)
    # mf objects associated with measurement objects will have a singular depth
    unique_loc_terms = [loc_id]

    # equivalent terms -- note elevation is defined as sensor only
    loc_names = [site_name, water_name]

    # header info
    header_row_char = '#'
    format_row_text = '# HeaderRows_Format:'
    header_delimiter = ';'
    column_header = 'Column_Header'
    units = 'Unit'

    missing_value_str = 'N/A'
    missing_value_numeric = -9999

    @classmethod
    def get_attr_name(cls, target_value: str) -> Optional[str]:
        for a_var, a_value in cls.__members__.items():
            if target_value == a_value.value:
                return a_var
        return None


@dataclass
class locInfo:
    ds_id: str
    ds_pid: str
    loc_id: str
    lat: Optional[float] = None
    long: Optional[float] = None
    depth: Optional[List[float]] = None
    sensor_depth: Optional[float] = None
    depth_reference: Optional[str] = None
    elevation: Optional[List[float]] = None
    sensor_elevation: Optional[float] = None
    elevation_reference: Optional[str] = None
    site_name: Optional[str] = None
    water_name: Optional[str] = None
    sensor_id: Optional[str] = None
    utc_offset: Optional[int] = None
    install_method_id: Optional[str] = None
    install_method_desc: Optional[str] = None
    loc_type: str = 'POINT'  # Future Search for river = HORIZONTAL_PATH, lake | groundwater = WATER_BODY, ecosystem = ECOSYSTEM, well = POINT


class UnitHandler:
    """ Class to handle specific unit conversions for the Hydrological Monitoring RF"""

    def __init__(self):
        self.fuzzy_match_store = {
            'centimeters': ['cm', 'centimeter'],
            'meters': ['m', 'meters'],
            'microsiemens_per_centimeter': ['uS/cm', 'uS / cm'],
            'millisiemens_per_centimeter': ['mS/cm', 'mS / cm'],
            'degree_celsius': ['deg C', 'deg_C', 'C'],
            'kilopascals': ['kPa'],
            'percent_saturation': ['%', 'percent'],
            'meters_above_mean_sea_level_NAVD88': ['meters', 'm', 'meter']
        }

    @staticmethod
    def cm_to_m(unit: str) -> float:
        if unit == "centimeters":
            return 0.01
        return 1

    @staticmethod
    def milli_to_micro(unit: str) -> int:
        if 'milli' in unit:
            return 1000
        return 1

    # ToDo: handle conversion between kPa and meters H2O depending on tbd BASIN-3D variable and units

    def match_unit(self, rf_units: List[str], unit: str) -> Optional[str]:
        """
        Try to match unit
        :param rf_units:
        :param unit:
        :return:
        """
        unit_match = None
        for rf_unit in rf_units:
            unit_syn_list = self.fuzzy_match_store.get(rf_unit, [])
            for unit_syn in unit_syn_list:
                if unit_syn == unit:
                    unit_match = rf_unit
                    return unit_match
        return unit_match

    def convert_value(self, rf_unit: str, convert_type: Optional[str]) -> Union[float, int]:
        if convert_type == 'to_m':
            return self.cm_to_m(rf_unit)
        elif convert_type == 'to_micro':
            return self.milli_to_micro(rf_unit)
        return 1


def _ess_dive_datasets_handler(dirpath: Optional[str] = ESSDIVE_DATASETS_PATH) -> dict:
    """
    Read ESS-DIVE Datasets directory and build dictionary with dataset info for each dataset

    Note: dataset info is updated by _build_locations_store if it is called after this handler method

    :param dirpath:
    :return:
    """
    datasets: Dict[str, Dict] = {}

    if dirpath is None:
        logger.warning('ESSDIVE_DATASETS_PATH not specified or configured in environmental variables.')
        return datasets

    # make path a Path object so works on linux and PC
    essdive_path = pathlib.Path(dirpath)

    for dataset in os.listdir(essdive_path):
        ds_path = essdive_path / dataset

        if not pathlib.Path.is_dir(ds_path):
            continue

        # assumes that directory name has format: <datasetID>-<datasetName>-pid-<datasetUUID>
        ds_full_name = dataset.split('-pid-')[0]
        ds_pieces = ds_full_name.split('-')
        ds_id = ds_pieces[0]
        ds_pid = dataset.split('-pid-')[-1]
        ds_name = '-'.join(ds_pieces[1:])

        ds_key = (ds_id, ds_pid)

        datasets.setdefault(ds_key, {}).update({  # type: ignore[arg-type]
            'pid': ds_pid,
            'id': ds_id,
            'name': ds_name,
            'locations': ds_path / 'locations',
            'data': ds_path / 'data',
            'dataset': ds_path
        })

    return datasets


def _list_csv_files(dir_path: str) -> List:
    """
    Return list of csv files in specified directory
    :param dir_path:
    :return:
    """
    dir_list = os.listdir(dir_path)
    return [f for f in dir_list if f.endswith('.csv')]


def _read_csv_to_pandas_df(filepath: pathlib.Path, columns_only: bool = False) -> Tuple[List, pd.DataFrame]:
    """
    Read a csv file into a pandas dataframe
    :param filepath:
    :param columns_only: bool, return the first couple rows so that the columns can be assessed efficiently
    :return:
    """
    with open(filepath, mode='r', encoding='utf-8') as f:

        # assess the number of header rows
        header_lines = []
        for line in f:
            if line.startswith('#'):
                header_lines.append(line)
            else:
                break

        f.seek(0)
        header_ct = len(header_lines)
        if columns_only:
            nrows = 2
        else:
            nrows = None

        pdf = pd.read_csv(f, header=header_ct, skipinitialspace=True, keep_default_na=False, on_bad_lines='warn', nrows=nrows)

    return header_lines, pdf


def _create_loc_key(ds_id: str, pd_row: Optional[pd.Series] = None, loc_dict: dict = {}) -> tuple:
    """
    Create the location key
    :param ds_id: Dataset id
    :param pd_row: a pandas dataframe row
    :param loc_dict: a location dictionary object
    :return: a tuple consisting of dataset id and location id
    """
    callable_obj = pd_row
    if callable_obj is None:
        callable_obj = loc_dict

    key_elements = [ds_id]

    for term in HydroRFTerms.unique_loc_terms.value:
        if term in callable_obj.keys():
            key_elements.append(callable_obj[term])
            continue
        key_elements.append('None')

    return tuple(key_elements)


def _make_lat_long_key(lat: float, long: float) -> str:
    """
    Make a location id from the lat, long information
    :param lat:
    :param long:
    :return:
    """
    return f'LAT{str(lat)}_LON{str(long)}'


def _has_lat_long_terms(term_list: list) -> bool:
    """
    Helper function to check if lat / long terms are present
    :param term_list:
    :return:
    """
    if HydroRFTerms.lat.value in term_list and HydroRFTerms.long.value in term_list:
        return True
    return False


def _extract_ds_id(mf_list: list) -> list:
    """
    Extract dataset identifier from monitoring feature list
    :param mf_list:
    :return:
    """
    ds_id_list = set()
    for mf_id in mf_list:
        mf_id_pieces = mf_id.split('-')
        ds_id_list.add(mf_id_pieces[0])

    return list(ds_id_list)


def _parse_data_file_header_rows(header_rows: List[str]) -> dict:
    """
    Parse a data file's header rows following the RF
    :param header_rows:
    :return:
    """
    header_store: Dict = {}

    loc_header_idx = []
    loc_header_terms = []

    for idx, row in enumerate(header_rows):
        if idx == 0:
            continue
        if idx == 1:
            if not row.startswith(HydroRFTerms.format_row_text.value):
                logger.error(f'Second header row in data file does not start with "{HydroRFTerms.format_row_text.value}"')
                return header_store
            format_terms_str = row.replace(HydroRFTerms.format_row_text.value, '').replace('\n', '').rstrip(',')
            format_terms = format_terms_str.split(HydroRFTerms.header_delimiter.value)
            format_terms = [x.strip() for x in format_terms]
            if not format_terms or format_terms[0] != HydroRFTerms.column_header.value or format_terms[1] != HydroRFTerms.units.value:
                logger.error(f'Second header row could not be parsed with delimiter {HydroRFTerms.header_delimiter.value} '
                             f'or the first header term was not {HydroRFTerms.column_header.value} '
                             f'or the second header term was not {HydroRFTerms.units.value}')
                return header_store
            for i, term in enumerate(format_terms):
                if term in HydroRFTerms.location_terms.value:
                    loc_header_idx.append(i)
                    loc_header_terms.append(term)
            header_store = {
                'loc_header_terms': loc_header_terms,
                'loc_header_idx': loc_header_idx,
                'loc_column_info': {},
                'variable_column_info': {},
                'time_column_info': {}
            }
            continue

        column_info_str = row.replace(HydroRFTerms.header_row_char.value, '').replace('\n', '').rstrip(',')
        column_info = column_info_str.split(HydroRFTerms.header_delimiter.value)
        column_info = [x.strip() for x in column_info]
        column_name = column_info[0]
        column_name_pieces = column_name.split('_')
        potential_var_col_name = '_'.join(column_name_pieces[:-1])
        sensor_id, variable_name = None, None
        if column_name in HydroRFTerms.location_terms.value:
            store_dict = header_store['loc_column_info']
        elif column_name in HydroRFTerms.variables.value.keys() or potential_var_col_name in HydroRFTerms.variables.value.keys():
            store_dict = header_store['variable_column_info']
            variable_name = column_name
            if potential_var_col_name in HydroRFTerms.variables.value.keys():
                sensor_id = column_name_pieces[-1]
                variable_name = potential_var_col_name
        elif column_name in HydroRFTerms.date_time_vars.value:
            store_dict = header_store['time_column_info']
        else:
            logger.info(f'header term {column_name} is not defined in RF. Skipping.')
            continue
        store_dict_element: Dict = store_dict.setdefault(column_name, {'unit': column_info[1]})
        for i, row_idx in enumerate(loc_header_idx):
            store_dict_element[loc_header_terms[i]] = column_info[row_idx]
        if variable_name:
            store_dict_element.update({HydroRFTerms.sensor_id.value: sensor_id, 'variable': variable_name})

    return header_store


def _check_columns_for_loc_info(data_pdf: pd.DataFrame, header_store: Dict):
    """
    Because data file header rows are not required to be a 1:1 match with the column names,
    look for additional loc info in the columns and update the header_store.
    :param data_pdf:
    :param header_store:
    """
    data_cols = list(data_pdf.columns)
    loc_col_info = header_store['loc_column_info']

    for col_name in data_cols:
        if col_name not in HydroRFTerms.location_terms.value:
            continue
        if col_name not in loc_col_info.keys():
            loc_col_info[col_name] = {}


def _get_queried_variable_info(variable_col_info: Dict, queried_observed_properties: List) -> Tuple[Dict, List, bool]:
    """
    Extract helper info to determine which of the queried observed properties are present in the data variables.
    This is not a 1:1 match on column names so extra handling is required.
    :param variable_col_info:
    :param queried_observed_properties:
    :return:
    """
    queried_col_info = {}
    var_set = set()
    var_list = []
    for col_name, col_info in variable_col_info.items():
        var = col_info.get('variable')
        if var and var in queried_observed_properties:
            var_set.add(var)
            var_list.append(var)
            queried_col_info.update({col_name: col_info})

    has_multiple_same_var = False
    var_set_list = list(var_set)
    if len(var_list) > len(var_set_list):
        has_multiple_same_var = True

    return queried_col_info, var_set_list, has_multiple_same_var


def _has_valid_datetime_col(time_info_store: dict) -> bool:
    """
    Confirm that a valid datetime column is present.
    :param time_info_store:
    :return:
    """
    # if no valid date time variables, return False
    if not time_info_store:
        return False

    # if date_time_end is present, then date_time_start must also be
    if HydroRFTerms.date_time_end.value in time_info_store.keys():
        if HydroRFTerms.date_time_start.value in time_info_store.keys() and HydroRFTerms.date_time.value not in time_info_store.keys():
            return True
        return False

    # If both date_time and date_time_start, return False
    if HydroRFTerms.date_time.value in time_info_store.keys() and HydroRFTerms.date_time_start.value in time_info_store.keys():
        return False

    # Otherwise if either date_time or date_time_start is present, then OK.
    return True


def _get_primary_date_time_col(time_info_store: dict) -> str:
    """
    BASIN-3D does not handle start and end timestamps in the time value pairs object yet.
    Choose a the timestamp start column if both start and end are reported
    :param time_info_store:
    :return:
    """
    if HydroRFTerms.date_time_start.value in time_info_store.keys():
        return HydroRFTerms.date_time_start.value

    return HydroRFTerms.date_time.value


def _build_locations_store(datasets: dict, mf_locations: list) -> Dict[Tuple, locInfo]:
    """
    Compile location information from the datasets give the requested mf_locations if any.
    :param datasets:
    :param mf_locations:
    :return:
    """

    # -------- Method internal functions

    def create_loc_info(ds_id: str, ds_pid: str, loc_id: str, loc_callable_obj: Union[pd.Series, dict]) -> locInfo:
        """
        Create an locinfo object instance
        :param ds_id:
        :param ds_pid:
        :param loc_id:
        :param loc_callable_obj:
        :return:
        """
        loc_info = locInfo(ds_id=ds_id, ds_pid=ds_pid, loc_id=loc_id)
        loc_info = update_loc_info(loc_info, callable_obj=loc_callable_obj)
        return loc_info

    def update_loc_info(loc_info_obj: locInfo, callable_obj: Union[pd.Series, dict]):
        """
        Update a locInfo object
        :param loc_info_obj:
        :param callable_obj:
        :return:
        """
        for term in HydroRFTerms.location_terms.value:
            if term != HydroRFTerms.loc_id.value and term in callable_obj.keys():
                attr_name = HydroRFTerms.get_attr_name(term)
                if attr_name is None:
                    logger.warning(f'Attribute name for term {term} is {attr_name}. This should never happen. Contact developer.')
                    continue

                attr_value = callable_obj[term]

                if isinstance(attr_value, pd.Series):
                    attr_value = attr_value.iloc[0]

                if term in HydroRFTerms.loc_numeric_type.value:
                    try:
                        attr_value = float(attr_value)
                    except Exception as e:
                        logger.warning(f'{e}')
                        continue
                # ToDo: potentially pass thru missing values -- RF requires missing values be report for empty values if the field is included but not enforcing that.
                if attr_value and (attr_value == HydroRFTerms.missing_value_numeric.value if isinstance(attr_value, float) or isinstance(attr_value, int) else False or
                                   attr_value == HydroRFTerms.missing_value_str.value if isinstance(attr_value, str) else False):
                    continue

                if term == HydroRFTerms.depth.value or term == HydroRFTerms.elevation.value:
                    value = getattr(loc_info_obj, attr_name)
                    if value:
                        setattr(loc_info_obj, attr_name, value.append(attr_value))
                    else:
                        setattr(loc_info_obj, attr_name, [attr_value])
                    continue
                setattr(loc_info_obj, attr_name, attr_value)
        return loc_info_obj

    def identify_loc_files(csv_files: List[str], loc_dir: pathlib.Path) -> Tuple[Optional[str], Optional[str]]:
        """
        Identify the filenames of the location files within the dataset.
        :param csv_files:
        :param loc_dir:
        :return:
        """
        install_methods_file = None
        site_id_file = None

        for f in csv_files:
            f_lower = f.lower()
            if not install_methods_file and 'installation' in f_lower and 'method' in f_lower:
                install_methods_file = f
                continue
            elif not site_id_file:
                try:
                    _, pdf = _read_csv_to_pandas_df(loc_dir / f)
                    if HydroRFTerms.loc_id.value in pdf.columns:
                        site_id_file = f
                        continue
                except Exception as e:
                    logger.info(f'MESSAGE: {e}')
                    continue

        return install_methods_file, site_id_file

    def parse_location_file(locations_store, loc_pdf, loc_file, is_additional, additional_pdf, additional_file,
                            im_has_site_id, ds_id, ds_pid, id_term, mf_locations, is_site_id_file=False):
        """
        Parse either the InstallationMethods or supplemental locations file.
        :param locations_store:
        :param loc_pdf:
        :param loc_file:
        :param is_additional:
        :param additional_pdf:
        :param additional_file:
        :param im_has_site_id:
        :param ds_id:
        :param ds_pid:
        :param id_term:
        :param mf_locations:
        :param is_site_id_file:
        :return:
        """
        nrows = loc_pdf.shape[0]
        for idx in range(0, nrows):
            loc_row = loc_pdf.iloc[idx, ]

            lat = loc_row[HydroRFTerms.lat.value]
            long = loc_row[HydroRFTerms.long.value]

            if not isinstance(lat, float) or not isinstance(long, float):
                logger.info(f'{ds_id}: {loc_file} row {idx} does not have valid lat {lat} or long {long} values. Skipping')
                continue

            # loc_id = f'LAT{str(lat)}_LONG{str(long)}'
            loc_id = _make_lat_long_key(lat, long)
            if im_has_site_id or is_site_id_file:
                loc_id = loc_row[id_term]

            # if locations are specified, and the current location is not specified, skip it
            if mf_locations and f'{ds_id}-{loc_id}' not in mf_locations:
                continue

            loc_key = _create_loc_key(ds_id, pd_row=loc_row)
            if loc_key in locations_store.keys():
                logger.warning(f'Row {idx} of {loc_file} duplicated info for {loc_key}. Skipping')
                continue

            loc_info = create_loc_info(ds_id, ds_pid, loc_id, loc_row)

            if im_has_site_id and is_additional:
                site_id_row = additional_pdf[additional_pdf[id_term] == loc_id]
                if site_id_row.shape[0] == 1:
                    update_loc_info(loc_info, site_id_row)
                    continue
                # ToDo: consider adding handling for multiple depths reported for different variables in the IM
                logger.info(f'{ds_id}: {additional_file} as multiple entries for {loc_id}. Only one entry allowed per {loc_id}. Skipping location info in file.')

            locations_store.update({loc_key: loc_info})

    def update_ds_info(ds_store: dict, im_has_lat_long: bool, im_has_site_id: bool, s_has_lat_long: bool,
                       id_term: Optional[str], has_id_in_column: Optional[bool], has_lat_long_in_header: Optional[bool],
                       has_lat_long_in_column: Optional[bool], install_methods_file: Optional[str] = None):
        """
        Update the dataset store for where the location information was found.

        :param ds_store:
        :param im_has_lat_long:
        :param im_has_site_id:
        :param s_has_lat_long:
        :param id_term:
        :param has_id_in_column:
        :param has_lat_long_in_header:
        :param has_lat_long_in_column:
        :return:
        """
        ds_store.update({
            'location_info': {
                'im_has_lat_long': im_has_lat_long,
                'im_has_site_id': im_has_site_id,
                's_has_lat_long': s_has_lat_long,
                'id_term': id_term,
                'has_id_in_column': has_id_in_column,
                'has_lat_long_in_header': has_lat_long_in_header,
                'has_lat_long_in_column': has_lat_long_in_column},
            'install_methods_file': install_methods_file})

    # ---- Main functionality for method

    # Because locations can occur multiple times across several files,
    #    keep track of what has been yielded with fields: <site_id>, <lat>, <long>)
    #    Assumptions:
    #    1. A dataset is consistent in that data files won't have differing locations fields in different places
    #    2. Elevation and depth references are consistent
    #
    #    Choices:
    #    1. A sensor is not a location so replicated sensors at the same location will
    #       not be treated as separate locations. This maybe tricky in the pandas df column name in a view.
    #    2. ID is formed by lat / long if site ID is not provided.
    #    3. Site IDs can have different depths -- going to treat them as the same location in the mf calls,
    #       and a specific location in the timeseries calls. Same column name issue in views as #1

    locations_store: Dict = {}

    ds_ids = _extract_ds_id(mf_locations)

    # ToDo: maybe go get DOI from essdive using pid?
    for ds_key, ds_info in datasets.items():
        ds_id, ds_pid = ds_key

        if ds_ids and ds_id not in ds_ids:
            continue

        loc_dir = ds_info.get('locations')
        loc_files = _list_csv_files(loc_dir)
        # Find the InstallationMethods file and see if there is an additional file that has the Site_ID term
        # If the additional location file does not have Site_ID, cannot support it.
        install_methods_file, site_id_file = identify_loc_files(loc_files, loc_dir)

        if not install_methods_file:
            logger.info(f'{ds_id}-{ds_pid}: No Installation Methods file detected. Cannot parse dataset.')
            continue

        loc_terms_install_id = []
        loc_terms_site_id = []

        id_term: Optional[str] = None
        im_has_site_id = False
        im_is_additional = False

        im_has_lat_long = False
        s_has_lat_long = False
        s_is_additional = False

        im_header_rows, im_pdf = _read_csv_to_pandas_df(loc_dir / install_methods_file)
        im_pdf_columns = list(im_pdf.columns)

        for col_term in im_pdf_columns:
            if col_term in HydroRFTerms.location_terms.value:
                loc_terms_install_id.append(col_term)

        if HydroRFTerms.install_method_id.value not in loc_terms_install_id and HydroRFTerms.install_method_desc.value not in loc_terms_install_id:
            logger.info(f'{ds_id}-{ds_pid} Installation Methods file does not have required fields. Cannot parse.')
            continue

        if _has_lat_long_terms(loc_terms_install_id):
            im_has_lat_long = True

        if not im_has_lat_long and len(loc_terms_install_id) > 2:
            im_is_additional = True

        im_pdf = im_pdf[loc_terms_install_id]

        if HydroRFTerms.loc_id.value in loc_terms_install_id:
            id_term = HydroRFTerms.loc_id.value
            im_has_site_id = True

        s_pdf: Optional[pd.DataFrame] = None
        if site_id_file:

            s_header_rows, s_pdf = _read_csv_to_pandas_df(loc_dir / site_id_file)
            s_pdf_columns = list(s_pdf.columns)

            for col_term in s_pdf_columns:
                if col_term in HydroRFTerms.location_terms.value:
                    loc_terms_site_id.append(col_term)

            if _has_lat_long_terms(loc_terms_site_id):
                if im_has_lat_long:
                    logger.info(f'{ds_id}-{ds_pid}: both Installation Methods and additional locations file have lat / long. Using Install Methods.')
                else:
                    s_has_lat_long = True
                    id_term = HydroRFTerms.loc_id.value

            if not s_has_lat_long and len(loc_terms_site_id) > 1:
                s_is_additional = True

            s_pdf = s_pdf[loc_terms_site_id]

        # ToDo: consider maybe expanding to handle depths in data files
        # ToDo: consider using Sensor_ID (sort of part of RF) as connector -- wait for use case

        has_id_in_column: Optional[bool] = None
        has_lat_long_in_header: Optional[bool] = None
        has_lat_long_in_column: Optional[bool] = None

        # If the lat / long terms are in the Installation Methods file, generate locations from that file.
        if im_has_lat_long:
            logger.info(f'{ds_id}-{ds_pid}: {install_methods_file}: Found lat / long in Installation Methods file. Assuming units / datum is Decimal degrees WGS84.')

            parse_location_file(locations_store=locations_store, loc_pdf=im_pdf, loc_file=install_methods_file,
                                is_additional=s_is_additional, additional_pdf=s_pdf, additional_file=site_id_file,
                                im_has_site_id=im_has_site_id, id_term=id_term, ds_id=ds_id, ds_pid=ds_pid, mf_locations=mf_locations)

            update_ds_info(ds_info, im_has_lat_long, im_has_site_id, s_has_lat_long, id_term,
                           has_id_in_column, has_lat_long_in_header, has_lat_long_in_column, install_methods_file)

        # Otherwise, if the lat / long terms are in the Site ID file, generate locations from this file
        elif s_has_lat_long:
            logger.info(f'{ds_id}-{ds_pid}: {site_id_file}: Found lat / long in additional locations file. Assuming units / datum is Decimal degrees WGS84.')

            parse_location_file(locations_store=locations_store, loc_pdf=s_pdf, loc_file=site_id_file,
                                is_additional=im_is_additional, additional_pdf=im_pdf, additional_file=install_methods_file,
                                im_has_site_id=im_has_site_id, id_term=id_term, ds_id=ds_id, ds_pid=ds_pid,
                                mf_locations=mf_locations, is_site_id_file=True)

            update_ds_info(ds_info, im_has_lat_long, im_has_site_id, s_has_lat_long, id_term,
                           has_id_in_column, has_lat_long_in_header, has_lat_long_in_column, install_methods_file)

        # Finally, if not in either location file, look for lat / long + locations info in the data files
        else:

            data_dir = ds_info.get('data')
            data_files = _list_csv_files(data_dir)
            has_id_in_column = False
            has_lat_long_in_header = False
            has_lat_long_in_column = False

            if not data_files:
                ds_path = ds_info.get('ds_path')
                logger.warning(f'Dataset {ds_id}-{ds_pid} does not have location or data files in {ds_path}. Cannot find locations info.')
                continue

            # Inspect the first data file to determine how to parse the remaining files
            data_file = data_files[0]
            data_header_rows, data_pdf = _read_csv_to_pandas_df(data_dir / data_file, columns_only=True)

            # parse the header rows
            header_store = _parse_data_file_header_rows(data_header_rows)
            _check_columns_for_loc_info(data_pdf, header_store)

            if not header_store:
                logger.warning(f'{ds_id}-{ds_pid}: Upon initial data file inspection, {data_file} has malformed header. Cannot parse dataset.')
                continue

            if _has_lat_long_terms(header_store['loc_header_terms']):
                has_lat_long_in_header = True
            elif _has_lat_long_terms(list(header_store['loc_column_info'].keys())):
                has_lat_long_in_column = True

            if HydroRFTerms.loc_id.value in header_store['loc_column_info'].keys():
                has_id_in_column = True
                id_term = HydroRFTerms.loc_id.value

            update_ds_info(ds_info, im_has_lat_long, im_has_site_id, s_has_lat_long, id_term,
                           has_id_in_column, has_lat_long_in_header, has_lat_long_in_column, install_methods_file)

            # -------- start the logic --------

            # if lat / long is in both the header and column, won't deal with it.
            if has_lat_long_in_header and has_lat_long_in_column:
                logger.info(f'{ds_id}-{ds_pid}: Data files based on initial file inspection of {data_file} has lat / long info in both the header and the columns. Cannot parse dataset.')
                continue

            # if lat / long in the header rows, loop through the rows that describe the columns if they are not location terms
            elif has_lat_long_in_header:
                logger.info(f'{ds_id}-{ds_pid}: Found lat / long in initial data file header inspection. Assuming units / datum is Decimal degrees WGS84.')

                for data_file in data_files:
                    data_header_rows, data_pdf = _read_csv_to_pandas_df(data_dir / data_file)

                    # parse the header rows
                    header_store = _parse_data_file_header_rows(data_header_rows)
                    _check_columns_for_loc_info(data_pdf, header_store)

                    if not header_store:
                        logger.warning(f'{ds_id}-{ds_pid}: Data file {data_file} has malformed header. Cannot parse. Skipping file.')
                        continue

                    if not _has_lat_long_terms(header_store['loc_header_terms']):
                        logger.warning(f'{ds_id}-{ds_pid}: Cannot find lat / long terms in {data_file}. Cannot parse. Skipping file.')
                        continue

                    for col_name, col_info in header_store['variable_column_info'].items():
                        try:
                            lat = float(col_info.get(HydroRFTerms.lat.value))
                            long = float(col_info.get(HydroRFTerms.long.value))
                        except Exception:
                            lat = None
                            long = None

                        if not isinstance(lat, float) or not isinstance(long, float):
                            logger.info(f'{ds_id}-{ds_pid}: {data_file}: {col_name} does not have valid lat {lat} or long {long} values. Skipping')
                            continue

                        loc_id = _make_lat_long_key(lat, long)
                        im_id = col_info.get(HydroRFTerms.install_method_id.value)
                        im_row: Optional[pd.Series] = None

                        try:
                            im_row = im_pdf[im_pdf[HydroRFTerms.install_method_id.value] == im_id]
                            if im_row.shape[0] > 1:
                                logger.info(f'{ds_id}-{ds_pid}: {install_methods_file} as multiple entries for {im_id}. '
                                            f'Only one entry allowed. Skipping integration with Install Methods for {col_name}.')
                                continue
                        except Exception:
                            pass

                        if HydroRFTerms.loc_id.value in header_store['loc_header_terms']:
                            loc_id = col_info[HydroRFTerms.loc_id.value]
                        elif im_has_site_id and im_row:
                            loc_id = im_row[HydroRFTerms.loc_id.value]
                        elif HydroRFTerms.loc_id.value in header_store['loc_column_info']:
                            logger.info(f'{ds_id}-{ds_pid}: lat / long info is in the header rows and {HydroRFTerms.loc_id.value} '
                                        f'is in the columns and cannot be utilized. Continuing with Lat/long identifier.')

                        loc_key = _create_loc_key(ds_id, loc_dict={HydroRFTerms.loc_id.value: loc_id,
                                                                   HydroRFTerms.lat.value: lat,
                                                                   HydroRFTerms.long.value: long})

                        if mf_locations and f'{ds_id}-{loc_id}' not in mf_locations:
                            continue

                        if loc_key in locations_store.keys():
                            logger.warning(f'{ds_id}-{ds_pid}: {data_file}: Loc info for {col_name} has duplicated info for {loc_key}. Skipping')
                            continue

                        loc_info = create_loc_info(ds_id, ds_pid, loc_id, col_info)

                        if im_is_additional:
                            update_loc_info(loc_info, im_row)

                        locations_store.update({loc_key: loc_info})

            # lat / long is in the columns, loop thru the pandas data frame rows
            elif has_lat_long_in_column:
                logger.info(f'{ds_id}: Found lat / long in initial data file column inspection. Assuming units / datum is Decimal degrees WGS84.')

                for data_file in data_files:
                    data_header_rows, data_pdf = _read_csv_to_pandas_df(data_dir / data_file)

                    # parse the header rows
                    header_store = _parse_data_file_header_rows(data_header_rows)
                    _check_columns_for_loc_info(data_pdf, header_store)

                    if not header_store:
                        logger.warning(f'{ds_id}-{ds_pid}: Data file {data_file} has malformed header. Cannot parse. Skipping file.')
                        continue

                    columns_store = header_store['loc_column_info']

                    if not _has_lat_long_terms(list(columns_store.keys())):
                        logger.warning(f'{ds_id}-{ds_pid}: Data file {data_file} does not have lat / long columns as expected. Skipping file.')
                        continue

                    data_pdf = data_pdf[list(columns_store.keys())]
                    group_cols = [HydroRFTerms.lat.value, HydroRFTerms.long.value]

                    if has_id_in_column and HydroRFTerms.loc_id.value not in columns_store.keys():
                        logger.info(f'{ds_id}-{ds_pid}: {data_file} Site ID should be available as a column and it is not. Skipping file.')
                        continue
                    elif has_id_in_column:
                        group_cols.append(HydroRFTerms.loc_id.value)

                    unique_groups = list(data_pdf.groupby(group_cols).indices.keys())

                    if not unique_groups:
                        logger.info(f'{ds_id}-{ds_pid}: {data_files} Could not find unique location groups among file rows. Skipping')
                        continue

                    for loc_group in unique_groups:
                        lat = loc_group[0]
                        long = loc_group[1]

                        if not isinstance(lat, float) or not isinstance(long, float):
                            logger.info(f'{ds_id}-{ds_pid}: {data_file} does not have valid lat {lat} or long {long} values. Skipping')
                            continue

                        loc_id = _make_lat_long_key(lat, long)
                        if has_id_in_column:
                            loc_id = loc_group[2]

                        loc_dict = {HydroRFTerms.loc_id.value: loc_id,
                                    HydroRFTerms.lat.value: lat,
                                    HydroRFTerms.long.value: long}

                        loc_key = _create_loc_key(ds_id, loc_dict=loc_dict)

                        if mf_locations and f'{ds_id}-{loc_id}' not in mf_locations:
                            continue

                        # We might expect the location to be in multiple files so not reporting log message.
                        if loc_key in locations_store.keys():
                            continue

                        loc_info = create_loc_info(ds_id, ds_pid, loc_id, loc_dict)
                        locations_store.update({loc_key: loc_info})

            else:
                logger.info(f'{ds_id}-{ds_pid}: Cannot find lat / long info in data files based on initial file inspection of {data_file}. Cannot parse dataset.')

    return locations_store


def _build_loc_store_index(ds_id: str, loc_store: dict) -> dict:
    """
    Build an index to the locations store
    This is a helper index b/c the key is a dataset id, location id tuple.
    :param ds_id:
    :param loc_store:
    :return:
    """
    key_index = {}
    for key_tuple in loc_store.keys():
        key_ds_id, loc_id = key_tuple
        if key_ds_id == ds_id:
            key_index.update({loc_id: key_tuple})

    return key_index


def _make_mf_object(plugin_access: DataSourcePluginAccess, mf_info: locInfo,
                    is_mf_query: bool = False, additional_data_info: dict = {}) -> Optional[MonitoringFeature]:
    """
    Construct a monitoring feature object instance
    :param plugin_access:
    :param mf_info:
    :param additional_data_info:
    :return:
    """

    def configure_elev_depth_fields(term: str, mf_info: locInfo, value, is_mf_query: bool) -> Tuple[dict, Optional[str]]:
        """
        Handle elevation and depth fields
        :param term: RF Depth or Elevation term
        :param mf_info: location info object from locations store
        :param value: the value acquired from the data columns
        :return:
        """
        elev_depth: Dict = {'value': None}

        # get the value for the specified term; if there is not and also no explicit value, return
        term_value = getattr(mf_info, term)
        if not value and not term_value:
            return elev_depth, ''

        # try to get the term reference
        ref = getattr(mf_info, f'{term}_reference')

        # is_mf_query, make a string of depths
        if is_mf_query:
            if value:
                term_value = [value]
            txt = ', '.join(str(v) for v in term_value)
            txt = f' Observations at known {term}s: {txt} {ref}. These values may differ from those reported within the data files.'
            return elev_depth, txt

        depth_height_value = value
        if not value and term_value:
            depth_height_value = term_value[0]

        if depth_height_value == HydroRFTerms.missing_value_numeric.value:
            return elev_depth, ''

        elev_depth['value'] = depth_height_value

        ref_datum = None
        ref_unit = None

        unit = DepthCoordinate.DISTANCE_UNITS_METERS
        datum = AltitudeCoordinate.DATUM_NAVD88
        if term == 'depth':
            datum = DepthCoordinate.DATUM_LOCAL_SURFACE

        if not ref:
            logger.info(f'{mf_info.ds_id}-{mf_info.ds_pid}: {term.capitalize()}_Reference not reported with {term} values. Assuming {datum} per RF.')
            ref_datum = datum
            ref_unit = unit
        elif term == 'elevation' and datum in ref:
            ref_datum = datum
        elif term == 'depth' and 'ground surface' in ref:
            ref_datum = datum

        if ref and unit in ref:
            ref_unit = unit
        elif ref:
            logger.info(f'{mf_info.ds_id}-{mf_info.ds_pid}: {term.capitalize()}_Reference does not contain standard units')
            if DepthCoordinate.DISTANCE_UNITS_FEET in ref:
                ref_unit = DepthCoordinate.DISTANCE_UNITS_FEET

        if ref_datum:
            elev_depth.update({'datum': ref_datum})
        if ref_unit:
            elev_depth.update({'distance_units': ref_unit})

        return elev_depth, ''

    # set up depth and elev info. If comes from data files and it separate from the location info, get it from the data parsing.
    depth, elev = None, None
    if additional_data_info:
        depth = additional_data_info.get('depth')
        elev = additional_data_info.get('elev')

    depth_info, depth_txt = configure_elev_depth_fields('depth', mf_info, depth, is_mf_query)
    elev_info, elev_txt = configure_elev_depth_fields('elevation', mf_info, elev, is_mf_query)

    mf_name = mf_info.site_name
    if not mf_name:
        mf_name = mf_info.water_name
    if not mf_name:
        mf_name = mf_info.loc_id

    coordinates = Coordinate(
            absolute=AbsoluteCoordinate(
                horizontal_position=GeographicCoordinate(
                    **{'latitude': mf_info.lat,
                       'longitude': mf_info.long,
                       'datum': HorizontalCoordinate.DATUM_WGS84,
                       'units': GeographicCoordinate.UNITS_DEC_DEGREES})))

    elev_str = ''
    if elev_info.get('value'):
        coordinates.absolute.vertical_extent = [AltitudeCoordinate(**elev_info)]
        elev_str = f'-ELEV{str(elev_info.get("value"))}'

    depth_str = ''
    if depth_info.get('value'):
        coordinates.representative = RepresentativeCoordinate(vertical_position=DepthCoordinate(**depth_info))
        depth_str = f'-DEPTH{str(depth_info.get("value"))}'

    add_desc = ''
    sensor_str = ''
    if mf_info.sensor_id:
        add_desc = f' Sensor_ID: {mf_info.sensor_id}.'
        sensor_str = f'-SENSOR{mf_info.sensor_id}'
    if mf_info.install_method_desc:
        add_desc = f'{add_desc} Installation Method Description: {mf_info.install_method_desc}.'

    if additional_data_info:
        col_unit = additional_data_info.get('col_unit')
        col_rf_unit = additional_data_info.get('col_rf_unit')
        extra_unit_txt = ''
        if col_unit != col_rf_unit:
            convert_txt = ''
            if additional_data_info.get('conversion_factor') != 1:
                convert_txt = f' with conversion factor {additional_data_info.get("conversion_factor")}'
            extra_unit_txt = f' that was changed to RF unit {col_rf_unit}{convert_txt}'
        add_desc = (f'{add_desc} Data file: {additional_data_info.get("data_file")}; '
                    f'Column name: {additional_data_info.get("col_name")} '
                    f'with units {col_unit}{extra_unit_txt}.')

    desc = f'ESSDIVE dataset: {mf_info.ds_id}; pid: {mf_info.ds_pid}.{elev_txt}{depth_txt}{add_desc}'

    monitoring_feature = MonitoringFeature(
        plugin_access,
        id=f'{mf_info.ds_id}-{mf_info.loc_id}{elev_str}{depth_str}{sensor_str}',
        name=mf_name,
        description=desc,
        feature_type=mf_info.loc_type,
        shape=SpatialSamplingShapes.SHAPE_POINT,
        observed_properties=[],
        coordinates=coordinates
    )

    return monitoring_feature


def _parse_data_file(plugin_access: DataSourcePluginAccess, data_dir: pathlib.Path, data_file: str,
                     observed_property: list, mf_locations: list, start_date: date, end_date: Optional[date],
                     id_term: Optional[str], id_in_header: bool, id_in_col: bool, id_ref_im: bool, has_sensor_id: bool,
                     ds_id: str, ds_pid: str, im_ref_store: dict, loc_store_index: dict, locations_store: dict) -> Iterator:
    """
    Parse data file that follows the variations of the RF.

    :param plugin_access:
    :param data_dir:
    :param data_file:
    :param observed_property:
    :param mf_locations:
    :param start_date:
    :param end_date:
    :param id_term:
    :param id_in_header:
    :param id_in_col:
    :param id_ref_im:
    :param has_sensor_id:
    :param ds_id:
    :param ds_pid:
    :param ds_info:
    :param im_ref_store:
    :param loc_store_index:
    :param locations_store:
    :return:
    """
    data_header_rows, data_pdf = _read_csv_to_pandas_df(data_dir / data_file)

    # parse the header rows and check column names for any location terms
    header_store = _parse_data_file_header_rows(data_header_rows)
    _check_columns_for_loc_info(data_pdf, header_store)

    if not header_store:
        logger.warning(f'{ds_id}-{ds_pid}: {data_file} has malformed header. Cannot parse file.')
        yield None

    # No datetime variable, skip.
    if not _has_valid_datetime_col(header_store.get('time_column_info', {})):
        logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} does not have valid date / time column names. Skipping.')
        yield None

    # Filter by the start date. Skip file if no date matches query dates.
    time_primary_var = _get_primary_date_time_col(header_store['time_column_info'])

    # First make sure date time values are date or datetime
    try:
        for time_col in header_store['time_column_info']:
            data_pdf[time_col] = data_pdf[time_col].apply(pd.Timestamp)
    except Exception as e:
        logger.warning(f'{ds_id}-{ds_pid}: {data_file} has malformed datetime columns format. Cannot parse file. Error {e}')
        yield None

    data_pdf = data_pdf[data_pdf[time_primary_var] >= start_date]
    if end_date:
        data_pdf = data_pdf[data_pdf[time_primary_var] <= end_date]

    if data_pdf.shape[0] == 0:
        logger.debug(f'{ds_id}-{ds_pid}: Data file {data_file} does not have timestamps matching the specfied query. Skipping.')
        yield None

    # Next, get variable info
    col_var_info, file_var_list, has_multiple_sensors_cols = _get_queried_variable_info(header_store.get('variable_column_info', {}), observed_property)

    vars_in_file = []
    for header_var_name, var_info in col_var_info.items():
        rf_var_name = var_info.get('variable')
        if rf_var_name not in file_var_list:
            continue
        if header_var_name not in data_pdf.columns:
            logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} header variable {header_var_name} does not have corresponding column name.')
            continue
        vars_in_file.append(rf_var_name)

    # Check if the data file has any of the specified observed properties. If not, skip
    if not vars_in_file:
        logger.debug(f'{ds_id}-{ds_pid}: Data file {data_file} does not have queried observed properties. Skipping.')
        yield None

    # If locations terms are in the column names (e.g., Site_ID, Depth, etc),
    #   then do a little work to separate out the datastreams

    # Get the location terms that are in the columns
    col_loc_terms = list(header_store.get('loc_column_info', []).keys())

    # set up to combo rows for individual locations
    column_term_for_combo = []

    # If Site_ID in column vars, then add, otherwise if lat / lon in col vars, add those
    if id_term and id_in_col and HydroRFTerms.loc_id.value in col_loc_terms:
        column_term_for_combo.append(HydroRFTerms.loc_id.value)
    elif id_term and id_in_col:
        logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} does not match expected format of other data files. Skipping.')
        yield None
    elif id_term is None and id_in_col and HydroRFTerms.lat.value in col_loc_terms and HydroRFTerms.long.value in col_loc_terms:
        # This could be a problem if lat / long info is also in the header or Install Methods file
        column_term_for_combo.extend([HydroRFTerms.lat.value, HydroRFTerms.long.value])
    elif id_term is None and id_in_col:
        logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} does not match expected format of other data files. Skipping.')
        yield None

    # If Sensor_ID is a column var, add
    if has_sensor_id and HydroRFTerms.sensor_id.value in col_loc_terms:
        column_term_for_combo.append(HydroRFTerms.sensor_id.value)
    elif has_sensor_id:
        logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} does not match expected format of other data files. Skipping.')
        yield None

    # check for depth / elevation columns -- add to combo if present
    # ToDo: add some handling for validating the reference datum and units
    if HydroRFTerms.depth.value in col_loc_terms:
        column_term_for_combo.append(HydroRFTerms.depth.value)
    if HydroRFTerms.elevation.value in col_loc_terms:
        column_term_for_combo.append(HydroRFTerms.elevation.value)

    # combo
    # add pdf combo info and the combo terms (or maybe this is in the pandas combo stuff)
    if column_term_for_combo:
        combo_pdf = data_pdf[column_term_for_combo].groupby(column_term_for_combo)
        combo_grp_store = combo_pdf.indices
    else:
        combo_grp_store = {'all': data_pdf.index.array}

    # loop thru the variable columns
    for col_name, col_info in col_var_info.items():
        col_var = col_info.get('variable')
        if col_var not in vars_in_file:
            continue

        col_sensor_id = col_info.get('sensor_id')
        col_unit = col_info.get('unit')

        loc_id: Optional[str] = None
        if id_in_header:
            if id_term == HydroRFTerms.loc_id.value:
                loc_id = col_info.get(HydroRFTerms.loc_id.value)
            else:
                lat = col_info.get(HydroRFTerms.lat.value)
                long = col_info.get(HydroRFTerms.long.value)
                loc_id = _make_lat_long_key(lat, long)

        if id_ref_im:
            im_id = col_info.get(HydroRFTerms.install_method_id.value)
            loc_id = im_ref_store.get(im_id)

        if loc_id:
            if f'{ds_id}-{loc_id}' not in mf_locations or loc_id not in loc_store_index.keys():
                continue
        elif not id_in_col and not loc_id:
            logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} variable {col_name}; Could not find location id.')
            continue

        # determine units
        # Assume the unit for the column matches the RF specification
        col_rf_unit = col_unit

        # Get the RF options for the col variable
        col_rf_var_units_store: Dict = HydroRFTerms.variables.value
        col_rf_units_info = col_rf_var_units_store.get(col_var, {})
        col_rf_units = col_rf_units_info.get('rf_options', [])
        col_convert = col_rf_units_info.get('convert')

        # Check if the column unit is in the RF options for the column variable, if not, see if can fuzzy match it.
        if col_unit not in col_rf_units:
            col_rf_unit = UnitHandler().match_unit(col_rf_units, col_unit)

        # if the unit does not match the specification, skip the column
        if not col_rf_unit:
            logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} variable {col_name} unit {col_unit} not match RF specification. Skipping variable.')
            continue

        conversion_factor = UnitHandler().convert_value(col_rf_unit, col_convert)

        # loop thru the unique loc combo loops, if loc info is not in columns, then only one combo -- the full df
        for col_combo, data_row_indices in combo_grp_store.items():

            data_rows: pd.DataFrame = data_pdf.iloc[data_row_indices, ]

            # find the location_id an/or make it.
            if id_in_col:
                if id_term:
                    loc_id = data_rows[HydroRFTerms.loc_id.value].iloc[0]
                else:
                    lat = data_rows[HydroRFTerms.lat.value].iloc[0]
                    long = data_rows[HydroRFTerms.long.value].iloc[0]
                    loc_id = _make_lat_long_key(lat, long)

            # if loc ID was not found OR the loc_id is not in the query OR the loc_id is not in the locations store, skip
            if not loc_id or (loc_id and f'{ds_id}-{loc_id}' not in mf_locations) or loc_id not in loc_store_index.keys():
                continue

            # otherwise, build the results list
            tvp_results = []
            value_errors = 0
            other_errors = 0

            nrows = data_rows.shape[0]
            for idx in range(0, nrows):
                data_row = data_rows.iloc[idx, ]

                dt_col = data_row[time_primary_var]
                dt_col_iso = dt_col.isoformat()
                val_col = data_row[col_name]
                if val_col == HydroRFTerms.missing_value_numeric.value:
                    continue
                # Checking in case the missing str value was used
                elif val_col == HydroRFTerms.missing_value_str.value:
                    continue

                try:
                    # only try conversion if value is not a numeric type b/c don't want to change an integer if the data is an integer so precision is not inflated
                    if not isinstance(val_col, float) or isinstance(val_col, int):
                        value = float(val_col) * conversion_factor
                    else:
                        value = val_col * conversion_factor
                except ValueError:
                    value_errors += 1
                    continue
                except Exception:
                    other_errors += 1
                    continue

                tvp_results.append(TimeValuePair(timestamp=dt_col_iso, value=value))

            for e_count, e_msg in zip([value_errors, other_errors],
                                      ['data value(s) could not be converted to numeric', 'data value(s) had unexpected errors']):
                if e_count > 0:
                    logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} variable {col_name}: {e_count} {e_msg}')

            # Do we want to return anything if no results?? I think not.
            if not tvp_results:
                logger.info(f'{ds_id}-{ds_pid}: Data file {data_file} had no results that were not missing values')
                continue

            # make the monitoring feature object
            mf_info: Optional[locInfo] = locations_store.get(loc_store_index.get(loc_id))

            if not mf_info:
                logger.info(f'{ds_id}-{ds_pid}: Could not find location information for {loc_id}.')
                continue

            depth, elev, sensor_id = None, None, None
            if HydroRFTerms.depth.value in column_term_for_combo:
                depth_val = data_rows[HydroRFTerms.depth.value].iloc[0]
                try:
                    depth = float(depth_val)
                except Exception:
                    depth = None
            if HydroRFTerms.elevation.value in column_term_for_combo:
                elev_val = data_rows[HydroRFTerms.elevation.value].iloc[0]
                try:
                    elev = float(elev_val)
                except Exception:
                    elev = None

            if col_sensor_id:
                sensor_id = col_sensor_id
            elif HydroRFTerms.sensor_id.value in column_term_for_combo:
                sensor_id = data_rows[HydroRFTerms.sensor_id.value].iloc[0]

            additional_data_info = {
                'data_file': data_file,
                'col_name': col_name,
                'depth': depth,
                'elev': elev,
                'sensor_id': sensor_id,
                'col_unit': col_unit,
                'col_rf_unit': col_rf_unit,
                'conversion_factor': conversion_factor}

            mf_obj = _make_mf_object(plugin_access=plugin_access, mf_info=mf_info, additional_data_info=additional_data_info)

            # make the timeseries object
            mvp = MeasurementTimeseriesTVPObservation(
                plugin_access=plugin_access,
                id=f'MeasTimeseriesTVPObs--{ds_id}-{loc_id}',  # FYI: this field is not unique and thus kinda useless
                feature_of_interest_type=FeatureTypeEnum.POINT,
                feature_of_interest=mf_obj,
                result=ResultListTVP(plugin_access=plugin_access, value=tvp_results),
                observed_property=col_var,
                aggregation_duration='NONE',  # Assuming all data is instantaneous b/c the RF doesn't support it
            )

            # Collect a bit more info:
            utc_offset = None
            if mf_info.utc_offset:
                utc_offset = mf_info.utc_offset
            elif HydroRFTerms.utc_offset.value in col_loc_terms:
                utc_offset = data_rows[HydroRFTerms.utc_offset.value].iloc[0]

            if utc_offset:
                try:
                    mvp.utc_offset = int(utc_offset)
                except Exception:
                    pass

            if time_primary_var == HydroRFTerms.date_time_start.value:
                mvp.time_reference_position = TimeMetadataMixin.TIME_REFERENCE_START

            # yield the object
            yield mvp


class ESSDIVEMonitoringFeatureAccess(DataSourcePluginAccess):
    """
    Access for mapping locations for ESS-DIVE datasets following the Hydrological Monitoring Reporting Format to
    :class:`~basin3d.core.models.MonitoringFeature` objects.

    A location is considered unique on location id
    """
    synthesis_model_class = MonitoringFeature

    def list(self, query: QueryMonitoringFeature) -> Iterator:
        """
        List the monitoring features that match the specified query.

        :param query: a :class:`~basin3d.core.schema.query.QueryMonitoringFeature` object
        :returns: a generator object that yields :class:`~basin3d.core.models.MonitoringFeature` objects
        """

        synthesis_messages: List[str] = []

        datasets = _ess_dive_datasets_handler()

        if query.feature_type and query.feature_type != 'POINT':
            logger.info(f'Dataset {self.datasource.id} does not yet support feature_type {query.feature_type}')
            yield

        if query.parent_feature:
            logger.info(f'Dataset {self.datasource.id} does not support query by parent_feature')
            yield

        mf_ids = []
        if query.monitoring_feature:
            mf_ids = query.monitoring_feature
        elif query.id:
            mf_ids = [query.id]

        mf_store = _build_locations_store(datasets, mf_ids)

        for mf_info in mf_store.values():
            yield _make_mf_object(self, mf_info, is_mf_query=True)

        StopIteration(synthesis_messages)

    def get(self, query):
        """
        Get a single monitoring feature object based on an identifier, i.e. the query must have id specified.

        :param query: a :class:`~basin3d.core.schema.query.QueryMonitoringFeature` object
        :return: a :class:`~basin3d.core.models.MonitoringFeature` object
        """
        if not query.id:
            return None

        mf_iterator = self.list(query)
        mf_list = [mf for mf in mf_iterator]

        if len(mf_list) == 0:
            return None
        elif len(mf_list) > 1:
            logger.warning(f'More than one MonitoringFeature object for id {query.id} only returning first object.')

        return mf_list[0]


class ESSDIVEMeasurementTimeseriesTVPObservationAccess(DataSourcePluginAccess):
    """
    Access for ESS-DIVE data in the Hydrological Monitoring Reporting Format to
    :class:`~basin3d.core.models.MeasurementTimeseriesTVPObservation` objects.
    """
    synthesis_model_class = MeasurementTimeseriesTVPObservation

    def list(self, query: QueryMeasurementTimeseriesTVP):
        """
        Get the data that matches the specified query

        :param query: a :class:`~basin3d.core.schema.query.QueryMeasurementTimeseriesTVP` object
        :return: a generator object that yields :class:`~basin3d.core.models.MeasurementTimeseriesTVPObservation` objects
        """

        synthesis_messages: List[str] = []

        observed_property = query.observed_property
        start_date = pd.Timestamp(query.start_date)
        end_date: Optional[pd.Timestamp] = None
        if query.end_date:
            end_date = pd.Timestamp(query.end_date)
        mf_locations = query.monitoring_feature
        mf_datasets = _extract_ds_id(mf_locations)

        datasets = _ess_dive_datasets_handler()

        location_store = _build_locations_store(datasets, mf_locations=query.monitoring_feature)

        for ds_key, ds_info in datasets.items():
            ds_id, ds_pid = ds_key

            if ds_id not in mf_datasets:
                continue

            data_dir = ds_info.get('data')
            data_files = _list_csv_files(data_dir)

            if not data_files:
                ds_path = ds_info.get('ds_path')
                logger.warning(f'Dataset {ds_id}-{ds_pid} does not have location or data files in {ds_path}. Cannot find locations info.')
                continue

            # Inspect the first data file to determine how to parse the remaining files
            data_file_init = data_files[0]
            data_header_rows, data_pdf = _read_csv_to_pandas_df(data_dir / data_file_init, columns_only=True)

            # parse the header rows
            header_store = _parse_data_file_header_rows(data_header_rows)
            _check_columns_for_loc_info(data_pdf, header_store)

            if not header_store:
                logger.warning(f'{ds_id}-{ds_pid}: Upon initial data file inspection, {data_file_init} has malformed header. Cannot parse dataset.')
                continue

            # 3 cases:
            #   * One file per sensor (could have multiple variables per sensor)
            #   * One file: a column for each sensor (<var_name>_index)
            #   * One file: Sensor_ID column

            # Also, have to find how to get location:
            #   * Site_ID
            #     -- column name
            #     -- header info
            #   * Lat / Long only
            #     -- column names
            #     -- header info
            # 'loc_header_terms': [], 'loc_header_idx': [], 'loc_column_info': {}, 'variable_column_info': {}, 'location_info': {}

            loc_info = ds_info.get('location_info')
            # keys:
            #   all datasets have: 'im_has_lat_long', 'im_has_site_id', 's_has_lat_long', 'id_term',
            #   only T/F if loc info from data files: 'has_id_in_column, 'has_lat_long_in_header', 'has_lat_long_in_column'
            #   id_term = None if using lat / long

            # Is the location identifier in the header, via IM reference, or columns?

            id_term = loc_info.get('id_term')

            col_loc_terms = list(header_store.get('loc_column_info', {}).keys())
            header_loc_terms = header_store.get('loc_header_terms', [])

            id_in_header = False
            id_in_col = False
            id_ref_im = False
            # if location term is Site_ID, where is it in the data file?
            if id_term == HydroRFTerms.loc_id.value:
                if HydroRFTerms.loc_id.value in header_loc_terms:
                    id_in_header = True
                elif HydroRFTerms.loc_id.value in col_loc_terms:
                    id_in_col = True
                elif ds_info.get('location_info').get('im_has_site_id'):
                    id_ref_im = True
            else:
                if _has_lat_long_terms(header_loc_terms):
                    id_in_header = True
                elif _has_lat_long_terms(col_loc_terms):
                    id_in_col = True
                elif ds_info.get('location_info').get('im_has_lat_long'):
                    id_ref_im = True

            if (id_in_header and id_in_col) or (id_in_header and id_ref_im) or (id_in_col and id_ref_im):
                logger.warning(f'{ds_id}-{ds_pid}: {data_file_init} has {HydroRFTerms.loc_id.value} in multiple places in the initial data file inspection. Cannot parse dataset.')
                continue
            elif all([id_in_header, id_in_col, id_ref_im]):
                logger.warning(f'{ds_id}-{ds_pid}: Could not identify location id in initial data file inspection {data_file_init}. Cannot parse dataset.')

            has_sensor_id = False
            if HydroRFTerms.sensor_id.value in col_loc_terms:
                has_sensor_id = True

            # if the location id is referenced via the InstallationMethods file -- parse it and return the dict.
            im_ref_store = {}
            if id_ref_im:
                loc_dir = ds_info.get('locations')
                install_methods_file = ds_info.get('install_methods_file')

                if not install_methods_file:
                    logger.warning(f'{ds_id}-{ds_pid}: Location ID is expected to be in the Install Methods file but cannot find it. Skipping dataset')
                    continue

                im_header_rows, im_pdf = _read_csv_to_pandas_df(loc_dir / install_methods_file)
                im_pdf_columns = list(im_pdf.columns)

                if (HydroRFTerms.install_method_id.value not in im_pdf_columns and
                        (HydroRFTerms.loc_id.value not in im_pdf_columns or
                         HydroRFTerms.lat.value not in im_pdf_columns and HydroRFTerms.long.value not in im_pdf_columns)):
                    logger.info(f'{ds_id}-{ds_pid} Installation Methods file does not have required fields. Cannot parse dataset.')
                    continue

                im_nrows = im_pdf.shape[0]
                for idx in range(0, im_nrows):
                    im_row = im_pdf.iloc[idx, ]
                    im_id = im_row[HydroRFTerms.install_method_id.value]
                    if HydroRFTerms.loc_id.value in im_pdf_columns:
                        im_loc_id = im_row[HydroRFTerms.loc_id.value]
                    else:
                        lat = im_row[HydroRFTerms.lat.value]
                        long = im_row[HydroRFTerms.long.value]
                        im_loc_id = _make_lat_long_key(lat, long)
                    im_ref_store.update({im_id: im_loc_id})

            # loc_id (no ds_prefix): location_store_key
            loc_store_idx = _build_loc_store_index(ds_id, location_store)

            for data_file in data_files:
                meas_tvp_obj_gen = _parse_data_file(
                    plugin_access=self, data_dir=data_dir, data_file=data_file, observed_property=observed_property,
                    mf_locations=mf_locations, start_date=start_date, end_date=end_date, id_term=id_term,
                    id_in_header=id_in_header, id_in_col=id_in_col, id_ref_im=id_ref_im, has_sensor_id=has_sensor_id,
                    ds_id=ds_id, ds_pid=ds_pid, im_ref_store=im_ref_store,
                    loc_store_index=loc_store_idx, locations_store=location_store)
                for mtvpo in meas_tvp_obj_gen:
                    if mtvpo:
                        yield mtvpo

        StopIteration(synthesis_messages)


@basin3d_plugin
class ESSDIVEDataSourcePlugin(DataSourcePluginPoint):
    title = 'ESS-DIVE Hydrological Monitoring Reporting Format Data Source Plugin'
    plugin_access_classes = (ESSDIVEMonitoringFeatureAccess, ESSDIVEMeasurementTimeseriesTVPObservationAccess)

    feature_types = ['POINT']

    class DataSourceMeta:
        """
        This is an internal metadata class for defining additional :class:`~basin3d.models.DataSource`
        attributes.

        **Attributes:**
            - *id* - unique id short name
            - *name* - human friendly name (more descriptive)
            - *location* - resource location
            - *id_prefix* - id prefix to make model object ids unique across plugins
            - *credentials_format* - if the data source requires authentication, this is where the
                format of the stored credentials is defined.

        """
        # Data Source attributes
        id = 'ESSDIVE'  # unique id for the datasource
        location = 'https://ess-dive.data.lbl.gov'
        id_prefix = 'ESSDIVE'
        name = 'ESSDIVE'  # Human Friendly Data Source Name
