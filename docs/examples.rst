.. _basin3dexamples:

Examples
********

Get Timeseries Data
-------------------

Load plugin(s)

>>> from basin3d.plugins import usgs

Load the synthsis module

>>> from basin3d import synthesis

Register all loaded plugins. See synthesizer documentation to register specific plugin(s).

>>> synthesizer = synthesis.register()

Get timeseries data for the specified locations, variables, and time period (see below for required and optional arguments).

>>> usgs_df, usgs_metadata = synthesis.get_timeseries_data(synthesizer, monitoring_features=['USGS-09110000'],
...    observed_property_variables=['RDC','WT'], start_date='2019-10-25', end_date='2019-10-30')

| The timeseries data is synthesized into a pandas dataframe with the timestamps as the index and as a pandas timedate class in the first column.
| The data column names have the format <location_id>__<variable_id> (2 underscores separate the values).

>>> usgs_df
            TIMESTAMP  USGS-09110000__WT  USGS-09110000__RDC
2019-10-25 2019-10-25                3.2            4.247527
2019-10-26 2019-10-26                4.1            4.219210
2019-10-27 2019-10-27                4.3            4.134260
2019-10-28 2019-10-28                3.2            4.332478
2019-10-29 2019-10-29                2.2            4.219210
2019-10-30 2019-10-30                0.5            4.247527

| A dictionary containing metadata for each data column is also returned.
| The column names are the keys. The dictionary values are a dictionary of the following fields:

>>> for k, v in usgs_metadata['USGS-09110000__WT'].items():
...     print(f'{k} = {v}')
data_start = 2019-10-25 00:00:00
data_end = 2019-10-30 00:00:00
records = 6
units = deg C
basin_3d_variable = WT
basin_3d_variable_full_name = Water Temperature
statistic = MEAN
temporal_aggregation = DAY
quality = CHECKED
sampling_medium = WATER
sampling_feature_id = USGS-09110000
datasource = USGS
datasource_variable = 00010

:emphasis:`Note: Currently units are passed from the data source.`

Required parameters
^^^^^^^^^^^^^^^^^^^

:param synthesizer: DataSynthesizer object with plugins registered
:param monitoring_features: List of location ids available from the datasources
:param observed_property_variables: List of basin3d-core variables
:param start_date: YYYY-MM-DD HH:MM:SS, The start date of the data record to synthesize
:param temporal_resolution: TimeFrequency, currently only DAY is supported

Optional parameters
^^^^^^^^^^^^^^^^^^^

:param end_date: YYYY-MM-DD HH:MM:SS, The end date of the data record to synthesize. If not provided, the current datetime is used.
:param aggregation_duration: AggregationDuration, currently = temporal_resolution = DAY
:param result_quality: ResultQuality: 'CHECKED' or 'UNCHECKED' (Must be enabled in plugin)
:param datasource: str, the id attribute of a registered datasource

How to know what data is available
----------------------------------

.. WARNING::
    These methods may change in the future.

For all of the methods below, a create a DataSynthesis object by:

* Load one or more plugins
* Load the register function from the syntesis module
* Create a DataSynthesis object

>>> from basin3d.plugins import usgs
>>> from basin3d import synthesis
>>> synthesizer = synthesis.register()

Datasources
^^^^^^^^^^^

To list the available datasource objects:

>>> synthesizer.datasources
[DataSource(id='USGS', name='USGS', id_prefix='USGS', location='https://waterservices.usgs.gov/nwis/', credentials={})]

When getting timeseries data from one datasource, use the datasource.id attribute.

>>> [datasource.id for datasource in synthesizer.datasources]
['USGS']

basin3d-core Variables
^^^^^^^^^^^^^^^^^^^^^^

To create a generator for the observed property variables available from the registered plugins:

>>> observed_property_variables = synthesizer.observed_property_variables()

To create a list of the basin3d variable ids (use these as parameters for observed_property_variables in the get_timeseries_data function)

>>> [obj.basin3d_id for obj in observed_property_variables]
['ACT', 'Br', 'Cl', 'DIN', 'DTN', 'F', 'NO3', ...]

Monitoring Features (a.k.a. Locations)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A generator can be created with the monitoring feature available from the registered plugins.

Use the following parameters to filter the results as the number of monitoring features can be quite large.

:param feature_type: FeatureTypes,

    To see all feature types:

    >>> from basin3d.core.types import FeatureTypes
    >>> [feature_type for feature_type in FeatureTypes.TYPES.values()]
    ['REGION', 'SUBREGION', 'BASIN', 'SUBBASIN', 'WATERSHED', 'SUBWATERSHED', 'SITE', 'PLOT', 'HORIZONTAL PATH', 'VERTICAL PATH', 'POINT']

    To see feature types for a given plugin: <plugin_module>.<plugin_class>.feature_types. For example:

    >>> usgs.USGSDataSourcePlugin.feature_types
    ['POINT', 'REGION', 'BASIN', 'SUBREGION', 'SUBBASIN']

:param datasource: the id attribute of a registered datasource
:param parent_feature: list of the id attributes of a monitoring features, the monitoring features that are returned have the specified parent_feature(s).

.. WARNING::
    | Filtering by parent features only works for the USGS plugin because they are hard-coded in the plugin.
    | We aim to add this functionality universally to basin3d-core in future.

Examples
""""""""

Inspect the SUBBASIN monitoring features available in USGS Region USGS-14: Upper Colorado

>>> for mf in synthesizer.monitoring_features(datasource='USGS', feature_type='SUBBASIN', parent_features=['USGS-14']):
...    print(f'{mf.id} - {mf.description}')
USGS-14010001 - Colorado headwaters Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14010002 - Blue Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14010003 - Eagle Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14010004 - Roaring Fork Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14010005 - Colorado headwaters-Plateau Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14010006 - Parachute-Roan Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14020001 - East-Taylor Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14020002 - Upper Gunnison Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14020003 - Tomichi Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14020004 - North Fork Gunnison Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14020005 - Lower Gunnison Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14020006 - Uncompahange Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14030001 - Westwater Canyon Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14030002 - Upper Dolores Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14030003 - San Miguel Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14030004 - Lower Dolores Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14030005 - Upper Colorado-Kane Springs Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040101 - Upper Green Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040102 - New Fork Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040103 - Upper Green-Slate Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040104 - Big Sandy Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040105 - Bitter Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040106 - Upper Green-Flaming Gorge Reservoir Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040107 - Blacks Fork Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040108 - Muddy Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040109 - Vermilion Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14040200 - Great Divide closed basin Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14050001 - Upper Yampa Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14050002 - Lower Yampa Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14050003 - Little Snake Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14050004 - Muddy Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14050005 - Upper White Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14050006 - Piceance-Yellow Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14050007 - Lower White Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060001 - Lower Green-Diamond Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060002 - Ashley-Brush Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060003 - Duchesne Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060004 - Strawberry Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060005 - Lower Green-Desolation Canyon Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060006 - Willow Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060007 - Price Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060008 - Lower Green Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14060009 - San Rafael Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14070001 - Upper Lake Powell Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14070002 - Muddy Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14070003 - Fremont Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14070004 - Dirty Devil Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14070005 - Escalante Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14070006 - Lower Lake Powell Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14070007 - Paria Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080101 - Upper San Juan Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080102 - Piedra Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080103 - Blanco Canyon Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080104 - Animas Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080105 - Middle San Juan Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080106 - Chaco Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080107 - Mancos Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080201 - Lower San Juan-Four Corners Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080202 - Mcelmo Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080203 - Montezuma Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080204 - Chinle Watershed: Drainage basin code is defined by the USGS State Office where the site is located.
USGS-14080205 - Lower San Juan Watershed: Drainage basin code is defined by the USGS State Office where the site is located.

Inspect the list of POINT monitoring features available in the USGS-14020002 - Upper Gunnison Watershed

:emphasis:`Note: USGS POINT monitoring feature ids are used as the monitoring_features parameter of get_timeseries_data`

>>> for mf in synthesizer.monitoring_features(feature_type='point',parent_features=['USGS-14020002']):
...    print(f"{mf.id} - {mf.name}: {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
USGS-09113000 - CASTLE CREEK NEAR BALDWIN, CO.: [(-107.101438, 38.76638315)]
USGS-09113100 - CASTLE CREEK ABOVE MOUTH NEAR BALDWIN, CO: [(-107.084493, 38.76916079)]
USGS-09113300 - OHIO CREEK AT BALDWIN, CO.: [(-107.0583812, 38.7655496)]
USGS-09113500 - OHIO CREEK NEAR BALDWIN, CO.: [(-106.9999472, 38.70304167)]
USGS-09113980 - OHIO CREEK ABOVE MOUTH NR GUNNISON, CO: [(-106.931432, 38.5877696)]
USGS-09114000 - OHIO CREEK NEAR GUNNISON, CO.: [(-106.9386546, 38.5752694)]
USGS-09114500 - GUNNISON RIVER NEAR GUNNISON, CO.: [(-106.9497661, 38.54193567)]
USGS-09114520 - GUNNISON RIVER AT GUNNISON WHITEWATER PARK, CO: [(-106.9490861, 38.5332722)]
USGS-09120500 - GUNNISON RIVER AT IOLA, CO.: [(-107.0889379, 38.4824915)]
USGS-09121500 - CEBOLLA CREEK NEAR LAKE CITY, CO.: [(-107.1686631, 37.9811084)]
USGS-09121800 - CEBOLLA CREEK NEAR POWDERHORN, CO.: [(-107.0733833, 38.22749466)]
USGS-09122000 - CEBOLLA CREEK AT POWDERHORN, CO.: [(-107.1144958, 38.291383)]
USGS-09122500 - SOAP CREEK NEAR SAPINERO, CO: [(-107.325, 38.5608333)]
USGS-09123000 - SOAP CREEK AT SAPINERO, CO.: [(-107.2989443, 38.474992)]
USGS-09123400 - LAKE FORK BELOW MILL GULCH NEAR LAKE CITY, CO.: [(-107.3847788, 37.90638636)]
USGS-09123450 - LAKE FORK BLW LAKE SAN CRISTOBAL NR LAKE CITY, CO: [(-107.2920972, 37.9843611)]
USGS-09123500 - LAKE FORK AT LAKE CITY, CO: [(-107.3144444, 38.01888889)]
USGS-09124000 - HENSON CREEK AT LAKE CITY, CO.: [(-107.3353338, 38.0197189)]
USGS-09124010 - HENSON CREEK AT LAKE CITY, CO: [(-107.3163472, 38.02566944)]
USGS-09124500 - LAKE FORK AT GATEVIEW, CO.: [(-107.2300557, 38.2988834)]
USGS-09124700 - GUNNISON RIVER BELOW BLUE MESA DAM, CO.: [(-107.3481119, 38.45221354)]
USGS-09125000 - CURECANTI CREEK NEAR SAPINERO, CO.: [(-107.415057, 38.4877673)]
USGS-09125800 - SILVER JACK RESERVOIR NEAR CIMARRON, CO: [(-107.5417263, 38.23276926)]
USGS-09126000 - CIMARRON RIVER NEAR CIMARRON, CO: [(-107.5461111, 38.25819444)]
USGS-09126500 - CIMARRON RIVER AT CIMARRON, CO.: [(-107.5542252, 38.44109929)]
USGS-09127000 - CIMARRON RIVER BLW SQUAW CREEK AT CIMARRON, CO: [(-107.5552222, 38.44694444)]
USGS-09127500 - CRYSTAL CREEK NEAR MAHER, CO.: [(-107.506169, 38.5519326)]
USGS-09128000 - GUNNISON RIVER BELOW GUNNISON TUNNEL, CO: [(-107.648947, 38.52915336)]
USGS-09128500 - SMITH FORK NEAR CRAWFORD, CO.: [(-107.5067234, 38.72776785)]
USGS-09129000 - SMITH FORK AT CRAWFORD, CO.: [(-107.5761685, 38.710544)]
USGS-09129500 - IRON CREEK NEAR CRAWFORD, CO.: [(-107.6025577, 38.68082114)]
USGS-09129550 - CRAWFORD RESERVOIR NEAR CRAWFORD, CO: [(-107.6061688, 38.69137668)]
USGS-09129600 - SMITH FORK NEAR LAZEAR, CO: [(-107.7101389, 38.70744444)]
USGS-383103106594200 - GUNNISON RIVER AT CNTY RD 32 BELOW GUNNISON, CO: [(-106.99545, 38.51725556)]

..
    Once bug is fixed, add this back in
    A single monitoring feature object can be specified using the id parameter:

    >>> mf = synthesizer.monitoring_features(id='USGS-1402')
    >>> print(f"{mf.id} - {mf.description}")
    USGS-1402 - SUBREGION: Gunnison
