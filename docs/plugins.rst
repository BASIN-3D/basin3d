.. _basin3dplugins:

Data Sources Plugins
********************************

USGS Daily and Instantaneous Values
-----------------------------------
The USGS Daily and Instantaneous Values Data Source is available. See :doc:`quick_guide` for example usage.

Additional details coming soon.


EPA Water Quality eXchange (WQX)
--------------------------------
Water quality data submitted to the `Environmental Protection Agency <https://www.epa.gov/waterdata/water-quality-data>`_
from federal, state and tribal agencies, watershed organizations and other groups.

These data are acquired from the National Water Quality Monitoring Council `Water Quality Portal <https://www.waterqualitydata.us/>`_,
specifying the provider STORET.

**Data Usage** See citation information at `WQP User Guide <https://www.waterqualitydata.us/portal_userguide/>`_. Requests to the WQP web services are logged in BASIN-3D. See :doc:`/quick_guide`.

Section 1: Data Source Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
None.
Data are publicly available and accessed via the `Water Quality Portal Web Services <https://www.waterqualitydata.us/webservices_documentation/>`_.
Please follow data usage guidelines at `User Guide <https://www.waterqualitydata.us/portal_userguide/>`_ .


Section 2: Using the EPA plugin in BASIN-3D
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Install basin3d using the instructions in the :doc:`/getting_started`.

2. Import the EPA WQX plugin:

>>> from basin3d.plugins import epa

3. Register the plugin (and any other you have imported):

>>> from basin3d import synthesis
>>> synthesizer = synthesis.register()

4. Find the available monitoring feature IDs (aka location identifiers). Note: These monitoring feature IDs are required parameters for BASIN-3D data requests.

Option 1: Specify a `USGS HUC <https://water.usgs.gov/GIS/huc.html>`_ 2, 4, 6, 8, 10, or 12-digit code in the parent_feature argument. Prefix the HUC identifier with "EPA-".
The BASIN-3D USGS plugin can be used to find USGS HUC information and identifiers:

.. code-block::

    >>> monitoring_features = synthesizer.monitoring_features(parent_feature=['EPA-14020001'])
    >>> for monitoring_feature in monitoring_features:
    >>>     print(f'{monitoring_feature.id} --- {monitoring_feature.name}')
    EPA-21COL001-000058 --- TAYLOR RIVER AT ALMONT
    EPA-21COL001-000078 --- EAST RIVER AT CONFL. WITH TAYLOR
    EPA-21COL001-000150 --- SLATE RIVER ABOVE COAL CREEK
    ...

Option 2: Specify one or more EPA Site IDs, prefixed by "EPA-", in the monitoring_feature argument:

.. code-block::

    >>> monitoring_features = synthesizer.monitoring_features(monitoring_feature=['EPA-CORIVWCH_WQX-176', 'EPA-11NPSWRD_WQX-BLCA_09128000'])
    >>> for monitoring_feature in monitoring_features:
    >>>     print(f'{monitoring_feature.id} --- {monitoring_feature.name}')
    EPA-11NPSWRD_WQX-BLCA_09128000 --- GUNNISON RIVER BELOW GUNNISON TUNNEL, CO
    EPA-CORIVWCH_WQX-176 --- Uncompahgre -  Confluence Park


5. Request time series data. Query argument aggregation_duration supports "NONE" or "DAY". See Section 4 below for full vocabulary mapping details. See Section 3: Data Considerations below for details on how data below detection limits are handled.

.. code-block::

    >>> measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(monitoring_feature=['EPA-CCWC-COAL-26', 'EPA-CCWC-MM-29 WASH #3'], observed_property=['As', 'WT', 'DO'], start_date='2010-01-01', end_date='2011-01-01', aggregation_duration='NONE')
    >>> for m in measurement_timeseries_tvp_observations:
    ...     for r in m.result.value:
    ...         print(f"feature_of_interest:'{m.feature_of_interest}' observed_property:{m.observed_property} timestamp:{r.timestamp} value:{r.value} {m.unit_of_measurement}" )
    ...
    Could not parse expected numerical measurement value <0.500
    Could not parse expected numerical measurement value <2.50
    Could not parse expected numerical measurement value <0.500
    Could not parse expected numerical measurement value <0.500
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-08-17T11:30:00-07:00 value:14.4 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-10-12T14:27:00-07:00 value:14.0 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-10-12T14:27:00-07:00 value:9.44 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-05-18T16:21:00-07:00 value:3.38 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-10-12T14:27:00-07:00 value:9.73 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-04-20T13:43:00-07:00 value:4.19 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-10-12T14:27:00-07:00 value:11.7 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-08-17T11:30:00-07:00 value:14.3 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-05-18T16:21:00-07:00 value:3.98 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:As timestamp:2010-08-17T11:30:00-07:00 value:15.1 ug/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:WT timestamp:2010-04-20T13:43:00-07:00 value:0.8 deg C
    feature_of_interest:'EPA-CCWC-MM-29 WASH #3' observed_property:WT timestamp:2010-04-20T15:00:00-07:00 value:1.52 deg C
    feature_of_interest:'EPA-CCWC-MM-29 WASH #3' observed_property:DO timestamp:2010-04-20T15:00:00-07:00 value:14.28 mg/L
    feature_of_interest:'EPA-CCWC-COAL-26' observed_property:DO timestamp:2010-04-20T13:43:00-07:00 value:10.78 mg/L
    Could not parse expected numerical measurement value <0.500
    Could not parse expected numerical measurement value <2.50
    Could not parse expected numerical measurement value <0.500
    Could not parse expected numerical measurement value <0.500


6. Synthesized data should be cited following the Water Quality Portal data use policies. See **Data Usage** above.


Section 3: Usage Notes
^^^^^^^^^^^^^^^^^^^^^^
.. warning::
  **BASIN-3D capabilities that cannot be supported or are limited for the EPA WQX data source include:**

    | - No unit conversions are performed for data values. Each :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` object will have consistent units for its data values. The EPA WQX units are reported in the :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` unit_of_measurement attribute and may not match the specified BASIN-3D vocabulary unit. Note: Unit conversions are performed for any depth / height information from "ft", "cm", or "in" to meters.
    |
    | - Timestamps are reported in both Standard and Daylight Savings time. When supplied, the utc_offset is reported in the :class:`basin3d.core.models.TimeValuePair` timestamp attribute following the ISO format. Because the utc_offset changes during the year, no value is reported in the :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` utc_offset attribute.
    |
    | - Only instantaneous and daily time aggregations are currently supported. Daily aggregations are supplied by the data source, not aggregated by BASIN-3D. All data with no EPA WQX Field "ResultTimeBasisText" reported are considered instantaneous. See Section 4 for details on EPA WQX Fields.

Data Considerations
"""""""""""""""""""
  * Supported data are `Sample Results (Physical/Chemical) <https://www.waterqualitydata.us/portal_userguide/#table-7-sample-results-physicalchemical-result-retrieval-metadata>`_ data categorization.
  * Data are not continuous time series; however they are reported as such. Thus, it is possible that replicate observations may be reported at the same timestamp. This may complicate combination with continuous time series data.
  * Data values below detection limits are indicated in EPA WQX using the less than symbol "<". These values are not supported by BASIN-3D. See the :class:`basin3d.core.schema.query.SynthesisMessage` in the :class:`basin3d.core.schema.query.SynthesisResponse` messages attribute.
  * Both start and end timestamps may be provided by EPA WQX. Only the start timestamp information is mapped to the BASIN-3D objects.
  * Additional metadata not supported by BASIN-3D like analysis temperature and sample fraction are reported in the :class:`basin3d.core.models.MeasurementTimeseriesTVPObservation` id attribute.

Location Considerations
"""""""""""""""""""""""
  * All locations are considered POINT locations.
  * Height and depth references do not seem to be standardized and are not consistently reported. When it is, it is not captured in the BASIN-3D objects.
  * EPA location identifiers may be acquired using USGS HUC codes in the :class:`basin3d.core.schema.query.QueryMonitoringFeature` parent_feature attribute. See example above.


Section 4: Data Source Info
^^^^^^^^^^^^^^^^^^^^^^^^^^^
**User Guide** https://www.waterqualitydata.us/portal_userguide/

**Vocabulary definitions** https://www.epa.gov/waterdata/storage-and-retrieval-and-water-quality-exchange-domain-services-and-downloads

  ============================  =======================
  EPA WQX Field                 BASIN-3D Attribute
  ============================  =======================
  CharacteristicName            OBSERVED_PROPERTY
  ----------------------------  -----------------------
  ActivityMedia                 SAMPLING_MEDIUM
  ----------------------------  -----------------------
  StatisticBase                 STATISTIC
  ----------------------------  -----------------------
  ResultStatus                  RESULT_QUALITY
  ----------------------------  -----------------------
  ResultValueType (Estimate)    RESULT_QUALITY
  ----------------------------  -----------------------
  ResultTimeBasis               AGGREGATION_DURATION
  ============================  =======================

**Vocabulary Mapping File** `epa_mapping.csv <https://github.com/BASIN-3D/basin3d/blob/main/basin3d/plugins/epa_mapping.csv>`_

**Citation** Water Quality Portal. Washington (DC): National Water Quality Monitoring Council, United States Geological Survey (USGS), Environmental Protection Agency (EPA); 2021. https://doi.org/10.5066/P9QRKUVJ.


ESS-DIVE Hydrologic Monitoring Reporting Format (RF) Plugin
-----------------------------------------------------------
The `Environmental System Science Data Infrastructure for a Virtual Ecosystem (ESS-DIVE) <https://ess-dive.lbl.gov/>`_ is a data repository for Earth and environmental sciences research supported by the US Department of Energy.

The ESS-DIVE plugin supports datasets formatted using the `ESS-DIVE Community Hydrologic Monitoring Reporting Format <https://github.com/ess-dive-community/essdive-hydrologic-monitoring>`_.

Desired datasets must be downloaded to your local machine. Use the `ESS-DIVE data portal <https://data.ess-dive.lbl.gov/data>`_ to discover and download datasets of interest. Additionally, any dataset that follows the reporting format can be synthesized with the plugin.

Data usage should follow the `ESS-DIVE Data Use and Citation policies <https://ess-dive.lbl.gov/data-use-and-citation>`_.
We recommend that DOI information be acquired for data citation while users are acquiring the datasets for local configuration. Future versions of the ESS-DIVE plugin aim to provide the DOI automatically with query results.

Section 1: Data Source Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1. Each dataset must have its own directory within a single, top-level directory. Each dataset must have files organized into 2 subdirectories called "data" and "locations".

  |    -- Top-level directory
  |       -- Dataset 1 directory
  |          -- data
  |          -- locations
  |       -- Dataset 2 directory
  |          -- data
  |          -- locations
  |       ...


2. Each dataset directory must use the following naming convention::

    <location_grouping_code>-<dataset_name>-pid-<essdive_dataset_pid>

  where,

   - *location_grouping_code* is a user-defined short, unique code for datasets that a share location naming schema. For example, ESS-DIVE projects may define a common set of location identifiers that project researchers use in their separate measurement collections.
     An example *location_group_code* is WFSFA for the Watershed Function-SFA project. If you don't have information to determine datasets that share the same location naming schema, we recommend using a different code of your choice for each dataset.
   - *dataset_name* is a user-defined human-readable name of the dataset that will be included in the BASIN-3D metadata. Use underscores to separate words. Users may choose to use the dataset title and/or a shortened versions of it.
   - *essdive_dataset_pid* is the ESS-DIVE persistent identifier (pid) for the dataset. It can be found on the ESS-DIVE dataset webpage in the header above the list of files in the dataset (see screenshot in example below).

  Do not use hyphens in the *location_grouping_code* or *dataset_name*.

  An example dataset directory name: ``SPS21-Spatial_Study_2021_River_Corridor_Watershed_Biogeochemistry_SFA-pid-ess-dive-af2abbeb5ffb423-20230509T155621313`` for the dataset
  `Spatial Study 2021: Sensor-Based Time Series of Surface Water Temperature, Specific Conductance, Total Dissolved Solids, Turbidity, pH, and Dissolved Oxygen from across Multiple Watersheds in the Yakima River Basin, Washington, USA <https://data.ess-dive.lbl.gov/view/doi:10.15485/1892052>`_,
  where,

    - ``SPS21`` is the *location_grouping_code*.
    - ``Spatial_Study_2021_River_Corridor_Watershed_Biogeochemistry_SFA`` is the *dataset_name*; Note: no hyphens used.
    - ``ess-dive-af2abbeb5ffb423-20230509T155621313`` is the *essdive_dataset_pid*. See screenshot below for pid location on a dataset's ESS-DIVE webpage.

    .. image:: _static/images/ess-dive_pid_example.png
      :align: center


  The same *location_grouping_code* should be used for datasets if they share the same location naming schema, i.e., the same location identifiers / names.
  For example, Watershed Function-SFA has a standardized locations list that all researchers use to identify the locations where measurements are being made.
  If 2 observation types are taken at the same WFSFA location and submitted to ESS-DIVE in separate datasets, both of those datasets should use the same *location_grouping_code* so that the BASIN-3D location identifiers are the same.

  See Section 3 below for more information on how location identification, including *location_grouping_code*, is used in the BASIN-3D monitoring feature objects.

3. The locations subdirectory in each dataset can contain only 2 files. One **must** be the Installation Methods file, described in
   the `reporting format instructions <https://github.com/ess-dive-community/essdive-hydrologic-monitoring/blob/main/HydroRF_Instructions.md>`_.
   The other can be a supplementary locations information file that uses
   the `Hydrologic Monitoring Reporting Format defined terms <https://github.com/ess-dive-community/essdive-hydrologic-monitoring/blob/main/HydroRF_Term_Guide.md>`_.

4. All data files should be put in the data subdirectory. Data files must follow the `reporting format instructions <https://github.com/ess-dive-community/essdive-hydrologic-monitoring/blob/main/HydroRF_Instructions.md>`_ or they will not be synthesized. Hierarchical structures are not supported.

5. The top-level directory path must be configured as an environmental variable in the environment where you are running basin3d::

    $ export $ESSDIVE_DATASETS_PATH=<top_level_directory_path>


Section 2: Using the ESSDIVE plugin in BASIN-3D
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
*Note: Only dataset files that follow the ESS-DIVE Hydrological Monitoring Reporting Format are supported by BASIN-3D in the ESSDIVE plugin.*

1. Install basin3d using the instructions in the :doc:`Getting Started Guide </getting_started>`.

2. Configure datasets as described above. Including specifying the top-level directory path as an environmental variable.

3. Import the ESS-DIVE plugin::

    >>> from basin3d.plugins import essdive

4. Register the plugin (and any other you have imported)::

    >>> synthesizer = synthesis.register()

5. Find the available monitoring feature IDs (aka location identifiers). Note: BASIN-3D data requests must have monitoring features listed by ID::

    >>> monitoring_features = synthesizer.monitoring_features(datasource='ESSDIVE')
    >>> for monitoring_feature in monitoring_features:
    >>>     print(f'{monitoring_feature.id} --- {monitoring_feature.name}')

6. Request time series data (arguments in the example below, including monitoring_feature IDs, are for illustration only)::

    >>> measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(monitoring_feature=['ESSDIVE-LOCGRP1-Site1'], observed_property=['PH', 'WT'], start_date='2022-01-01', aggregation_duration='NONE')
    >>> for mvp in measurement_timeseries_tvp_observations:
    >>>    print(f'{mvp.feature_of_interest.id} --- {mvp.observed_property}'

7. Synthesized data should be cited following the ESS-DIVE data usage policy.

Section 3: Usage Notes
^^^^^^^^^^^^^^^^^^^^^^

.. warning::
  **BASIN-3D capabilities that cannot be supported or are limited for the ESS-DIVE Hydrological Monitoring RF include:**

    | - All locations are considered POINT for the feature_type Monitoring Feature attribute.

    | - All data are considered instantaneous. The RF does not describe standard reporting of temporal aggregation and statistics.

    | - Monitoring Feature parent_feature attribute is not supported because the reporting format does not support it.

    | - Monitoring Feature observed_properties attribute is not supported.

    | - When using the synthesizer.monitoring_feature() method, locations are not resolved by depth. If available in the metadata, depths will be listed in the description field of the monitoring feature object. Depths are be resolved for data requests, i.e., for synthesizer.measurement_timeseries_tvp_observations() method, separate time series objects with distinct location information are generated.

General considerations
""""""""""""""""""""""

  * The plugin will extract only information that strictly follows the defined portions of the Hydrologic Monitoring Reporting Format. Datasets and individual files that do not match the format are not snythesized. The plugin may skip a dataset wholly or partial.
  * For example, the plugin does not support custom vocabularies defined in a data dictionary. It cannot extract location information referenced in another ESS-DIVE dataset listed in the file-level metadata and/or data dictionary.
  * The plugin supports csv files that can be ingested with Python pandas package. Large files may not be readable as chunking is not enabled in this first version.
  * Location latitude and longitude must be present in the dataset for a particular time series to be synthesized.
  * The plugin assumes that the reporting format is applied uniformly within a dataset. It assesses an initial data file and discards any data files there after that do not follow the same reporting format application.

Data considerations
"""""""""""""""""""

  * Only `vocabulary defined by the reporting format <https://github.com/ess-dive-community/essdive-hydrologic-monitoring/blob/main/HydroRF_RecommendedVocabulary.md>`_ is supported.
  * The reporting format allows variables to have a single suffix (e.g., pH_1) to indicate the sensor when multiple sensors measuring the same property are employed. The plugin supports any defined variable vocabulary with a single suffix separated by an underscore. Note: the plugin does not validate the suffix as a valid sensor ID.
  * The reporting format defined terms Sensor_Depth and Sensor_Elevation are assumed to vary in time and are not supported at this time. Depth and Elevation terms are considered fixed and included in a time series location metadata.
  * The reporting format implies that complete time series are contained in a single file for a given variable. The plugin follows this assumption and does not piece together a complete time series (i.e., time periods) separated into multiple files.

Location considerations
"""""""""""""""""""""""

  * If Site_ID is not provided, an location ID is created using the lat / long coordinates. The lat / long ID is used as a monitoring feature ID in a data query.
  * BASIN-3D monitoring feature identifiers are constructed as follows: ESSDIVE-<location_grouping_code>-<dataset_location_id>, where the *dataset_location_identifier* is either the provided Site_ID or the constructed lat / long ID. *location_grouping_code* is described in Section 1.
  * Sensor_ID is not considered a unique location identifier. Different lat, long, depth/elevation values must be used to distinguish separate locations. If multiple sensors are deployed as replicates at the same location, their data will be returned in separate time series objects with the same location information.
  * The plugin does not validate consistency of Site_ID and lat / long coordinates. The reporting format allows for location information to be specified repeatedly in multiple places within the various files. Only one location per Site_ID is generated. All others with the same Site_ID that encountered afterward are ignored.

Section 4: Data Source Info
^^^^^^^^^^^^^^^^^^^^^^^^^^^
**User Guide** https://github.com/ess-dive-community/essdive-hydrologic-monitoring/ See the Instructions documentation.

**Vocabulary definitions**
https://github.com/ess-dive-community/essdive-hydrologic-monitoring/blob/main/HydroRF_RecommendedVocabulary.md
https://github.com/ess-dive-community/essdive-hydrologic-monitoring/blob/main/HydroRF_Term_Guide.md

**Vocabulary Mapping File** `essdive_mapping.csv <https://github.com/BASIN-3D/basin3d/blob/main/basin3d/plugins/essdive_mapping.csv>`_

**Citation** Goldman A E ; Ren H ; Torgeson J ; Zhou H (2021): ESS-DIVE Reporting Format for Hydrologic Monitoring Data and Metadata. Environmental Systems Science Data Infrastructure for a Virtual Ecosystem (ESS-DIVE). doi:10.15485/1822940