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

Section 1: EPA Data Source Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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

Option 1: Specify a `USGS HUC <https://water.usgs.gov/GIS/huc.html>`_ 2, 4, 6, or 8-digit code in the parent_feature argument. Prefix the HUC identifier with "EPA-".
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
