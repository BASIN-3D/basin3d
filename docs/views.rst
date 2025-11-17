.. _basin3dviews:

Views
*****

BASIN-3D Views is a collection of BASIN-3D results (e.g., Monitoring Features, Measurement Timeseries TVP Observations)
formatted in common data structures, like GeoPandas Geo Data Frames.

.. _watershed_workflows_monitoring_features_geopandas:

Watershed Workflows - Monitoring Features GeoPandas View
--------------------------------------------------------

Watershed Workflows is a python-based, open source chain of tools for generating meshes and other data inputs
for hyper-resolution hydrology.
See https://environmental-modeling-workflows.github.io/watershed-workflow/stable/index.html

The BASIN-3D Monitoring Feature GeoPandas View was developed in coordination with Ethan Coon to support
acquisition of USGS water monitoring location information using BASIN-3D tools.


Usage
^^^^^
.. code-block::

    >>> from basin3d.plugins import usgs
    >>> from basin3d import synthesis
    >>> from basin3d.views import watershed_workflow as b3dww
    >>> synthesizer = synthesis.register()
    >>> monitoring_feature_geopandas = b3dww.get_monitoring_features(synthesizer, datasource=['USGS'], feature_type='point', monitoring_feature=[(-90.6, 34.4, -90.5, 34.6)])
    >>> monitoring_feature_geopandas.shape
    (2, 7)
    >>> print(monitoring_feature_geopandas)
                  id                             name feature_type description data_source  elevation                    geometry
    0  USGS-07047970  MISSISSIPPI RIVER AT HELENA, AR        POINT        None        USGS      141.7    POINT (-90.58399 34.524)
    1  USGS-07287700     PHILLIPS BAYOU AT POWELL, MS        POINT        None        USGS        NaN  POINT (-90.53022 34.48425)

    [2 rows x 7 columns]


Note: The coordinate reference system (CRS) is by default ESPG:4326 (WGS84). An alternative CRS may be provided by including the parameter crs in the get_monitoring_features call.


Technical details
^^^^^^^^^^^^^^^^^

.. autofunction:: basin3d.views.watershed_workflow.get_monitoring_features
    :noindex:
