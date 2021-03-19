.. _modelsreference:

Quick Guide
******************

Hook up Data Sources
---------------------
To access a data source, its BASIN-3D plugin must be configured. Custom plugins can be built. Documentation will be forthcoming in future versions. Currently available plugins:

    - USGS Daily Values

If a plugin exists for the data source, import it before registering a synthesizer.

    ::

        from basin3d.plugins import <plugin_name>


Register a Synthesizer
------------------------
.. autofunction:: basin3d.synthesis.register


Get a List of Locations for a Data Source
------------------------------------------------
.. autofunction:: basin3d.synthesis.DataSynthesizer.monitoring_features


Get a List of Variables Supported by a Data Source
----------------------------------------------------
.. autofunction:: basin3d.synthesis.DataSynthesizer.observed_property_variables


Get Time Series Data
----------------------
.. autofunction:: basin3d.synthesis.get_timeseries_data







