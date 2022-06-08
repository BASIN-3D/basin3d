.. _quickguidereference:

Quick Guide
******************

Hook up Data Sources
---------------------
To access a data source, its BASIN-3D plugin must be configured. Custom plugins can be built. Documentation will be forthcoming in future versions. Current available plugins:

    - USGS Daily Values and Instantaneous Values

If a plugin exists for the data source, import it before registering a synthesizer.

    ::

        from basin3d.plugins import <plugin_name>


Register a Synthesizer
------------------------
.. autofunction:: basin3d.synthesis.register
    :noindex:


Get a List of Locations for a Data Source
------------------------------------------------
.. autofunction:: basin3d.synthesis.DataSynthesizer.monitoring_features
    :noindex:


Get a List of Variables Supported by a BASIN-3D
------------------------------------------------------------
.. autofunction:: basin3d.synthesis.DataSynthesizer.observed_properties
    :noindex:


Logging
-------
Use :func:`basin3d.monitor.configure` to configure python logging.

The example below changes the log level to DEBUG. Please refer to the Python logging documentation
(https://docs.python.org/3/howto/logging.html#configuring-logging) on logging configuration options. :func:`~basin3d.monitor.configure`
returns the logging configuration as a dictionary for review.

.. code-block::

   >>> from basin3d import monitor, synthesis
   >>> from basin3d.plugins import usgs
   >>> monitor.configure(loggers={"basin3d": {"level": "DEBUG"}})
   {'version': 1, 'incremental': False, 'disable_existing_loggers': True,
   ...

Once logging is configured, all BASIN-3D logging is outputted.

.. code-block::

   >>> synthesizer = synthesis.register()
   2022-04-14T16:59:31.082 INFO * basin3d.core.synthesis * - Loading Plugin = USGSDataSourcePlugin
   2022-04-14T16:59:31.083 DEBUG * basin3d.core.catalog * - Initializing CatalogTinyDb metadata catalog
   2022-04-14T16:59:31.084 INFO * basin3d.core.catalog * - Loading metadata catalog for Plugin USGS
   2022-04-14T16:59:31.085 DEBUG * basin3d.core.catalog * - Mapping file mapping_usgs.csv for plugin package basin3d.plugins
   2022-04-14T16:59:31.086 DEBUG * basin3d.core.catalog * - Mapped 00400 to pH
   2022-04-14T16:59:31.087 DEBUG * basin3d.core.catalog * - Mapped 00060 to River Discharge
   ...


To create and output custom log messages:

1. Use :func:`basin3d.monitor.get_logger` to create a custom logger.
2. Use :func:`basin3d.monitor.configure` to configure the custom logger output.

.. code-block::

    >>> from basin3d import monitor
    >>> monitor.configure(loggers={"my_logger": {"level": "DEBUG",
    ...         'handlers': ['error-console', 'console'], 'propagate': True}})
    {'version': 1, 'incremental': False, 'disable_existing_loggers': True,
    ...
    >>> logger = monitor.get_logger("my_logger")
    >>> logger.info("My logging message")
    2022-04-14T16:57:36.163 INFO * my_logger * - My logging message


Write log messages to a file called `basin3d.log`:

.. code-block::

    >>> from basin3d import monitor, synthesis
    >>> from basin3d.plugins import usgs
    >>> monitor.configure(
    ...    handlers={  "file":
    ...       { "class":"logging.FileHandler", "formatter": "simple", "filename": ".}/basin3d.log"}},
    ...   loggers={"basin3d": {"handlers": ["console", "file"]}})
    {'version': 1, 'incremental': False, 'disable_existing_loggers': True,
    ...
    >>> synthesizer = synthesis.register()
    2022-04-14T17:32:50.502 INFO * basin3d.core.synthesis * - Loading Plugin = USGSDataSourcePlugin
    2022-04-14T17:32:50.507 INFO * basin3d.core.catalog * - Loading metadata catalog for Plugin USGS
    2022-04-14T17:32:50.509 INFO * basin3d.core.catalog * - Initialized CatalogTinyDb metadata catalog
