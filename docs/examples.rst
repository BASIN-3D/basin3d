.. _basin3dexamples:

Examples
********

.. _monitoring_feature_object_example:

Monitoring Feature Object Structure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The structure of a Monitoring Feature object is shown below for the first object of the iterator resulting from the specified query.

.. code-block::

    >>> from basin3d.plugins import usgs
    >>> from basin3d import synthesis
    >>> synthesizer = synthesis.register()
    >>> monitoring_features = synthesizer.monitoring_features(feature_type='point', monitoring_feature=['USGS-13010000', 'USGS-385508107021201'])
    >>> for mf in monitoring_features:
    ...     print(mf.to_json())
    {
        "coordinates": {
            "absolute": {
                "datasource": null,
                "datasource_ids": null,
                "horizontal_position": [
                    {
                        "datasource": null,
                        "datasource_ids": null,
                        "datum": "NAD83",
                        "id": null,
                        "original_id": null,
                        "type": "GEOGRAPHIC",
                        "units": "DD",
                        "x": -110.6647222,
                        "y": 44.1336111
                    }
                ],
                "id": null,
                "original_id": null,
                "vertical_extent": [
                    {
                        "datasource": null,
                        "datasource_ids": null,
                        "datum": "NGVD29",
                        "distance_units": null,
                        "encoding_method": null,
                        "id": null,
                        "original_id": null,
                        "resolution": 20.0,
                        "type": "ALTITUDE",
                        "value": 6880.0
                    }
                ]
            },
            "datasource": null,
            "datasource_ids": null,
            "id": null,
            "original_id": null,
            "representative": null
        },
        "datasource": {
            "credentials": {},
            "id": "USGS",
            "id_prefix": "USGS",
            "location": "https://waterservices.usgs.gov/nwis/",
            "name": "USGS"
        },
        "datasource_ids": null,
        "description": null,
        "description_reference": null,
        "feature_type": "POINT",
        "id": "USGS-13010000",
        "name": "SNAKE RIVER AT S BOUNDARY OF YELLOWSTONE NATL PARK",
        "observed_properties": [
            {
                "attr_mapping": {
                    "attr_type": "OBSERVED_PROPERTY:SAMPLING_MEDIUM",
                    "basin3d_desc": [
                        {
                            "basin3d_vocab": "RDC",
                            "categories": [
                                "Hydrogeology",
                                "Water Physical/Quality Parameters"
                            ],
                            "full_name": "River Discharge",
                            "units": "mV"
                        },
                        "WATER"
                    ],
                    "basin3d_vocab": "RDC:WATER",
                    "datasource": {
                        "credentials": {},
                        "id": "USGS",
                        "id_prefix": "USGS",
                        "location": "https://waterservices.usgs.gov/nwis/",
                        "name": "USGS"
                    },
                    "datasource_desc": "Discharge, cubic feet per second",
                    "datasource_vocab": "00060"
                },
                "attr_type": "OBSERVED_PROPERTY"
            }
        ],
        "original_id": "13010000",
        "related_party": [],
        "related_sampling_feature_complex": [
            {
                "datasource": {
                    "credentials": {},
                    "id": "USGS",
                    "id_prefix": "USGS",
                    "location": "https://waterservices.usgs.gov/nwis/",
                    "name": "USGS"
                },
                "datasource_ids": [
                    "related_sampling_feature"
                ],
                "id": null,
                "original_id": null,
                "related_sampling_feature": "USGS-17040101",
                "related_sampling_feature_type": "SUBBASIN",
                "role": "PARENT"
            }
        ],
        "shape": "POINT",
        "utc_offset": null
    }
    ...

.. _measurement_timeseries_tvp_object_example:

Measurement Timeseries TVP Observations Object Structure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The structure of a Measurement Timeseries TVP Observations object is shown below for the first object of the iterator resulting from the specified query.

For other output formats, see `basin3d-views <https://github.com/BASIN-3D/basin3d-views>`_.

.. code-block::

    >>> from basin3d.plugins import usgs
    >>> from basin3d import synthesis
    >>> synthesizer = synthesis.register()
    >>> measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(monitoring_feature=['USGS-09110990'], observed_property=['RDC'], start_date='2019-10-01', end_date='2019-10-05', datasource='USGS')
    >>> for mtvpo in measurement_timeseries_tvp_observations:
    ...     print(mtvpo.to_json())
    {
        "aggregation_duration": {
            "attr_mapping": {
                "attr_type": "AGGREGATION_DURATION",
                "basin3d_desc": [
                    "DAY"
                ],
                "basin3d_vocab": "DAY",
                "datasource": {
                    "credentials": {},
                    "id": "USGS",
                    "id_prefix": "USGS",
                    "location": "https://waterservices.usgs.gov/nwis/",
                    "name": "USGS"
                },
                "datasource_desc": "",
                "datasource_vocab": "DAY"
            },
            "attr_type": "AGGREGATION_DURATION"
        },
        "datasource": {
            "credentials": {},
            "id": "USGS",
            "id_prefix": "USGS",
            "location": "https://waterservices.usgs.gov/nwis/",
            "name": "USGS"
        },
        "datasource_ids": null,
        "feature_of_interest": {
            "coordinates": {
                "absolute": {
                    "datasource": null,
                    "datasource_ids": null,
                    "horizontal_position": [
                        {
                            "datasource": null,
                            "datasource_ids": null,
                            "datum": "NAD83",
                            "id": null,
                            "original_id": null,
                            "type": "GEOGRAPHIC",
                            "units": "DD",
                            "x": -107.0597722,
                            "y": 38.85665
                        }
                    ],
                    "id": null,
                    "original_id": null,
                    "vertical_extent": [
                        {
                            "datasource": null,
                            "datasource_ids": null,
                            "datum": "NAVD88",
                            "distance_units": null,
                            "encoding_method": null,
                            "id": null,
                            "original_id": null,
                            "resolution": 4.3,
                            "type": "ALTITUDE",
                            "value": 9570.0
                        }
                    ]
                },
                "datasource": null,
                "datasource_ids": null,
                "id": null,
                "original_id": null,
                "representative": null
            },
            "datasource": {
                "credentials": {},
                "id": "USGS",
                "id_prefix": "USGS",
                "location": "https://waterservices.usgs.gov/nwis/",
                "name": "USGS"
            },
            "datasource_ids": null,
            "description": null,
            "description_reference": null,
            "feature_type": "POINT",
            "id": "USGS-09110990",
            "name": "ELK CREEK AT COAL CREEK ABV CRESTED BUTTE, CO",
            "observed_properties": [
                {
                    "attr_mapping": {
                        "attr_type": "OBSERVED_PROPERTY:SAMPLING_MEDIUM",
                        "basin3d_desc": [
                            {
                                "basin3d_vocab": "RDC",
                                "categories": [
                                    "Hydrogeology",
                                    "Water Physical/Quality Parameters"
                                ],
                                "full_name": "River Discharge",
                                "units": "mV"
                            },
                            "WATER"
                        ],
                        "basin3d_vocab": "RDC:WATER",
                        "datasource": {
                            "credentials": {},
                            "id": "USGS",
                            "id_prefix": "USGS",
                            "location": "https://waterservices.usgs.gov/nwis/",
                            "name": "USGS"
                        },
                        "datasource_desc": "Discharge, cubic feet per second",
                        "datasource_vocab": "00060"
                    },
                    "attr_type": "OBSERVED_PROPERTY"
                },
                {
                    "attr_mapping": {
                        "attr_type": "OBSERVED_PROPERTY",
                        "basin3d_desc": [],
                        "basin3d_vocab": "NOT_SUPPORTED",
                        "datasource": {
                            "credentials": {},
                            "id": "USGS",
                            "id_prefix": "USGS",
                            "location": "https://waterservices.usgs.gov/nwis/",
                            "name": "USGS"
                        },
                        "datasource_desc": "No mapping was found for datasource vocab: \"00065\" in datasource: \"USGS\".",
                        "datasource_vocab": "00065"
                    },
                    "attr_type": "OBSERVED_PROPERTY"
                }
            ],
            "original_id": "09110990",
            "related_party": [],
            "related_sampling_feature_complex": [
                {
                    "datasource": {
                        "credentials": {},
                        "id": "USGS",
                        "id_prefix": "USGS",
                        "location": "https://waterservices.usgs.gov/nwis/",
                        "name": "USGS"
                    },
                    "datasource_ids": [
                        "related_sampling_feature"
                    ],
                    "id": null,
                    "original_id": null,
                    "related_sampling_feature": "USGS-14020001",
                    "related_sampling_feature_type": "SUBBASIN",
                    "role": "PARENT"
                }
            ],
            "shape": "POINT",
            "utc_offset": null
        },
        "feature_of_interest_type": "POINT",
        "id": "USGS-09110990",
        "observed_property": {
            "attr_mapping": {
                "attr_type": "OBSERVED_PROPERTY:SAMPLING_MEDIUM",
                "basin3d_desc": [
                    {
                        "basin3d_vocab": "RDC",
                        "categories": [
                            "Hydrogeology",
                            "Water Physical/Quality Parameters"
                        ],
                        "full_name": "River Discharge",
                        "units": "mV"
                    },
                    "WATER"
                ],
                "basin3d_vocab": "RDC:WATER",
                "datasource": {
                    "credentials": {},
                    "id": "USGS",
                    "id_prefix": "USGS",
                    "location": "https://waterservices.usgs.gov/nwis/",
                    "name": "USGS"
                },
                "datasource_desc": "Discharge, cubic feet per second",
                "datasource_vocab": "00060"
            },
            "attr_type": "OBSERVED_PROPERTY"
        },
        "original_id": "09110990",
        "phenomenon_time": null,
        "result": {
            "datasource": {
                "credentials": {},
                "id": "USGS",
                "id_prefix": "USGS",
                "location": "https://waterservices.usgs.gov/nwis/",
                "name": "USGS"
            },
            "datasource_ids": null,
            "id": null,
            "original_id": null,
            "result_quality": [
                {
                    "attr_mapping": {
                        "attr_type": "RESULT_QUALITY",
                        "basin3d_desc": [
                            "VALIDATED"
                        ],
                        "basin3d_vocab": "VALIDATED",
                        "datasource": {
                            "credentials": {},
                            "id": "USGS",
                            "id_prefix": "USGS",
                            "location": "https://waterservices.usgs.gov/nwis/",
                            "name": "USGS"
                        },
                        "datasource_desc": "Approved for publication -- Processing and review completed.",
                        "datasource_vocab": "A"
                    },
                    "attr_type": "RESULT_QUALITY"
                },
                {
                    "attr_mapping": {
                        "attr_type": "RESULT_QUALITY",
                        "basin3d_desc": [
                            "VALIDATED"
                        ],
                        "basin3d_vocab": "VALIDATED",
                        "datasource": {
                            "credentials": {},
                            "id": "USGS",
                            "id_prefix": "USGS",
                            "location": "https://waterservices.usgs.gov/nwis/",
                            "name": "USGS"
                        },
                        "datasource_desc": "Approved for publication -- Processing and review completed.",
                        "datasource_vocab": "A"
                    },
                    "attr_type": "RESULT_QUALITY"
                },
                {
                    "attr_mapping": {
                        "attr_type": "RESULT_QUALITY",
                        "basin3d_desc": [
                            "VALIDATED"
                        ],
                        "basin3d_vocab": "VALIDATED",
                        "datasource": {
                            "credentials": {},
                            "id": "USGS",
                            "id_prefix": "USGS",
                            "location": "https://waterservices.usgs.gov/nwis/",
                            "name": "USGS"
                        },
                        "datasource_desc": "Approved for publication -- Processing and review completed.",
                        "datasource_vocab": "A"
                    },
                    "attr_type": "RESULT_QUALITY"
                },
                {
                    "attr_mapping": {
                        "attr_type": "RESULT_QUALITY",
                        "basin3d_desc": [
                            "VALIDATED"
                        ],
                        "basin3d_vocab": "VALIDATED",
                        "datasource": {
                            "credentials": {},
                            "id": "USGS",
                            "id_prefix": "USGS",
                            "location": "https://waterservices.usgs.gov/nwis/",
                            "name": "USGS"
                        },
                        "datasource_desc": "Approved for publication -- Processing and review completed.",
                        "datasource_vocab": "A"
                    },
                    "attr_type": "RESULT_QUALITY"
                },
                {
                    "attr_mapping": {
                        "attr_type": "RESULT_QUALITY",
                        "basin3d_desc": [
                            "VALIDATED"
                        ],
                        "basin3d_vocab": "VALIDATED",
                        "datasource": {
                            "credentials": {},
                            "id": "USGS",
                            "id_prefix": "USGS",
                            "location": "https://waterservices.usgs.gov/nwis/",
                            "name": "USGS"
                        },
                        "datasource_desc": "Approved for publication -- Processing and review completed.",
                        "datasource_vocab": "A"
                    },
                    "attr_type": "RESULT_QUALITY"
                }
            ],
            "value": [
                [
                    "2019-10-01T00:00:00.000",
                    0.01076040186
                ],
                [
                    "2019-10-02T00:00:00.000",
                    0.01047723339
                ],
                [
                    "2019-10-03T00:00:00.000",
                    0.010194064919999999
                ],
                [
                    "2019-10-04T00:00:00.000",
                    0.01076040186
                ],
                [
                    "2019-10-05T00:00:00.000",
                    0.010194064919999999
                ]
            ]
        },
        "result_quality": [
            {
                "attr_mapping": {
                    "attr_type": "RESULT_QUALITY",
                    "basin3d_desc": [
                        "VALIDATED"
                    ],
                    "basin3d_vocab": "VALIDATED",
                    "datasource": {
                        "credentials": {},
                        "id": "USGS",
                        "id_prefix": "USGS",
                        "location": "https://waterservices.usgs.gov/nwis/",
                        "name": "USGS"
                    },
                    "datasource_desc": "Approved for publication -- Processing and review completed.",
                    "datasource_vocab": "A"
                },
                "attr_type": "RESULT_QUALITY"
            }
        ],
        "sampling_medium": {
            "attr_mapping": {
                "attr_type": "OBSERVED_PROPERTY:SAMPLING_MEDIUM",
                "basin3d_desc": [
                    {
                        "basin3d_vocab": "RDC",
                        "categories": [
                            "Hydrogeology",
                            "Water Physical/Quality Parameters"
                        ],
                        "full_name": "River Discharge",
                        "units": "mV"
                    },
                    "WATER"
                ],
                "basin3d_vocab": "RDC:WATER",
                "datasource": {
                    "credentials": {},
                    "id": "USGS",
                    "id_prefix": "USGS",
                    "location": "https://waterservices.usgs.gov/nwis/",
                    "name": "USGS"
                },
                "datasource_desc": "Discharge, cubic feet per second",
                "datasource_vocab": "00060"
            },
            "attr_type": "SAMPLING_MEDIUM"
        },
        "statistic": {
            "attr_mapping": {
                "attr_type": "STATISTIC",
                "basin3d_desc": [
                    "MEAN"
                ],
                "basin3d_vocab": "MEAN",
                "datasource": {
                    "credentials": {},
                    "id": "USGS",
                    "id_prefix": "USGS",
                    "location": "https://waterservices.usgs.gov/nwis/",
                    "name": "USGS"
                },
                "datasource_desc": "",
                "datasource_vocab": "00003"
            },
            "attr_type": "STATISTIC"
        },
        "time_reference_position": "MIDDLE",
        "type": "MEASUREMENT_TVP_TIMESERIES",
        "unit_of_measurement": "m^3/s",
        "utc_offset": -7
    }


