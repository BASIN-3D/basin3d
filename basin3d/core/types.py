"""
`basin3d.core.types`
************************

.. currentmodule:: basin3d.core.synthesis

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` type classes
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""


class SpatialSamplingShapes(object):
    """
    Spatial sampling shape describing a spatial sampling feature

    Controlled CV list as defined by OGC Observation & Measurement GM_Shape.
    """

    #: The shape of a spatially extensive sampling feature which provides a complete sampling domain.
    SHAPE_SOLID = "SOLID"

    #: The shape of a spatially extensive sampling feature which provides a complete sampling domain.
    SHAPE_SURFACE = "SURFACE"

    #: The shape of a spatially extensive sampling feature which provides a complete sampling domain.
    SHAPE_CURVE = "CURVE"

    #: The shape of a spatially extensive sampling feature which provides a complete sampling domain.
    SHAPE_POINT = "POINT"


class FeatureTypes(object):
    """
    Feature Types where an Observation can be made.

    Controlled CV list that is maintained. USGS Watershed Boundry Dataset is used.
    The goal is to strike a balance between commonly used hierarchical levels and features
    versus a runaway list of FeatureTypes. OGC O&M suggests that Features should be
    determined as needed.
    """

    REGION = 0
    SUBREGION = 1
    BASIN = 2
    SUBBASIN = 3
    WATERSHED = 4
    SUBWATERSHED = 5
    SITE = 6
    PLOT = 7
    HORIZONTAL_PATH = 8  # Rivers, Transects
    VERTICAL_PATH = 9  # Towers, Boreholes, Trees, Pits
    POINT = 10

    TYPES = {
        REGION: "REGION",
        SUBREGION: "SUBREGION",
        BASIN: "BASIN",
        SUBBASIN: "SUBBASIN",
        WATERSHED: "WATERSHED",
        SUBWATERSHED: "SUBWATERSHED",
        SITE: "SITE",
        PLOT: "PLOT",
        HORIZONTAL_PATH: "HORIZONTAL PATH",
        VERTICAL_PATH: "VERTICAL PATH",
        POINT: "POINT"
    }

    SHAPE_TYPES = {
        SpatialSamplingShapes.SHAPE_POINT: [POINT],
        SpatialSamplingShapes.SHAPE_CURVE: [HORIZONTAL_PATH, VERTICAL_PATH],
        SpatialSamplingShapes.SHAPE_SURFACE: [REGION, SUBREGION, BASIN, SUBBASIN, WATERSHED,
                                              SUBWATERSHED, SITE, PLOT],
        SpatialSamplingShapes.SHAPE_SOLID: []
    }


class ResultQuality(object):
    """
    Controlled Vocabulary for result quality assessment
    """

    #: The result has been checked for quality
    RESULT_QUALITY_CHECKED = "CHECKED"

    #: The result is raw or unchecked for quality
    RESULT_QUALITY_UNCHECKED = "UNCHECKED"

    #: The result contains checked and unchecked portions
    RESULT_QUALITY_PARTIALLY_CHECKED = "PARTIALLY_CHECKED"


class SamplingMedium:
    """
    Types of sampling mediums for Observed Properties
    """

    SOLID_PHASE = "SOLID PHASE"
    WATER = "WATER"
    GAS = "GAS"
    OTHER = "OTHER"
    NOT_APPLICABLE = "N/A"
    SAMPLING_MEDIUMS = [WATER, GAS, SOLID_PHASE, OTHER, NOT_APPLICABLE]


class TimeFrequency:
    """
    Types of time frequencies
    """
    # ToDo: Check OGC for correct term (i.e., replace TimeFrequency)

    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"

    # From https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
    PANDAS_FREQUENCY_MAP = {
        YEAR: 'A',
        MONTH: 'M',
        DAY: 'D',
        HOUR: 'H',
        MINUTE: 'T',
        SECOND: 'S'
    }
