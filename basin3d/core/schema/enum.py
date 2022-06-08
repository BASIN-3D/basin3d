"""
`basin3d.core.schema.enum`
***************************

.. currentmodule:: basin3d.core.schema.enum

:platform: Unix, Mac
:synopsis: BASIN-3D Enumeration Schema
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top


"""
from enum import Enum

from basin3d.core.types import SpatialSamplingShapes

# From https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
PANDAS_TIME_FREQUENCY_MAP = {
    'YEAR': 'A',
    'MONTH': 'M',
    'DAY': 'D',
    'HOUR': 'H',
    'MINUTE': 'T',
    'SECOND': 'S'
}


class BaseEnum(Enum):
    """Base Enumeration Class that adds some helper methods"""

    @classmethod
    def values(cls):
        role_names = [member.value for role, member in cls.__members__.items()]
        return role_names

    @classmethod
    def names(cls):
        return cls._member_names_


class MappedAttributeEnum(str, BaseEnum):
    """
    Enumeration for attributes
    """
    TIME_FREQUENCY = 'TIME_FREQUENCY'
    STATISTIC = 'STATISTIC'
    RESULT_QUALITY = 'RESULT_QUALITY'
    # ToDo: see if we can add FeatureType


class TimeFrequencyEnum(str, BaseEnum):
    """
    Enumeration for time frequencies
    """
    # ToDo: Check OGC for correct term (i.e., replace TimeFrequency)

    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    NONE = "NONE"
    NOT_SUPPORTED = 'NOT_SUPPORTED'


class FeatureTypeEnum(str, BaseEnum):
    """Enumeration for Feature Types"""
    REGION = "REGION"
    SUBREGION = "SUBREGION"
    BASIN = "BASIN"
    SUBBASIN = "SUBBASIN"
    WATERSHED = "WATERSHED"
    SUBWATERSHED = "SUBWATERSHED"
    SITE = "SITE"
    PLOT = "PLOT"
    HORIZONTAL_PATH = "HORIZONTAL PATH"
    VERTICAL_PATH = "VERTICAL PATH"
    POINT = "POINT"


FEATURE_SHAPE_TYPES = {
    SpatialSamplingShapes.SHAPE_POINT: [FeatureTypeEnum.POINT],
    SpatialSamplingShapes.SHAPE_CURVE: [FeatureTypeEnum.HORIZONTAL_PATH, FeatureTypeEnum.VERTICAL_PATH],
    SpatialSamplingShapes.SHAPE_SURFACE: [FeatureTypeEnum.REGION, FeatureTypeEnum.SUBREGION, FeatureTypeEnum.BASIN,
                                          FeatureTypeEnum.SUBBASIN,
                                          FeatureTypeEnum.WATERSHED,
                                          FeatureTypeEnum.SUBWATERSHED, FeatureTypeEnum.SITE, FeatureTypeEnum.PLOT],
    SpatialSamplingShapes.SHAPE_SOLID: []
}


class ResultQualityEnum(str, BaseEnum):
    """Enumeration for Result Quality"""

    #: The result is raw or unchecked for quality. Synonyms: Unchecked, Preliminary, No QC
    UNVALIDATED = "UNVALIDATED"

    #: The result has been checked for quality and no issues identified. Synonyms: Checked, Accepted, Pass, OK, Good
    VALIDATED = "VALIDATED"

    #: The result is identified as poor quality. Synonyms: Poor, Bad, Unaccepted
    REJECTED = "REJECTED"

    #: The result's quality is suspect. Synonyms: Questionable, Doubtful, Spike/Noise, Flagged
    SUSPECTED = "SUSPECTED"

    #: The result is estimated. Synonyms: Interpolated, Modeled.
    ESTIMATED = "ESTIMATED"

    #: The quality type is not supported
    NOT_SUPPORTED = "NOT_SUPPORTED"


class StatisticEnum(str, BaseEnum):
    """Enumeration for Statistics"""
    INSTANT = "INSTANT"
    MEAN = "MEAN"
    MIN = "MIN"
    MAX = "MAX"
    TOTAL = "TOTAL"
    NOT_SUPPORTED = 'NOT_SUPPORTED'


class MessageLevelEnum(str, BaseEnum):
    """Enumeration of Message Levels"""

    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
