"""

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

MAPPING_DELIMITER = ':'
NO_MAPPING_TEXT = 'NOT_SUPPORTED'


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
    Enumeration for mapped attributes
    """
    OBSERVED_PROPERTY = 'OBSERVED_PROPERTY'
    AGGREGATION_DURATION = 'AGGREGATION_DURATION'
    RESULT_QUALITY = 'RESULT_QUALITY'
    SAMPLING_MEDIUM = 'SAMPLING_MEDIUM'
    STATISTIC = 'STATISTIC'


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
    NOT_SUPPORTED = NO_MAPPING_TEXT


class AggregationDurationEnum(str, BaseEnum):
    """
    Aggregation Duration enums
    """
    #: Observations aggregated by year
    YEAR = TimeFrequencyEnum.YEAR.value

    #: Observations aggregated by month
    MONTH = TimeFrequencyEnum.MONTH.value

    #: Observations aggregated by day
    DAY = TimeFrequencyEnum.DAY.value

    #: Observations aggregated by hour
    HOUR = TimeFrequencyEnum.HOUR.value

    #: Observations aggregated by minute
    MINUTE = TimeFrequencyEnum.MINUTE.value

    #: Observations aggregated by second
    SECOND = TimeFrequencyEnum.SECOND.value

    #: Observations aggregated by no standard frequency, used for instantaneous values
    NONE = TimeFrequencyEnum.NONE.value


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
    HORIZONTAL_PATH = "HORIZONTAL_PATH"
    VERTICAL_PATH = "VERTICAL_PATH"
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
    NOT_SUPPORTED = NO_MAPPING_TEXT


class StatisticEnum(str, BaseEnum):
    """Enumeration for Statistics"""
    INSTANT = "INSTANT"
    MEAN = "MEAN"
    MIN = "MIN"
    MAX = "MAX"
    TOTAL = "TOTAL"
    NOT_SUPPORTED = NO_MAPPING_TEXT


class SamplingMediumEnum(str, BaseEnum):
    """
    Types of sampling mediums for Observed Properties
    """
    SOLID_PHASE = "SOLID_PHASE"
    WATER = "WATER"
    GAS = "GAS"
    OTHER = "OTHER"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_SUPPORTED = NO_MAPPING_TEXT


class MessageLevelEnum(str, BaseEnum):
    """Enumeration of Message Levels"""

    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def set_mapped_attribute_enum_type(attr_type: str):
    """
    Return the enum type for the specified MappedAttributeEnum.
    :param attr_type: MappedAttributeEnum
    :return: the type's Enum
    """
    if attr_type == MappedAttributeEnum.AGGREGATION_DURATION:
        return AggregationDurationEnum
    elif attr_type == MappedAttributeEnum.RESULT_QUALITY:
        return ResultQualityEnum
    elif attr_type == MappedAttributeEnum.SAMPLING_MEDIUM:
        return SamplingMediumEnum
    elif attr_type == MappedAttributeEnum.STATISTIC:
        return StatisticEnum
    else:
        return None
