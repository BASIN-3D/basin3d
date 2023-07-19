"""

.. currentmodule:: basin3d.core.schema.query

:platform: Unix, Mac
:synopsis: BASIN-3D Query Schema
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top


"""
from datetime import date
from typing import ClassVar, List, Optional, Union

from pydantic import BaseModel, Field

from basin3d.core.schema.enum import FeatureTypeEnum, MessageLevelEnum, ResultQualityEnum, SamplingMediumEnum, StatisticEnum, AggregationDurationEnum


def _to_camelcase(string) -> str:
    """
        Change provided string with underscores to Javascript camelcase
        (e.g. to_camelcase -> toCamelcase)
        :param string: The string to transform
        :return:
        """
    return "".join(i and s[0].upper() + s[1:] or s for i, s in enumerate(string.split("_")))


class QueryBase(BaseModel):
    """ Query Base Class.  This sets `QueryBase.Config` defaults and processes incoming datasource ids"""

    datasource: Optional[List[str]] = Field(title="Datasource Identifiers",
                                            description="List of datasource identifiers to query by.")

    id: Optional[str] = Field(title="Identifier", description="The unique identifier for the desired object")

    is_valid_translated_query: Union[None, bool] = Field(default=None, title="Valid translated query",
                                                         description="Indicates whether the translated query is valid: None = is not translated")

    def __init__(self, **data):
        """
        Custom constructor to modify datasource string to list, if necessary

        :param data: the data
        """
        if "datasource" in data and data['datasource']:
            data['datasource'] = isinstance(data['datasource'], str) and list([data['datasource']]) or data[
                'datasource']
        super().__init__(**data)

    class Config:
        # output fields to camelcase
        alias_generator = _to_camelcase
        # whether an aliased field may be populated by its name as given by the model attribute
        #  (allows bot camelcase and underscore fields)
        allow_population_by_field_name = True
        # Instead of using enum class use enum value (string object)
        use_enum_values = True
        # Validate all fields when initialized
        validate_all = True

    # Get the query fields that have mappings. Subclasses may overwrite this base function
    mapped_fields: ClassVar[List[str]] = []

    # Get the query fields that have prefixes. Subclasses may overwrite ths base function
    prefixed_fields: ClassVar[List[str]] = []


class QueryMonitoringFeature(QueryBase):
    """Query :class:`basin3d.core.models.MonitoringFeature`"""
    # optional but id (QueryBase) is required to query by named monitoring feature
    feature_type: Optional[FeatureTypeEnum] = Field(title="Feature Type",
                                                    description="Filter results by the specified feature type.")
    monitoring_feature: Optional[List[str]] = Field(title="Monitoring Features",
                                                    description="Filter by the list of monitoring feature identifiers")
    parent_feature: Optional[List[str]] = Field(title="Parent Monitoring Features",
                                                description="Filter by the list of parent monitoring feature identifiers")

    def __init__(self, **data):
        """
        Custom constructor to modify feature_type strings to uppercase

        :param data: the data
        """

        # convert strings to lists for some fields; the camel case is for Pydantic validation
        for field in ["monitoring_feature", "monitoringFeature", "parent_feature", "parentFeature"]:
            if field in data and data[field] and isinstance(data[field], str):
                data[field] = list([data[field]])

        # To upper for feature type
        for field in ["featureType", "feature_type"]:
            if field in data and data[field]:
                data[field] = isinstance(data[field], str) and data[field].upper() or data[field]
        super().__init__(**data)

    prefixed_fields: ClassVar[List[str]] = ['id', 'monitoring_feature', 'parent_feature']


class QueryMeasurementTimeseriesTVP(QueryBase):
    """Query :class:`basin3d.core.models.MeasurementTimeseriesTVP`"""
    # required
    monitoring_feature: List[str] = Field(min_items=1, title="Monitoring Features",
                                          description="Filter by the list of monitoring feature identifiers")
    observed_property: List[str] = Field(min_items=1, title="Observed Property Variables",
                                         description="Filter by the list of observed property variables")
    start_date: date = Field(title="Start Date", description="Filter by data taken on or after the start date")

    # optional
    aggregation_duration: AggregationDurationEnum = Field(default='DAY', title="Aggregation Duration",
                                                          description="Filter by the specified aggregation duration or time frequency")
    end_date: Optional[date] = Field(title="End Date", description="Filter by data taken on or before the end date")
    statistic: Optional[List[StatisticEnum]] = Field(title="Statistic",
                                                     description="Return specified statistics, if they exist.")
    result_quality: Optional[List[ResultQualityEnum]] = Field(title="Result Quality",
                                                              description="Filter by specified result qualities")
    sampling_medium: Optional[List[SamplingMediumEnum]] = Field(title="Sampling Medium",
                                                                description="Filter results by specified sampling medium")

    def __init__(self, **data):
        """
        Custom constructor

        :param data: the data
        """

        # convert strings to lists for some fields; the camel case are for Pydantic validation (don't delete)
        for field in ["monitoring_feature", "monitoringFeature", "observed_property", "observedProperty",
                      "statistic", "result_quality", "sampling_medium"]:
            if field in data and data[field] and isinstance(data[field], str):
                data[field] = list([data[field]])

        data = self.__validate__(**data)

        super().__init__(**data)

    @staticmethod
    def __validate__(**data):
        """
        Valiate
        :return:
        """
        if 'aggregation_duration' in data and data['aggregation_duration'] is None:
            del data['aggregation_duration']
        return data

    # observed_property_variables is first b/c it is most likely to have compound mappings.
    # ToDo: check how order may affect translation (see core/synthesis)
    mapped_fields: ClassVar[List[str]] = ['observed_property', 'aggregation_duration', 'statistic', 'result_quality', 'sampling_medium']
    prefixed_fields: ClassVar[List[str]] = ['monitoring_feature']


class SynthesisMessage(BaseModel):
    """BASIN-3D Synthesis Message """

    msg: str = Field(title="Msg", description="The synthesis message ")
    level: MessageLevelEnum = Field(title="Level", description="The severity level of the message.")
    where: Optional[List[str]] = Field([], title="Where",
                                       description="The place in BASIN-3D where the synthesis message was generated "
                                                   "from. "
                                                   "If empty or null, this is a BASIN-3D error, the first item in "
                                                   "the list is the datsource id, "
                                                   "the second should be the synthesis model.")

    class Config:
        # output fields to camelcase
        alias_generator = _to_camelcase
        # whether an aliased field may be populated by its name as given by the model attribute
        #  (allows bot camelcase and underscore fields)
        allow_population_by_field_name = True
        # Instead of using enum class use enum value (string object)
        use_enum_values = True
        # Validate all fields when initialized
        validate_all = True


class SynthesisResponse(BaseModel):
    """BASIN-3D Synthesis Response """

    query: QueryBase = Field(title="Query", description="The original query for the current response")
    data: Optional[Union[object, List[object]]] = Field(title="Data",
                                                        description="The data for the current response. Empty if provided "
                                                                    "via Iterator.")
    messages: List[Optional[SynthesisMessage]] = Field([], title="Messages",
                                                       description="The synthesis messages for this response")

    class Config:
        # output fields to camelcase
        alias_generator = _to_camelcase
        # whether an aliased field may be populated by its name as given by the model attribute
        #  (allows bot camelcase and underscore fields)
        allow_population_by_field_name = True
        # Instead of using enum class use enum value (string object)
        use_enum_values = True
        # Validate all fields when initialized
        validate_all = True
        # Allows generic object to be used for data field
        arbitrary_types_allowed = True
