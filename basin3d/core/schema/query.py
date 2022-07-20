"""
`basin3d.core.schema.query`
***************************

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
from typing import List, Optional, Union

from pydantic import BaseModel, Field

from basin3d.core.schema.enum import FeatureTypeEnum, MessageLevelEnum, ResultQualityEnum, SamplingMediumEnum, StatisticEnum, TimeFrequencyEnum


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

    def list_attribute_names(self):
        # ToDo: probably don't want to use the protected class. Could use vars()?
        return [a.name for a in self.__fields__.values()]


class QueryById(QueryBase):
    """Query for a single data object by identifier"""

    id: str = Field(title="Identifier", description="The unique identifier for the desired data object")


class QueryMonitoringFeature(QueryBase):
    """Query :class:`basin3d.core.models.MonitoringFeature`"""
    feature_type: Optional[FeatureTypeEnum] = Field(title="Feature Type",
                                                    description="Filter results by the specified feature type.")
    monitoring_features: Optional[List[str]] = Field(title="Monitoring Features",
                                                     description="Filter by the list of monitoring feature identifiers")
    parent_features: Optional[List[str]] = Field(title="Parent Monitoring Features",
                                                 description="Filter by the list of parent monitoring feature identifiers")

    def __init__(self, **data):
        """
        Custom constructor to modify feature_type strings to uppercase

        :param data: the data
        """

        # convert strings to lists for some fields
        for field in ["monitoring_features", "parent_features", "monitoringFeatures", "parentFeatures"]:
            if field in data and data[field] and isinstance(data[field], str):
                data[field] = list([data[field]])

        # To upper for feature type
        for field in ["featureType", "feature_type"]:
            if field in data and data[field]:
                data[field] = isinstance(data[field], str) and data[field].upper() or data[field]
        super().__init__(**data)


class QueryMeasurementTimeseriesTVP(QueryBase):
    """Query :class:`basin3d.core.models.MeasurementTimeseriesTVP`"""
    # required
    monitoring_features: List[str] = Field(min_items=1, title="Monitoring Features",
                                           description="Filter by the list of monitoring feature identifiers")
    observed_property_variables: List[str] = Field(min_items=1, title="Observed Property Variables",
                                                   description="Filter by the list of observed property variables")
    start_date: date = Field(title="Start Date", description="Filter by data taken on or after the start date")

    # optional
    aggregation_duration: Optional[TimeFrequencyEnum] = Field(default='DAY', title="Aggregation Duration",
                                                              description="Filter by the specified time frequency")
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

        # convert strings to lists for some fields
        for field in ["monitoring_features", "observed_property_variables", "monitoringFeatures",
                      "observedPropertyVariables"]:
            if field in data and data[field] and isinstance(data[field], str):
                data[field] = list([data[field]])
        super().__init__(**data)


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
    messages: List[SynthesisMessage] = Field([], title="Messages",
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
