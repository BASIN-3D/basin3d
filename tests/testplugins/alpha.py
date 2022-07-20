import logging
from typing import Any, List

from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, DepthCoordinate, \
    GeographicCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature, \
    RepresentativeCoordinate, SpatialSamplingShapes, VerticalCoordinate, ResultListTVP
from basin3d.core.plugin import DataSourcePluginPoint, basin3d_plugin, basin3d_plugin_access
from basin3d.core.schema.enum import FeatureTypeEnum, TimeFrequencyEnum, ResultQualityEnum
from basin3d.core.schema.query import QueryById, QueryMeasurementTimeseriesTVP, QueryMonitoringFeature

logger = logging.getLogger(__name__)


@basin3d_plugin
class AlphaSourcePlugin(DataSourcePluginPoint):
    title = 'Alpha Source Plugin'

    # Question: should we use the FeatureTypeEnum CV directly?
    feature_types = ['REGION', 'POINT', 'TREE']

    class DataSourceMeta:
        """
        This is an internal metadata class for defining additional :class:`~basin3d.models.DataSource`
        attributes.

        **Attributes:**
            - *id* - unique id short name
            - *name* - human friendly name (more descriptive)
            - *location* - resource location
            - *id_prefix* - id prefix to make model object ids unique across plugins
            - *credentials_format* - if the data source requires authentication, this is where the
                format of the stored credentials is defined.

        """
        # Data Source attributes
        location = 'https://asource.foo/'
        id = 'Alpha'  # unique id for the datasource
        id_prefix = 'A'
        name = id  # Human Friendly Data Source Name


@basin3d_plugin_access(AlphaSourcePlugin, MeasurementTimeseriesTVPObservation, 'list')
def find_measurement_timeseries_tvp_observations(self, query: QueryMeasurementTimeseriesTVP):
    """ Generate the MeasurementTimeseriesTVPObservation

      Attributes:
        - *id:* string, Cs137 MR survey ID
        - *observed_property:* string, Cs137MID
        - *utc_offset:* float (offset in hours), +9
        - *geographical_group_id:* string, River system ID (Region ID).
        - *geographical_group_type* enum (sampling_feature, site, plot, region)
        - *results_points:* Array of DataPoint objects

    """
    synthesis_messages: List[str] = []
    data: List[Any] = []
    quality: List[Any] = []

    if query.monitoring_features == ['region']:
        return StopIteration({"message": "FOO"})

    from datetime import datetime
    for num in range(1, 10):
        data.append((datetime(2016, 2, num), num * 0.3454))
    data = [data, data, [], data]
    rqe1 = 'VALIDATED'
    rqe2 = 'UNVALIDATED'
    rqe3 = 'REJECTED'
    quality = [[rqe1, rqe1, rqe1, rqe1, rqe1, rqe1, rqe1, rqe1, rqe1],
               [rqe2, rqe2, rqe2, rqe2, rqe2, rqe2, rqe2, rqe3, rqe3],
               [],
               [rqe1, rqe2, rqe3, rqe1, rqe1, rqe1, rqe1, rqe1, rqe1]]
    qualities = [[rqe1],
                 [rqe2, rqe3],
                 [],
                 [rqe1, rqe2, rqe3]]
    observed_property_variables = ["Acetate", "Acetate", "Aluminum", "Aluminum"]
    units = ['nm', 'nm', 'mg/L', 'mg/L']
    statistics = ['mean', 'max', 'mean', 'max']

    for num in range(1, 5):
        observed_property_variable = observed_property_variables[num - 1]
        feature_id = f'A-{str(num - 1)}'
        if query:
            if query.monitoring_features:
                if str(num) not in query.monitoring_features:
                    continue
            if query.statistic:
                if statistics[num - 1] not in query.statistic:
                    continue
            result_value = data[num - 1]
            result_value_quality = quality[num - 1]
            result_qualities = qualities[num - 1]
            if query.result_quality:
                filtered_value = []
                filtered_quality = []
                has_filtered_data_points = 0

                for v, q in zip(result_value, result_value_quality):
                    if q in query.result_quality:
                        filtered_value.append(v)
                        filtered_quality.append(q)
                    else:
                        has_filtered_data_points += 1

                if has_filtered_data_points > 0:
                    synthesis_messages.append(f'{feature_id} - {observed_property_variable}: {str(has_filtered_data_points)} timestamps did not match data quality query.')

                if len(filtered_value) == 0:
                    synthesis_messages.append(f'{feature_id} - {observed_property_variable}: No data values matched result_quality query.')
                    print(f'{feature_id} - {observed_property_variable}')
                    continue

                result_value = filtered_value
                result_value_quality = filtered_quality
                if len(result_value_quality) > 0:
                    result_qualities = list(set(result_value_quality))
                else:
                    result_qualities = []

        yield MeasurementTimeseriesTVPObservation(
            plugin_access=self,
            id=num,
            observed_property=observed_property_variable,
            utc_offset=-8 - num,
            feature_of_interest=MonitoringFeature(
                plugin_access=self,
                id=num,
                name="Point Location " + str(num),
                description="The point.",
                feature_type=FeatureTypeEnum.POINT,
                shape=SpatialSamplingShapes.SHAPE_POINT,
                coordinates=Coordinate(
                    absolute=AbsoluteCoordinate(
                        horizontal_position=GeographicCoordinate(
                            units=GeographicCoordinate.UNITS_DEC_DEGREES,
                            latitude=70.4657, longitude=-20.4567),
                        vertical_extent=AltitudeCoordinate(
                            datum=AltitudeCoordinate.DATUM_NAVD88,
                            value=1500,
                            distance_units=VerticalCoordinate.DISTANCE_UNITS_FEET)),
                    representative=RepresentativeCoordinate(
                        vertical_position=DepthCoordinate(
                            datum=DepthCoordinate.DATUM_LOCAL_SURFACE,
                            value=-0.5 - num * 0.1,
                            distance_units=VerticalCoordinate.DISTANCE_UNITS_METERS)
                    )
                ),
                observed_property_variables=["Ag", "Acetate", "Aluminum"],
                related_sampling_feature_complex=[
                    RelatedSamplingFeature(plugin_access=self,
                                           related_sampling_feature="Region1",
                                           related_sampling_feature_type=FeatureTypeEnum.REGION,
                                           role=RelatedSamplingFeature.ROLE_PARENT)]
            ),
            feature_of_interest_type=FeatureTypeEnum.POINT,
            unit_of_measurement=units[num - 1],
            aggregation_duration=TimeFrequencyEnum.DAY,
            result_quality=result_qualities,
            time_reference_position=None,
            statistic=statistics[num - 1],
            result=ResultListTVP(plugin_access=self, value=result_value, quality=result_value_quality)
        )

    return StopIteration(synthesis_messages)


@basin3d_plugin_access(AlphaSourcePlugin, MeasurementTimeseriesTVPObservation, 'get')
def get_measurement_timeseries_tvp_observation(self, query: QueryById):
    """
        Get a MeasurementTimeseriesTVPObservation
        :param query:
    """
    if query:
        for s in self.list():
            if s.id.endswith(query.id):
                return s
    return None


@basin3d_plugin_access(AlphaSourcePlugin, MonitoringFeature, 'list')
def list_monitoring_features(self, query: QueryMonitoringFeature):
    """
    Get Monitoring Feature Info
    """
    assert query

    obj_region = self.synthesis_model_class(
        plugin_access=self,
        id="Region1",
        name="AwesomeRegion",
        description="This region is really awesome.",
        feature_type=FeatureTypeEnum.REGION,
        shape=SpatialSamplingShapes.SHAPE_SURFACE,
        coordinates=Coordinate(representative=RepresentativeCoordinate(
            representative_point=AbsoluteCoordinate(
                horizontal_position=GeographicCoordinate(
                    units=GeographicCoordinate.UNITS_DEC_DEGREES,
                    latitude=70.4657, longitude=-20.4567),
                vertical_extent=AltitudeCoordinate(
                    datum=AltitudeCoordinate.DATUM_NAVD88,
                    value=1500,
                    distance_units=VerticalCoordinate.DISTANCE_UNITS_FEET)),
            representative_point_type=RepresentativeCoordinate.REPRESENTATIVE_POINT_TYPE_CENTER_LOCAL_SURFACE)
        )
    )

    yield obj_region

    obj_point = self.synthesis_model_class(
        plugin_access=self,
        id="1",
        name="Point Location 1",
        description="The first point.",
        feature_type=FeatureTypeEnum.POINT,
        shape=SpatialSamplingShapes.SHAPE_POINT,
        coordinates=Coordinate(
            absolute=AbsoluteCoordinate(
                horizontal_position=GeographicCoordinate(
                    units=GeographicCoordinate.UNITS_DEC_DEGREES,
                    latitude=70.4657, longitude=-20.4567),
                vertical_extent=AltitudeCoordinate(
                    datum=AltitudeCoordinate.DATUM_NAVD88,
                    value=1500,
                    distance_units=VerticalCoordinate.DISTANCE_UNITS_FEET)),
            representative=RepresentativeCoordinate(
                vertical_position=DepthCoordinate(
                    datum=DepthCoordinate.DATUM_LOCAL_SURFACE,
                    value=-0.5,
                    distance_units=VerticalCoordinate.DISTANCE_UNITS_METERS)
            )
        ),
        observed_property_variables=["Ag", "Acetate"],
        related_sampling_feature_complex=[
            RelatedSamplingFeature(plugin_access=self,
                                   related_sampling_feature="Region1",
                                   related_sampling_feature_type=FeatureTypeEnum.REGION,
                                   role=RelatedSamplingFeature.ROLE_PARENT)]
    )

    yield obj_point

    return StopIteration(['message1', 'message2', 'message3'])


@basin3d_plugin_access(AlphaSourcePlugin, MonitoringFeature, 'get')
def get_monitoring_feature(self, query: QueryById):
    """
    Get a MonitoringFeature
    :param pk: primary key
    """
    if query:
        for s in self.list():
            if s.id.endswith(query.id):
                return s
    return None
