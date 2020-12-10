import logging

from basin3d.core.types import TimeFrequency
from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, DepthCoordinate, FeatureTypes, \
    GeographicCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature, \
    RepresentativeCoordinate, SpatialSamplingShapes, VerticalCoordinate
from basin3d.core.plugin import DataSourcePluginPoint, basin3d_plugin, basin3d_plugin_access

logger = logging.getLogger(__name__)


@basin3d_plugin
class AlphaSourcePlugin(DataSourcePluginPoint):
    title = 'Alpha Source Plugin'

    # Question: should we use the FeatureTypes CV directly?
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
def find_measurement_timeseries_tvp_observations(self, **kwargs):
    """ Generate the MeasurementTimeseriesTVPObservation

      Attributes:
        - *id:* string, Cs137 MR survey ID
        - *observed_property:* string, Cs137MID
        - *utc_offset:* float (offset in hours), +9
        - *geographical_group_id:* string, River system ID (Region ID).
        - *geographical_group_type* enum (sampling_feature, site, plot, region)
        - *results_points:* Array of DataPoint objects

    """
    data = []
    from datetime import datetime
    for num in range(1, 10):
        data.append((datetime(2016, 2, num), num * 0.3454))

    for num in range(1, 3):
        yield MeasurementTimeseriesTVPObservation(
            plugin_access=self,
            id=num,
            observed_property_variable="Acetate",
            utc_offset=-8 - num,
            feature_of_interest=MonitoringFeature(
                plugin_access=self,
                id=num,
                name="Point Location " + str(num),
                description="The point.",
                feature_type=FeatureTypes.POINT,
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
                observed_property_variables=["Ag", "Acetate"],
                related_sampling_feature_complex=[
                    RelatedSamplingFeature(plugin_access=self,
                                           related_sampling_feature="Region1",
                                           related_sampling_feature_type=FeatureTypes.REGION,
                                           role=RelatedSamplingFeature.ROLE_PARENT)]
            ),
            feature_of_interest_type=FeatureTypes.POINT,
            unit_of_measurement="nm",
            aggregation_duration=TimeFrequency.DAY,
            result_quality="CHECKED",
            time_reference_position=None,
            statistic="MEAN",
            result_points=data
        )


@basin3d_plugin_access(AlphaSourcePlugin, MeasurementTimeseriesTVPObservation, 'get')
def get_measurement_timeseries_tvp_observation(self, pk=None):
    """
        Get a MeasurementTimeseriesTVPObservation
        :param pk: primary key
    """
    for s in self.list():
        if s.id.endswith(pk):
            return s
    return None


@basin3d_plugin_access(AlphaSourcePlugin, MonitoringFeature, 'list')
def list_monitoring_features(self, **kwargs):
    """
    Get Monitoring Feature Info
    """

    obj_region = self.synthesis_model_class(
        plugin_access=self,
        id="Region1",
        name="AwesomeRegion",
        description="This region is really awesome.",
        feature_type=FeatureTypes.REGION,
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
        feature_type=FeatureTypes.POINT,
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
                                   related_sampling_feature_type=FeatureTypes.REGION,
                                   role=RelatedSamplingFeature.ROLE_PARENT)]
    )

    yield obj_point


@basin3d_plugin_access(AlphaSourcePlugin, MonitoringFeature, 'get')
def get_monitoring_feature(self, pk=None):
    """
    Get a MonitoringFeature
    :param pk: primary key
    """
    for s in self.list():
        if s.id.endswith(pk):
            return s
    return None
