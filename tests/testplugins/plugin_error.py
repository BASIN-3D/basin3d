import logging
from typing import Any, List

from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, DepthCoordinate, \
    GeographicCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature, RelatedSamplingFeature, \
    RepresentativeCoordinate, SpatialSamplingShapes, VerticalCoordinate
from basin3d.core.plugin import DataSourcePluginPoint, basin3d_plugin, basin3d_plugin_access
from basin3d.core.schema.enum import FeatureTypeEnum, TimeFrequencyEnum
from basin3d.core.schema.query import QueryById, QueryMeasurementTimeseriesTVP, QueryMonitoringFeature

logger = logging.getLogger(__name__)


@basin3d_plugin
class ErrorSourcePlugin(DataSourcePluginPoint):
    title = 'Error Source Plugin'

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
        id = 'Error'  # unique id for the datasource
        id_prefix = 'E'
        name = id  # Human Friendly Data Source Name


@basin3d_plugin_access(ErrorSourcePlugin, MeasurementTimeseriesTVPObservation, 'list')
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
    raise Exception("This is a find_measurement_timeseries_tvp_observations error")


@basin3d_plugin_access(ErrorSourcePlugin, MeasurementTimeseriesTVPObservation, 'get')
def get_measurement_timeseries_tvp_observation(self, query: QueryById):
    """
        Get a MeasurementTimeseriesTVPObservation
        :param query:
    """
    raise Exception("This is a get_measurement_timeseries_tvp_observation error")


@basin3d_plugin_access(ErrorSourcePlugin, MonitoringFeature, 'list')
def list_monitoring_features(self, query: QueryMonitoringFeature):
    """
    Get Monitoring Feature Info
    """
    raise Exception("This is a list_monitoring_features exception")


@basin3d_plugin_access(ErrorSourcePlugin, MonitoringFeature, 'get')
def get_monitoring_feature(self, query: QueryById):
    """
    Get a MonitoringFeature
    :param pk: primary key
    """
    raise Exception("This is a get_monitoring_feature exception")
