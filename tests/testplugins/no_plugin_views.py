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
class NoPluginViewsPlugin(DataSourcePluginPoint):
    title = 'No Plugin Views Source Plugin'

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
        id = 'NoPluginView'  # unique id for the datasource
        id_prefix = 'NPV'
        name = id  # Human Friendly Data Source Name


