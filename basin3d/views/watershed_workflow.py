"""

.. currentmodule:: basin3d.views.watershed_workflow

:synopsis: output views for integration with Watershed Workflow
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

Acknowledgements
----------------
The Watershed Workflow views are constructed in collaboration with Ethan Coon.

"""

import geopandas
import shapely
from typing import Optional

from basin3d.core import monitor
from basin3d.synthesis import DataSynthesizer

logger = monitor.get_logger(__name__)


def get_monitoring_features(synthesizer: DataSynthesizer, crs: str = 'EPSG:4326', **kwargs) -> geopandas.GeoDataFrame:
    """
    Acquire monitoring features that match the given query in a geopandas.GeoDataFrame output format.

    :param synthesizer: DataSynthesizer configured with the desired data source plugins
    :param crs: optional coordinate reference system, specific to the Watershed Workflow codebase. If provided, it will override the default EPSG:4326.
    :param kwargs: arguments for the monitoring feature query that is passed to the :class:`~basin3d.synthesis.DataSynthesizer.monitoring_features` method.
    :return: geopandas.GeoDataFrame with results of monitoring feature query.

    Query arguments for the monitoring features include.
      * feature_type: str (optional but recommended; and required by some data sources)
      * datasource: list (optional but recommended)
      * One of the following is required:
        * monitoring_feature = list of one or more feature identifiers (str) and or bounding boxes (tuple)
        * parent_feature = list of feature identifiers (str)

    >>> from basin3d.plugins import usgs
    >>> from basin3d import synthesis
    >>> from basin3d.views import watershed_workflow as b3dww
    >>> synthesizer = synthesis.register()
    >>> monitoring_feature_geopandas = b3dww.get_monitoring_features(synthesizer, datasource=['USGS'], feature_type='point', monitoring_feature=[(-90.6, 34.4, -90.5, 34.6)])
    >>> monitoring_feature_geopandas.shape
    (2, 7)
    >>> print(monitoring_feature_geopandas)
                  id  ...                    geometry
    0  USGS-07047970  ...    POINT (-90.58399 34.524)
    1  USGS-07287700  ...  POINT (-90.53022 34.48425)
    <BLANKLINE>
    [2 rows x 7 columns]

    """

    class ColumnNames:
        id = 'id'
        name = 'name'
        feature_type = 'feature_type'
        description = 'description'
        data_source = 'data_source'
        elevation = 'elevation'

    def extract_coordinates(mf) -> Optional[tuple]:
        try:
            if not mf.coordinates:
                return None

            abs_coord = mf.coordinates.absolute
            if not abs_coord:
                return None

            # Extract horizontal position (longitude, latitude)
            # ToDo: add check to compare datum to crs value
            if abs_coord.horizontal_position:
                # Get first position; note if the feature is not a point,
                #    this could be a point location describing the feature
                h_pos = abs_coord.horizontal_position[0]
                longitude = h_pos.longitude
                latitude = h_pos.latitude
            else:
                return None

            # Extract elevation (optional)
            elevation = None
            if abs_coord.vertical_extent:
                # Get first extent; note if the feature is not a point,
                #    this could be a point location describing the feature
                v_ext = abs_coord.vertical_extent[0]
                elevation = getattr(v_ext, 'value', None)

            return longitude, latitude, elevation

        except Exception as e:
            logger.warning(f"Failed to extract coordinates: {e}")
            return None

    empty_result_view = geopandas.GeoDataFrame(
        columns=[ColumnNames.id, ColumnNames.name, ColumnNames.feature_type,
                 ColumnNames.description, ColumnNames.data_source, ColumnNames.elevation],
        geometry=[],
        crs=crs)

    monitoring_features: list = []

    monitoring_features_generator = synthesizer.monitoring_features(**kwargs)

    monitoring_features.extend(monitoring_features_generator)

    # log any messages in executing the generator above
    for msg in monitoring_features_generator.synthesis_response.messages:  # type: ignore
        logger.info(msg)

    if not monitoring_features:
        logger.info('No monitoring features found for query parameters: {}'.format(kwargs))
        return empty_result_view

    results_geom = []
    results_records = []

    for monitoring_feature in monitoring_features:
        mf_id = monitoring_feature.id

        try:
            loc_coords = extract_coordinates(monitoring_feature)
            if not loc_coords:
                logger.info(f'No coordinates available for {mf_id}')
                continue
            long, lat, elev = loc_coords
            geom = shapely.geometry.Point(long, lat)

            loc_record = {
                ColumnNames.id: mf_id,
                ColumnNames.name: getattr(monitoring_feature, 'name', ''),
                ColumnNames.feature_type: getattr(monitoring_feature, 'feature_type', ''),
                ColumnNames.description: getattr(monitoring_feature, 'description', ''),
                ColumnNames.data_source: getattr(monitoring_feature, 'datasource', ''),
                ColumnNames.elevation: elev
            }

            results_geom.append(geom)
            results_records.append(loc_record)

        except Exception as e:
            logger.warning(f'Could not extract location info for {mf_id}: {e}')
            continue

    if all([results_geom, results_records, len(results_records) == len(results_geom)]):
        return geopandas.GeoDataFrame(results_records, geometry=results_geom, crs=crs)

    logger.warning('No parsable monitoring features were found.')
    return empty_result_view