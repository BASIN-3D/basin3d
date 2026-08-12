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
    >>> monitoring_feature_geopandas = b3dww.get_monitoring_features(synthesizer, datasource=['USGS'], feature_type='point', monitoring_feature=[(-90.6, 34.45, -90.5, 34.55)])
    >>> monitoring_feature_geopandas.shape
    (20, 7)
    >>> print(monitoring_feature_geopandas)
                              id                             name feature_type                                        description data_source  elevation                    geometry
    0          USGS-07047970  MISSISSIPPI RIVER AT HELENA, AR        POINT  site type: Stream; hydrologic_unit_code: 08020...        USGS     141.70    POINT (-90.58399 34.524)
    1          USGS-07287700     PHILLIPS BAYOU AT POWELL, MS        POINT  site type: Stream; hydrologic_unit_code: 08030...        USGS        NaN  POINT (-90.53022 34.48425)
    2   USGS-342827090324001                027A0016  COAHOMA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     176.00  POINT (-90.54455 34.47427)
    3   USGS-342849090323501                027A0035  COAHOMA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     180.00  POINT (-90.54306 34.48028)
    4   USGS-342900090323501                027A0019  COAHOMA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     182.00  POINT (-90.54316 34.48344)
    5   USGS-342915090315501                027A0006  COAHOMA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     182.00   POINT (-90.53205 34.4876)
    6   USGS-342946090344701                027A0023  COAHOMA        POINT  site type: Well; hydrologic_unit_code: 0802010...        USGS     185.00  POINT (-90.57983 34.49621)
    7   USGS-342957090344001                027A2003  COAHOMA        POINT  site type: Well; hydrologic_unit_code: 0802010...        USGS     185.00  POINT (-90.57778 34.49917)
    8   USGS-342958090344701                027A0030  COAHOMA        POINT  site type: Well; hydrologic_unit_code: 0802010...        USGS     180.00  POINT (-90.57972 34.49944)
    9   USGS-343007090322801                027A0001  COAHOMA        POINT    site type: Well; hydrologic_unit_code: 08020100        USGS     176.00  POINT (-90.54121 34.50205)
    10  USGS-343047090301501                 143J0053  TUNICA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     181.00  POINT (-90.50427 34.51316)
    11  USGS-343058090321201                 143J0114  TUNICA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     179.00  POINT (-90.53594 34.51177)
    12  USGS-343104090352501                     02S05E16BCB2        POINT  site type: Well; hydrologic_unit_code: 0802030...        USGS     187.00  POINT (-90.59032 34.51783)
    13  USGS-343106090320501                 143J0020  TUNICA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     179.00  POINT (-90.53483 34.51844)
    14  USGS-343110090352501                     02S05E16BCB1        POINT  site type: Well; hydrologic_unit_code: 0802030...        USGS     188.00  POINT (-90.59046 34.51898)
    15  USGS-343116090353101                     02S05E16BBC1        POINT  site type: Well; hydrologic_unit_code: 0802030...        USGS     185.00  POINT (-90.59205 34.52121)
    16  USGS-343141090314801                 143J0019  TUNICA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     181.00  POINT (-90.51871 34.53483)
    17  USGS-343152090310301                  143J0502 TUNICA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     179.96    POINT (-90.5175 34.5311)
    18  USGS-343224090351101                     02S05E04CDC1        POINT  site type: Well; hydrologic_unit_code: 0802030...        USGS     183.00   POINT (-90.58649 34.5401)
    19  USGS-343239090301801                 143J0033  TUNICA        POINT  site type: Well; hydrologic_unit_code: 0803020...        USGS     177.00   POINT (-90.5051 34.54427)
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