import pytest

from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, DepthCoordinate, \
    GeographicCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature, Observation, \
    ObservedPropertyVariable, RelatedSamplingFeature, RepresentativeCoordinate, TimeValuePair, ResultListTVP, \
    VerticalCoordinate, DataSource
from basin3d.core.schema.enum import FeatureTypeEnum, ResultQualityEnum, StatisticEnum
from basin3d.core.types import SpatialSamplingShapes


@pytest.fixture
def observed_property_var():
    """
    Load some fake data to use in the tests
    """
    return ObservedPropertyVariable(basin3d_id='FH2O', full_name='Groundwater Flux',
                                    categories=['Hydrology', 'Subsurface'], units='m3/m/s')


# @pytest.fixture
# def observed_property(datasource, observed_property_var):
#     return ObservedProperty(datasource_variable='water_flux', observed_property_variable=observed_property_var,
#                             sampling_medium=SamplingMedium.WATER, datasource=datasource,
#                             datasource_description='a test variable')


def test_data_source_model(datasource):
    """Test DataSource model"""

    assert datasource.id == 'Alpha'
    assert datasource.name == 'Alpha'
    assert datasource.id_prefix == 'A'
    assert datasource.location == 'https://asource.foo/'


def test_observed_property_create(observed_property, observed_property_var, datasource):
    """ Was the object created correctly? """

    assert observed_property.sampling_medium == 'WATER'
    assert observed_property.datasource_variable == 'water_flux'
    assert observed_property.observed_property_variable == observed_property_var
    assert observed_property.datasource == datasource
    assert observed_property.datasource_description == 'a test variable'


def test_observed_property_variable_create(observed_property_var):
    """ create the object and test attributes """

    assert observed_property_var.basin3d_id == 'FH2O'
    assert observed_property_var.full_name == 'Groundwater Flux'
    assert observed_property_var.categories == ['Hydrology', 'Subsurface']
    assert observed_property_var.units == 'm3/m/s'


def test_representative_coordinate():
    """Test a Representative Coordinate"""

    r_coord = RepresentativeCoordinate(
        representative_point=AbsoluteCoordinate(
            horizontal_position=GeographicCoordinate(
                units=GeographicCoordinate.UNITS_DEC_DEGREES,
                latitude=70.4657, longitude=-20.4567),
            vertical_extent=AltitudeCoordinate(
                datum=AltitudeCoordinate.DATUM_NAVD88,
                value=1500, distance_units=VerticalCoordinate.DISTANCE_UNITS_FEET)),
        representative_point_type=RepresentativeCoordinate.REPRESENTATIVE_POINT_TYPE_CENTER_LOCAL_SURFACE)

    assert r_coord.representative_point.vertical_extent[0].datum == AltitudeCoordinate.DATUM_NAVD88
    assert r_coord.representative_point.vertical_extent[0].value == 1500
    assert r_coord.representative_point.vertical_extent[0].distance_units == VerticalCoordinate.DISTANCE_UNITS_FEET
    assert r_coord.representative_point.horizontal_position[0].longitude == -20.4567
    assert r_coord.representative_point.horizontal_position[0].x == -20.4567
    assert r_coord.representative_point.horizontal_position[0].y == 70.4657
    assert r_coord.representative_point.horizontal_position[0].latitude == 70.4657
    assert r_coord.representative_point.horizontal_position[0].units == GeographicCoordinate.UNITS_DEC_DEGREES
    assert r_coord.representative_point_type == RepresentativeCoordinate.REPRESENTATIVE_POINT_TYPE_CENTER_LOCAL_SURFACE


def test_related_sampling_feature(plugin_access_alpha):
    """Test a Related Sampling feature"""
    related_sampling_feature = RelatedSamplingFeature(plugin_access=plugin_access_alpha,
                                                      related_sampling_feature='Region1',
                                                      related_sampling_feature_type=FeatureTypeEnum.REGION,
                                                      role=RelatedSamplingFeature.ROLE_PARENT)

    assert related_sampling_feature.datasource == plugin_access_alpha.datasource
    assert related_sampling_feature.related_sampling_feature == 'A-Region1'
    assert related_sampling_feature.related_sampling_feature_type == FeatureTypeEnum.REGION
    assert related_sampling_feature.role == RelatedSamplingFeature.ROLE_PARENT


def test_absolute_coordinate():
    a_coord = AltitudeCoordinate(
        datum=AltitudeCoordinate.DATUM_NAVD88,
        value=1500,
        distance_units=VerticalCoordinate.DISTANCE_UNITS_FEET)

    assert a_coord.datum == AltitudeCoordinate.DATUM_NAVD88
    assert a_coord.value == 1500
    assert a_coord.distance_units == VerticalCoordinate.DISTANCE_UNITS_FEET


def test_monitoring_feature_create(plugin_access_alpha):
    """Test instance of monitoring feature"""

    a_region = MonitoringFeature(
        plugin_access=plugin_access_alpha,
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
                    value=1500, distance_units=VerticalCoordinate.DISTANCE_UNITS_FEET)),
            representative_point_type=RepresentativeCoordinate.REPRESENTATIVE_POINT_TYPE_CENTER_LOCAL_SURFACE)
        )
    )

    assert a_region.datasource.id == 'Alpha'
    assert a_region.id == 'A-Region1'
    assert a_region.name == 'AwesomeRegion'
    assert a_region.feature_type == FeatureTypeEnum.REGION
    assert a_region.description == 'This region is really awesome.'
    assert a_region.shape == SpatialSamplingShapes.SHAPE_SURFACE
    assert a_region.coordinates.representative.representative_point.horizontal_position[0].units == \
        GeographicCoordinate.UNITS_DEC_DEGREES
    assert a_region.coordinates.representative.representative_point.horizontal_position[0].latitude == 70.4657
    assert a_region.coordinates.representative.representative_point.horizontal_position[0].longitude == -20.4567
    assert a_region.coordinates.representative.representative_point.vertical_extent[0].datum == \
        AltitudeCoordinate.DATUM_NAVD88
    assert a_region.coordinates.representative.representative_point.vertical_extent[0].value == 1500
    assert a_region.coordinates.representative.representative_point.vertical_extent[0].distance_units == \
        VerticalCoordinate.DISTANCE_UNITS_FEET
    assert a_region.coordinates.representative.representative_point_type == \
        RepresentativeCoordinate.REPRESENTATIVE_POINT_TYPE_CENTER_LOCAL_SURFACE

    a_point = MonitoringFeature(
        plugin_access=plugin_access_alpha,
        id='1',
        name='Point Location 1',
        description='The first point.',
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
                    value=-0.5, distance_units=VerticalCoordinate.DISTANCE_UNITS_METERS)
            )
        ),
        observed_property_variables=['Ag', 'Acetate'],
        related_sampling_feature_complex=[
            RelatedSamplingFeature(plugin_access=plugin_access_alpha,
                                   related_sampling_feature='Region1',
                                   related_sampling_feature_type=FeatureTypeEnum.REGION,
                                   role=RelatedSamplingFeature.ROLE_PARENT)]
    )

    assert a_point.datasource.id == 'Alpha'
    assert a_point.id == 'A-1'
    assert a_point.name == 'Point Location 1'
    assert a_point.feature_type == FeatureTypeEnum.POINT
    assert a_point.description == 'The first point.'
    assert a_point.shape == SpatialSamplingShapes.SHAPE_POINT
    assert a_point.coordinates.absolute.horizontal_position[0].units == \
        GeographicCoordinate.UNITS_DEC_DEGREES
    assert a_point.coordinates.absolute.horizontal_position[0].latitude == 70.4657
    assert a_point.coordinates.absolute.horizontal_position[0].longitude == -20.4567
    assert a_point.coordinates.absolute.vertical_extent[0].datum == \
        AltitudeCoordinate.DATUM_NAVD88
    assert a_point.coordinates.absolute.vertical_extent[0].value == 1500
    assert a_point.coordinates.absolute.vertical_extent[0].distance_units == \
        VerticalCoordinate.DISTANCE_UNITS_FEET
    assert a_point.coordinates.representative.vertical_position.value == -0.5
    assert a_point.coordinates.representative.vertical_position.distance_units == \
        VerticalCoordinate.DISTANCE_UNITS_METERS
    assert a_point.coordinates.representative.vertical_position.datum == \
        DepthCoordinate.DATUM_LOCAL_SURFACE
    assert a_point.observed_property_variables == ['ACT', 'Ag']
    assert a_point.related_sampling_feature_complex[0].related_sampling_feature == 'A-Region1'
    assert a_point.related_sampling_feature_complex[0].role == 'PARENT'


def test_observation_create(plugin_access_alpha):
    """
    Test instance of observation model class
    NOTE: In practice, the Observation should not be used stand alone
    """
    obs01 = Observation(
        plugin_access=plugin_access_alpha,
        id='timeseries01',
        utc_offset='9',
        phenomenon_time='20180201',
        result_quality=ResultQualityEnum.VALIDATED,
        feature_of_interest='Point011')

    assert obs01.datasource.id == 'Alpha'
    assert obs01.id == 'A-timeseries01'
    assert obs01.utc_offset == '9'
    assert obs01.phenomenon_time == '20180201'
    assert obs01.observed_property is None
    assert obs01.result_quality == ResultQualityEnum.VALIDATED
    assert obs01.feature_of_interest == 'Point011'


def test_measurement_timeseries_tvp_observation_create(plugin_access_alpha):
    """Test instance of Measurement Timeseries TVP Observation"""

    obs01 = MeasurementTimeseriesTVPObservation(
        plugin_access=plugin_access_alpha,
        id='timeseries01',
        utc_offset='9',
        phenomenon_time='20180201',
        result_quality=[ResultQualityEnum.VALIDATED],
        feature_of_interest='Point011',
        feature_of_interest_type=FeatureTypeEnum.POINT,
        aggregation_duration='daily',
        time_reference_position='start',
        observed_property_variable='Acetate',
        statistic='mean',
        result=ResultListTVP(value=[TimeValuePair('201802030100', '5.32')],
                             quality=[ResultQualityEnum.VALIDATED]),
        unit_of_measurement='m'
    )

    assert obs01.id == 'A-timeseries01'
    assert obs01.utc_offset == '9'
    assert obs01.phenomenon_time == '20180201'
    # assert obs01.observed_property == ObservedProperty(
    #     datasource_variable='Acetate',
    #     observed_property_variable=ObservedPropertyVariable(
    #         basin3d_id='ACT', full_name='Acetate (CH3COO)',
    #         categories=['Biogeochemistry', 'Anions'], units='mM'),
    #     sampling_medium=SamplingMedium.WATER,
    #     datasource=DataSource(
    #         id='Alpha', name='Alpha', id_prefix='A',
    #         location='https://asource.foo/', credentials={}),
    #     datasource_description='')
    assert obs01.observed_property == 'ACT'
    assert obs01.result_quality == [ResultQualityEnum.VALIDATED]
    assert obs01.feature_of_interest == 'Point011'
    assert obs01.feature_of_interest_type == FeatureTypeEnum.POINT
    assert obs01.aggregation_duration == 'daily'
    assert obs01.time_reference_position == 'start'
    assert obs01.statistic == StatisticEnum.MEAN
    assert obs01.unit_of_measurement == 'm'
    assert obs01.datasource.id == 'Alpha'
    assert obs01.result.value[0] == TimeValuePair('201802030100', '5.32')
    assert obs01.result.quality[0] == ResultQualityEnum.VALIDATED
