import pytest

from basin3d.core.models import AbsoluteCoordinate, AltitudeCoordinate, Coordinate, DepthCoordinate, \
    GeographicCoordinate, MeasurementTimeseriesTVPObservation, MonitoringFeature, Observation, \
    ObservedProperty, RelatedSamplingFeature, RepresentativeCoordinate, TimeValuePair, ResultListTVP, \
    VerticalCoordinate, MappedAttribute, AttributeMapping
from basin3d.core.schema.enum import FeatureTypeEnum, ResultQualityEnum, StatisticEnum, SamplingMediumEnum
from basin3d.core.types import SpatialSamplingShapes


@pytest.fixture
def observed_property():
    """
    Load some fake data to use in the tests
    """
    return ObservedProperty(basin3d_vocab='FH2O', full_name='Groundwater Flux',
                            categories=['Hydrology', 'Subsurface'], units='m3/m/s')


@pytest.fixture
def mapped_attribute_op_ag(datasource):
    return MappedAttribute(
        attr_type='OBSERVED_PROPERTY',
        attr_mapping=AttributeMapping(
            attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
            basin3d_vocab='Ag:WATER',
            basin3d_desc=[
                ObservedProperty(basin3d_vocab='Ag', full_name='Silver (Ag)', categories=['Biogeochemistry', 'Trace elements'], units='mg/L'),
                SamplingMediumEnum.WATER],
            datasource_vocab='Ag',
            datasource_desc='sliver concentration in water',
            datasource=datasource))


@pytest.fixture
def mapped_attribute_op_act(datasource):
    return MappedAttribute(
        attr_type='OBSERVED_PROPERTY',
        attr_mapping=AttributeMapping(
            attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
            basin3d_vocab='ACT:WATER',
            basin3d_desc=[ObservedProperty(basin3d_vocab='ACT', full_name='Acetate (CH3COO)', categories=['Biogeochemistry', 'Anions'], units='mM'),
                          SamplingMediumEnum.WATER],
            datasource_vocab='Acetate',
            datasource_desc='acetate',
            datasource=datasource))


@pytest.fixture
def mapped_attribute_sampling_medium_act(datasource):
    return MappedAttribute(
        attr_type='SAMPLING_MEDIUM',
        attr_mapping=AttributeMapping(
            attr_type='OBSERVED_PROPERTY:SAMPLING_MEDIUM',
            basin3d_vocab='ACT:WATER',
            basin3d_desc=[ObservedProperty(basin3d_vocab='ACT', full_name='Acetate (CH3COO)', categories=['Biogeochemistry', 'Anions'], units='mM'),
                          SamplingMediumEnum.WATER],
            datasource_vocab='Acetate',
            datasource_desc='acetate',
            datasource=datasource))


@pytest.fixture
def mapped_attribute_result_quality_validated(datasource):
    return MappedAttribute(attr_type='RESULT_QUALITY',
                           attr_mapping=AttributeMapping(
                               attr_type='RESULT_QUALITY',
                               basin3d_vocab='VALIDATED',
                               basin3d_desc=[ResultQualityEnum.VALIDATED],
                               datasource_vocab='VALIDATED',
                               datasource_desc='',
                               datasource=datasource))


@pytest.fixture
def mapped_attribute_agg_dur_not_supported(datasource):
    return MappedAttribute(attr_type='AGGREGATION_DURATION',
                           attr_mapping=AttributeMapping(
                               attr_type='AGGREGATION_DURATION',
                               basin3d_vocab='NOT_SUPPORTED',
                               basin3d_desc=[],
                               datasource_vocab='daily',
                               datasource_desc='No mapping was found for datasource vocab: "daily" in datasource: "Alpha".',
                               datasource=datasource))


def test_data_source_model(datasource):
    """Test DataSource model"""

    assert datasource.id == 'Alpha'
    assert datasource.name == 'Alpha'
    assert datasource.id_prefix == 'A'
    assert datasource.location == 'https://asource.foo/'


def test_observed_property_variable_create(observed_property):
    """ create the object and test attributes """

    assert observed_property.basin3d_vocab == 'FH2O'
    assert observed_property.full_name == 'Groundwater Flux'
    assert observed_property.categories == ['Hydrology', 'Subsurface']
    assert observed_property.units == 'm3/m/s'


def test_attribute_mapping(mapped_attribute_op_act, datasource):
    attr_mapping = mapped_attribute_op_act.attr_mapping
    assert attr_mapping.attr_type == 'OBSERVED_PROPERTY:SAMPLING_MEDIUM'
    assert attr_mapping.basin3d_vocab == 'ACT:WATER'
    assert attr_mapping.basin3d_desc[0] == ObservedProperty(basin3d_vocab='ACT', full_name='Acetate (CH3COO)', categories=['Biogeochemistry', 'Anions'], units='mM')
    assert attr_mapping.basin3d_desc[1] == SamplingMediumEnum.WATER
    assert attr_mapping.datasource_vocab == 'Acetate'
    assert attr_mapping.datasource_desc == 'acetate'
    assert attr_mapping.datasource == datasource


def test_mapped_attribute(mapped_attribute_op_act):
    assert mapped_attribute_op_act.attr_type == 'OBSERVED_PROPERTY'
    assert isinstance(mapped_attribute_op_act.attr_mapping, AttributeMapping) is True


def test_mapped_attribute_not_supported(mapped_attribute_agg_dur_not_supported, datasource):
    assert mapped_attribute_agg_dur_not_supported.attr_type == 'AGGREGATION_DURATION'
    attr_mapping = mapped_attribute_agg_dur_not_supported.attr_mapping
    assert attr_mapping.attr_type == 'AGGREGATION_DURATION'
    assert attr_mapping.basin3d_vocab == 'NOT_SUPPORTED'
    assert attr_mapping.basin3d_desc == []
    assert attr_mapping.datasource_vocab == 'daily'
    assert attr_mapping.datasource_desc == 'No mapping was found for datasource vocab: "daily" in datasource: "Alpha".'
    assert attr_mapping.datasource == datasource


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


def test_monitoring_feature_create(plugin_access_alpha, mapped_attribute_op_ag, mapped_attribute_op_act):
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
        observed_properties=['Ag', 'Acetate'],
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
    assert a_point.observed_properties[0] == mapped_attribute_op_ag
    assert a_point.observed_properties[1] == mapped_attribute_op_act
    assert a_point.related_sampling_feature_complex[0].related_sampling_feature == 'A-Region1'
    assert a_point.related_sampling_feature_complex[0].role == 'PARENT'


def test_observation_create(plugin_access_alpha, mapped_attribute_result_quality_validated):
    """
    Test instance of observation model class
    NOTE: In practice, the Observation should not be used stand alone
    """
    obs01 = Observation(
        plugin_access=plugin_access_alpha,
        id='timeseries01',
        utc_offset='9',
        phenomenon_time='20180201',
        result_quality=['VALIDATED'],
        feature_of_interest='Point011')

    assert obs01.datasource.id == 'Alpha'
    assert obs01.id == 'A-timeseries01'
    assert obs01.utc_offset == '9'
    assert obs01.phenomenon_time == '20180201'
    assert obs01.observed_property is None
    assert obs01.result_quality == [mapped_attribute_result_quality_validated]
    assert obs01.feature_of_interest == 'Point011'


def test_measurement_timeseries_tvp_observation_create(
        plugin_access_alpha, datasource,
        mapped_attribute_op_act, mapped_attribute_sampling_medium_act, mapped_attribute_result_quality_validated, mapped_attribute_agg_dur_not_supported):
    """Test instance of Measurement Timeseries TVP Observation"""

    obs01 = MeasurementTimeseriesTVPObservation(
        plugin_access=plugin_access_alpha,
        id='timeseries01',
        utc_offset='9',
        phenomenon_time='20180201',
        result_quality=['VALIDATED'],
        feature_of_interest='Point011',
        feature_of_interest_type=FeatureTypeEnum.POINT,
        aggregation_duration='daily',
        time_reference_position='start',
        observed_property='Acetate',
        statistic='mean',
        result=ResultListTVP(plugin_access=plugin_access_alpha,
                             value=[TimeValuePair('201802030100', '5.32')],
                             quality=['VALIDATED']),
        unit_of_measurement='m'
    )

    assert obs01.id == 'A-timeseries01'
    assert obs01.utc_offset == '9'
    assert obs01.phenomenon_time == '20180201'
    assert obs01.observed_property == mapped_attribute_op_act
    assert obs01.sampling_medium == mapped_attribute_sampling_medium_act
    assert obs01.result_quality == [mapped_attribute_result_quality_validated]
    assert obs01.feature_of_interest == 'Point011'
    assert obs01.feature_of_interest_type == FeatureTypeEnum.POINT
    assert obs01.aggregation_duration == mapped_attribute_agg_dur_not_supported
    assert obs01.time_reference_position == 'start'
    assert obs01.statistic == MappedAttribute(attr_type='STATISTIC',
                                              attr_mapping=AttributeMapping(
                                                  attr_type='STATISTIC',
                                                  basin3d_vocab='MEAN',
                                                  basin3d_desc=[StatisticEnum.MEAN],
                                                  datasource_vocab='mean',
                                                  datasource_desc='',
                                                  datasource=datasource))
    assert obs01.unit_of_measurement == 'm'
    assert obs01.datasource.id == 'Alpha'
    assert obs01.result.value[0] == TimeValuePair('201802030100', '5.32')
    assert obs01.result.quality[0] == mapped_attribute_result_quality_validated

