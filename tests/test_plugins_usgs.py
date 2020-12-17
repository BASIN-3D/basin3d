import datetime as dt
import os
import pandas as pd
import pytest

from typing import Iterator

from basin3d.core.models import Base
from basin3d.core.types import FeatureTypes, TimeFrequency, ResultQuality, SamplingMedium
from basin3d.synthesis import register, SynthesisException, get_timeseries_data


@pytest.mark.integration
def test_measurement_timeseries_tvp_observations_usgs():
    """ Test USGS Timeseries data query"""

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query0 = {
        "monitoring_features": ["USGS-09110990", "USGS-09111250"],
        "observed_property_variables": [],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "CHECKED"
    }
    with pytest.raises(SynthesisException):
        synthesizer.measurement_timeseries_tvp_observations(**query0)

    query1 = {
        "monitoring_features": ["USGS-09110990", "USGS-09111250"],
        "observed_property_variables": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "CHECKED"
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query1)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            print(timeseries.to_json())
            count += 1

        assert count == 2
    else:
        pytest.fail("Returned object must be iterator")

    query2 = {
        "observed_property_variables": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "CHECKED"
    }
    with pytest.raises(TypeError):
        synthesizer.measurement_timeseries_tvp_observations(**query2)


@pytest.mark.integration
@pytest.mark.parametrize("query, feature_type", [({"id": "USGS-13"}, "region"),
                                                 ({"id": "USGS-0102"}, "subregion"),
                                                 ({"id": "USGS-011000"}, "basin"),
                                                 ({"id": "USGS-01020004"}, "subbasin"),
                                                 ({"id": "USGS-09129600", "feature_type": "POINT"}, "point"),
                                                 ({"id": "USGS-383103106594200", "feature_type": "POINT"}, "point")],
                         ids=["region", "subregion", "basin", "subbasin", "point", "point_long_id"])
def test_usgs_monitoring_feature(query, feature_type):
    """Test USGS search by region  """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_feature = synthesizer.monitoring_features(**query)

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert FeatureTypes.TYPES[monitoring_feature.feature_type] == feature_type.upper()


@pytest.mark.integration
@pytest.mark.parametrize("query, expected_count", [({}, 2889),
                                                   ({"feature_type": "region"}, 21),
                                                   ({"feature_type": "subregion"}, 222),
                                                   ({"feature_type": "basin"}, 379),
                                                   ({"feature_type": "subbasin"}, 2267),
                                                   ({"feature_type": "watershed"}, 0),
                                                   ({"feature_type": "subwatershed"}, 0),
                                                   ({"feature_type": "site"}, 0),
                                                   ({"feature_type": "plot"}, 0),
                                                   ({"feature_type": "vertical path"}, 0),
                                                   ({"feature_type": "horizontal path"}, 0),
                                                   ({"feature_type": "point"}, 0),
                                                   ({"parent_features": ['USGS-02']}, 118),
                                                   (
                                                           {"parent_features": ['USGS-02020004'],
                                                            "feature_type": "point"}, 48),
                                                   ({"parent_features": ['USGS-0202'], "feature_type": "subbasin"}, 8),
                                                   ({"parent_features": ['USGS-020200'], "feature_type": "point"}, 0)],
                         ids=["all", "region", "subregion",
                              "basin", "subbasin",
                              "watershed", "subwatershed",
                              "site", "plot",
                              "vertical_path",
                              "horizontal_path",
                              "point", "all_by_region",
                              "points_by_subbasin",
                              "subbasin_by_subregion", "invalid_points"])
def test_usgs_monitoring_features(query, expected_count):
    """Test USGS search by region  """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_featurues = synthesizer.monitoring_features(**query)

    # TODO should there be some kind of exeption handling for invalid queries that don't return anything?
    count = 0
    for mf in monitoring_featurues:
        count += 1
        print(
            f"{mf.id} ({FeatureTypes.TYPES[mf.feature_type]}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert FeatureTypes.TYPES[mf.feature_type] == query['feature_type'].upper()

    assert count == expected_count


@pytest.mark.integration
def test_usgs_get_data():
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    usgs_data = get_timeseries_data(
        synthesizer=synthesizer, monitoring_features=["USGS-09110000"],
        observed_property_variables=['RDC', 'WT'], start_date='2019-10-25', end_date='2019-10-28')
    usgs_df = usgs_data.data
    usgs_metadata = usgs_data.metadata_store

    # check the dataframe
    assert isinstance(usgs_df, pd.DataFrame) is True
    for column_name in list(usgs_df.columns):
        assert column_name in ['TIMESTAMP', 'USGS-09110000__RDC', 'USGS-09110000__WT']
    assert usgs_df.shape == (4, 3)
    alpha_data = usgs_df.get('USGS-09110000__RDC')
    assert list(alpha_data.values) == [4.2475270499999995, 4.219210203, 4.134259662, 4.332477591]
    assert list(alpha_data.index) == [dt.datetime(2019, 10, num) for num in range(25, 29)]
    # make sure the temporary data directory is removed
    temp_dir = os.path.join(os.getcwd(), 'temp_data')
    assert os.path.isdir(temp_dir) is False
    # check the metadata store
    var_metadata = usgs_metadata.get('USGS-09110000__RDC')
    assert var_metadata['data_start'] == dt.datetime(2019, 10, 25)
    assert var_metadata['data_end'] == dt.datetime(2019, 10, 28)
    assert var_metadata['records'] == 4
    assert var_metadata['units'] == 'm^3/s'
    assert var_metadata['basin_3d_variable'] == 'RDC'
    assert var_metadata['basin_3d_variable_full_name'] == 'River Discharge'
    assert var_metadata['statistic'] == 'MEAN'
    assert var_metadata['temporal_aggregation'] == TimeFrequency.DAY
    assert var_metadata['quality'] == ResultQuality.RESULT_QUALITY_CHECKED
    assert var_metadata['sampling_medium'] == SamplingMedium.WATER
    assert var_metadata['sampling_feature_id'] == 'USGS-09110000'
    assert var_metadata['datasource'] == 'USGS'
    assert var_metadata['datasource_variable'] == '00060'
