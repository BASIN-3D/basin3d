import datetime as dt
import os
import pandas as pd
import pytest

from typing import Iterator

from pydantic import ValidationError

from basin3d.core.models import Base
from basin3d.core.schema.enum import ResultQualityEnum, TimeFrequencyEnum
from basin3d.core.types import SamplingMedium
from basin3d.synthesis import register, get_timeseries_data


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

    with pytest.raises(ValidationError):
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

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query2)


@pytest.mark.integration
@pytest.mark.parametrize("query, feature_type", [({"id": "USGS-13"}, "region"),
                                                 ({"id": "USGS-0102"}, "subregion"),
                                                 ({"id": "USGS-011000"}, "basin"),
                                                 ({"id": "USGS-01020004"}, "subbasin"),
                                                 ({"id": "USGS-09129600"}, "point"),
                                                 ({"id": "USGS-383103106594200", "feature_type": "POINT"}, "point")],
                         ids=["region", "subregion", "basin", "subbasin", "point", "point_long_id"])
def test_usgs_monitoring_feature(query, feature_type):
    """Test USGS search by region  """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()


@pytest.mark.integration
@pytest.mark.parametrize("query, expected_count", [({"datasource": "USGS"}, 2889),
                                                   ({"monitoring_features": ['USGS-02']}, 1),
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
                                                   ({"monitoring_features": ["USGS-09129600"], "feature_type": "point"}, 1),
                                                   ({"parent_features": ['USGS-02']}, 118),
                                                   ({"parent_features": ['USGS-02020004'], "feature_type": "point"}, 49),
                                                   ({"parent_features": ['USGS-0202'], "feature_type": "subbasin"}, 8),
                                                   ({"parent_features": ['USGS-020200'], "feature_type": "point"}, 0)],
                         ids=["all", "region_by_id", "region", "subregion",
                              "basin", "subbasin",
                              "watershed", "subwatershed",
                              "site", "plot",
                              "vertical_path",
                              "horizontal_path",
                              "point", "point_by_id", "all_by_region",
                              "points_by_subbasin",
                              "subbasin_by_subregion", "invalid_points"])
def test_usgs_monitoring_features(query, expected_count):
    """Test USGS search by region  """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    # TODO should there be some kind of exeption handling for invalid queries that don't return anything?
    count = 0
    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    assert count == expected_count


@pytest.mark.integration
def test_usgs_get_data():
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    usgs_data = get_timeseries_data(synthesizer=synthesizer, monitoring_features=["USGS-09110000"],
                                    observed_property_variables=['RDC', 'WT'], start_date='2019-10-25',
                                    end_date='2019-10-28')
    usgs_df = usgs_data.data
    usgs_metadata_df = usgs_data.metadata

    # check the dataframe
    assert isinstance(usgs_df, pd.DataFrame) is True
    for column_name in list(usgs_df.columns):
        assert column_name in ['TIMESTAMP', 'USGS-09110000__RDC__MEAN', 'USGS-09110000__WT__MEAN',
                               'USGS-09110000__WT__MIN', 'USGS-09110000__WT__MAX']
    assert usgs_df.shape == (4, 5)
    alpha_data = usgs_df.get('USGS-09110000__RDC__MEAN')
    assert list(alpha_data.values) == [4.2475270499999995, 4.219210203, 4.134259662, 4.332477591]
    assert list(alpha_data.index) == [dt.datetime(2019, 10, num) for num in range(25, 29)]

    # make sure the temporary data directory is removed
    temp_dir = os.path.join(os.getcwd(), 'temp_data')
    assert os.path.isdir(temp_dir) is False

    # check the metadata store
    # Get synthesized variable field names and values
    var_metadata = usgs_metadata_df['USGS-09110000__RDC__MEAN']
    assert var_metadata['data_start'] == dt.datetime(2019, 10, 25)
    assert var_metadata['data_end'] == dt.datetime(2019, 10, 28)
    assert var_metadata['records'] == 4
    assert var_metadata['units'] == 'm^3/s'
    assert var_metadata['basin_3d_variable'] == 'RDC'
    assert var_metadata['basin_3d_variable_full_name'] == 'River Discharge'
    assert var_metadata['statistic'] == 'MEAN'
    assert var_metadata['temporal_aggregation'] == TimeFrequencyEnum.DAY
    assert var_metadata['quality'] == ResultQualityEnum.CHECKED
    assert var_metadata['sampling_medium'] == SamplingMedium.WATER
    assert var_metadata['sampling_feature_id'] == 'USGS-09110000'
    assert var_metadata['datasource'] == 'USGS'
    assert var_metadata['datasource_variable'] == '00060'

    assert usgs_metadata_df['USGS-09110000__WT__MIN']['statistic'] == 'MIN'
    assert usgs_metadata_df['USGS-09110000__WT__MAX']['statistic'] == 'MAX'

    # check filtering by single statistic
    usgs_data = get_timeseries_data(synthesizer=synthesizer, monitoring_features=["USGS-09110000"],
                                    observed_property_variables=['RDC', 'WT'], start_date='2019-10-25',
                                    end_date='2019-10-28', statistic=['MEAN'])
    usgs_df = usgs_data.data

    # check the dataframe
    assert isinstance(usgs_df, pd.DataFrame) is True
    for column_name in list(usgs_df.columns):
        assert column_name in ['TIMESTAMP', 'USGS-09110000__RDC__MEAN', 'USGS-09110000__WT__MEAN']
    assert usgs_df.shape == (4, 3)

    # check filtering by multiple statistic
    usgs_data = get_timeseries_data(synthesizer=synthesizer, monitoring_features=["USGS-09110000"],
                                    observed_property_variables=['RDC', 'WT'], start_date='2019-10-25',
                                    end_date='2019-10-28', statistic=['MIN', 'MAX'])
    usgs_df = usgs_data.data

    # check the dataframe
    assert isinstance(usgs_df, pd.DataFrame) is True
    for column_name in list(usgs_df.columns):
        assert column_name in ['TIMESTAMP', 'USGS-09110000__WT__MIN', 'USGS-09110000__WT__MAX']
    assert usgs_df.shape == (4, 3)
