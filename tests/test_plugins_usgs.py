import json
import datetime as dt
import os
from unittest.mock import Mock, MagicMock
import pandas as pd
import pytest

from typing import Iterator

from pydantic import ValidationError

from basin3d.core.models import Base
from basin3d.core.types import FeatureTypes, TimeFrequency, ResultQuality, SamplingMedium
from basin3d.synthesis import register, SynthesisException, get_timeseries_data
from tests.utilities import get_text, get_json
import basin3d.plugins.usgs
from basin3d.plugins.usgs import USGSMonitoringFeatureAccess


def get_url(data):
    """
    Creates a get_url call for mocking with the specified return data
    :param data:
    :return:
    """

    return type('Dummy', (object,), {
        "json": lambda: data,
        "status_code": 200,
        "url": "/testurl"})


def get_url_text(text):
    """
    Creates a get_url call for mocking with the specified return data
    :param data:
    :return:
    """

    return type('Dummy', (object,), {
        "text": text,
        "status_code": 200,
        "url": "/testurl"})


def test_measurement_timeseries_tvp_observations_usgs(monkeypatch):
    """ Test USGS Timeseries data query"""

    import basin3d
    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("usgs_mtvp_sites.rdb")),
                                               get_url(get_json("usgs_nwis_dv_p00060_l09110990_l09111250.json"))]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
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
def test_usgs_monitoring_feature(query, feature_type, monkeypatch):
    """Test USGS search by region  """

    def mock_get_huc_codes(*args):
        return get_text("new_huc_rdb.txt")

    monkeypatch.setattr(USGSMonitoringFeatureAccess, 'get_hydrological_unit_codes', mock_get_huc_codes)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_feature = synthesizer.monitoring_features(**query)

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert FeatureTypes.TYPES[monitoring_feature.feature_type] == feature_type.upper()
    # add more asserts


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
def test_usgs_monitoring_features(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("new_huc_rdb.txt"))]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_featurues = synthesizer.monitoring_features(**query)

    # TODO should there be some kind of exeption handling for invalid queries that don't return anything?
    count = 0
    print(query.values(), "count:", count, "expected:", expected_count)

    for mf in monitoring_featurues:
        count += 1
        print(
            f"{mf.id} ({FeatureTypes.TYPES[mf.feature_type]}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert FeatureTypes.TYPES[mf.feature_type] == query['feature_type'].upper()

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count


@pytest.mark.parametrize("query, expected_count", [
    ({"monitoring_features": ['USGS-02']}, 1)],
                         ids=["region_by_id"])
def test_usgs_monitoring_features1(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("new_huc_rdb.txt"))]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_featurues = synthesizer.monitoring_features(**query)

    # TODO should there be some kind of exeption handling for invalid queries that don't return anything?
    count = 0
    print(query.values(), "count:", count, "expected:", expected_count)

    for mf in monitoring_featurues:
        count += 1
        print(
            f"{mf.id} ({FeatureTypes.TYPES[mf.feature_type]}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert FeatureTypes.TYPES[mf.feature_type] == query['feature_type'].upper()

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count


@pytest.mark.parametrize("query, expected_count", [
    ({"parent_features": ['USGS-02020004'], "feature_type": "point"}, 49)],
                         ids=["points_by_subbasin"])
def test_usgs_monitoring_features2(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([
        get_url(get_json("usgs_monitoring_features_query_02020004.json")),
        get_url_text(get_text("usgs_monitoring_features_query_point_02020004.rdb"))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_featurues = synthesizer.monitoring_features(**query)

    # TODO should there be some kind of exeption handling for invalid queries that don't return anything?
    count = 0
    print(query.values(), "count:", count, "expected:", expected_count)

    for mf in monitoring_featurues:
        count += 1
        print(
            f"{mf.id} ({FeatureTypes.TYPES[mf.feature_type]}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert FeatureTypes.TYPES[mf.feature_type] == query['feature_type'].upper()

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count


def test_usgs_get_data(monkeypatch):
    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("usgs_data_09110000.rdb")),
                                               get_url(get_json("usgs_get_data_09110000.json"))]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    usgs_data = get_timeseries_data(synthesizer=synthesizer, monitoring_features=["USGS-09110000"],
                                    observed_property_variables=['RDC', 'WT'], start_date='2019-10-25',
                                    end_date='2019-10-28')
    usgs_df = usgs_data.data
    usgs_metadata_df = usgs_data.metadata


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
    # Get synthesized variable field names and values
    var_metadata = usgs_metadata_df['USGS-09110000__RDC']
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
