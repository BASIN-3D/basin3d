import json
import datetime as dt
import os
from unittest.mock import MagicMock

import basin3d
from basin3d.plugins.usgs import USGSMonitoringFeatureAccess
from tests.utilities import get_text, get_json

import pandas as pd
import pytest

from typing import Iterator

from pydantic import ValidationError

from basin3d.core.models import Base
from basin3d.core.schema.enum import ResultQualityEnum, TimeFrequencyEnum, StatisticEnum
from basin3d.core.types import SamplingMedium
from basin3d.synthesis import register, get_timeseries_data


def get_url(data, status=200):
    """
    Creates a get_url call for mocking with the specified return data
    :param data:
    :return:
    """
    return type('Dummy', (object,), {
        "json": lambda: data,
        "status_code": status,
        "url": "/testurl"})


def get_url_text(text, status=200):
    """
    Creates a get_url_text call for mocking with the specified return data
    :param text:
    :return:
    """

    return type('Dummy', (object,), {
        "text": text,
        "status_code": status,
        "url": "/testurl"})


@pytest.mark.parametrize('additional_query_params',
                         [({"monitoring_features": ["USGS-09110990", "USGS-09111250"], "observed_property_variables": []}),
                          ({"observed_property_variables": ["RDC"]})],
                         ids=['missing-variables', 'missing-monitoring_features'])
def test_measurement_timeseries_tvp_observations_usgs_errors(additional_query_params, monkeypatch):
    """ Test USGS Timeseries data query"""

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("usgs_mtvp_sites.rdb")),
                                               get_url(get_json("usgs_nwis_dv_p00060_l09110990_l09111250.json"))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query = {
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.DAY,
        "quality": [ResultQualityEnum.VALIDATED],
        **additional_query_params
    }
    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query)


@pytest.mark.parametrize('additional_filters, usgs_response, expected_results',
                         [
                          # all-good
                          ({"monitoring_features": ["USGS-09110990", "USGS-09111250"], "observed_property_variables": ["RDC"], "result_quality": [ResultQualityEnum.VALIDATED]},
                           "usgs_nwis_dv_p00060_l09110990_l09111250.json", {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED], "count": 2}),
                          # some-quality-filtered-data
                          ({"monitoring_features": ["USGS-09110990"], "observed_property_variables": ["WT"], "result_quality": [ResultQualityEnum.UNVALIDATED]},
                           "usgs_get_data_09110000_VALIDATED_UNVALIDATED_WT_only.json",
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.UNVALIDATED], "count": 1, "synthesis_msgs": ['09110000 - 00010: 2 timestamps did not match data quality query.']}),
                          # all-data-filtered
                          ({"monitoring_features": ["USGS-09110990"], "observed_property_variables": ["WT"], "result_quality": [ResultQualityEnum.REJECTED]},
                           "usgs_get_data_09110000_VALIDATED_UNVALIDATED_WT_only.json",
                           {"statistic": StatisticEnum.MEAN, "result_quality": [], "count": 1, "synthesis_msgs": ['09110000 - 00010: 4 timestamps did not match data quality query.', '09110000 had no valid data values for 00010 that match the query.']})
                         ],
                         ids=['all-good', 'some-quality-filtered-data', 'all-data-filtered'])
def test_measurement_timeseries_tvp_observations_usgs(additional_filters, usgs_response, expected_results, monkeypatch):
    """ Test USGS Timeseries data query"""

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("usgs_mtvp_sites.rdb")),
                                               get_url(get_json(usgs_response))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query = {
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.DAY,
        **additional_filters
    }

    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    # loop through generator and serialized the object, get actual object and compare
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            print(timeseries.to_json())
            data = json.loads(timeseries.to_json())
            count += 1
            assert data["statistic"] == expected_results.get("statistic")
            assert data["result_quality"] == expected_results.get("result_quality")
        assert count == expected_results.get("count")
        if expected_results.get('synthesis_msgs'):
            expected_msgs = expected_results.get('synthesis_msgs')
            msgs = measurement_timeseries_tvp_observations.synthesis_response.messages
            for idx, msg in enumerate(msgs):
                assert msg.msg == expected_msgs[idx]
    else:
        pytest.fail("Returned object must be iterator")


@pytest.mark.parametrize("query, feature_type", [({"id": "USGS-13"}, "region"),
                                                 ({"id": "USGS-0102"}, "subregion"),
                                                 ({"id": "USGS-011000"}, "basin"),
                                                 ({"id": "USGS-01020004"}, "subbasin")],
                         ids=["region", "subregion", "basin", "subbasin"])
def test_usgs_monitoring_feature(query, feature_type, monkeypatch):
    """Test USGS search by region  """

    def mock_get_huc_codes(*args, **kwargs):
        return get_text("new_huc_rdb.txt")

    monkeypatch.setattr(USGSMonitoringFeatureAccess, 'get_hydrological_unit_codes', mock_get_huc_codes)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()


@pytest.mark.parametrize("query, feature_type", [({"id": "USGS-09129600"}, "point")],
                         ids=["point"])
def test_usgs_monitoring_feature2(query, feature_type, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([
        get_url_text(get_text("new_huc_rdb.txt")),
        get_url(get_json("usgs_monitoring_features_query_point_09129600.json")),
        get_url_text(get_text("usgs_monitoring_features_query_point_rdb_09129600.rdb"))
    ]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()


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
                                                   ({"parent_features": ['USGS-02']}, 118),
                                                   ({"parent_features": ['USGS-0202'], "feature_type": "subbasin"}, 8)],
                         ids=["all", "region_by_id", "region", "subregion", "basin", "subbasin", "watershed", "subwatershed",
                              "site", "plot", "vertical_path", "horizontal_path", "all_by_region", "subbasin_by_subregion"])
def test_usgs_monitoring_features(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("new_huc_rdb.txt"))]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

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
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    print(query.values(), "count:", count, "expected:", expected_count)

    assert count == expected_count


@pytest.mark.parametrize("query, expected_count", [({"monitoring_features": ["USGS-09129600"], "feature_type": "point"}, 1)],
                         ids=["point_by_id"])
def test_usgs_monitoring_features3(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([
        get_url(get_json("usgs_monitoring_features_query_point_09129600.json")),
        get_url_text(get_text("usgs_monitoring_features_query_point_rdb_09129600.rdb"))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0

    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    assert count == expected_count


@pytest.mark.parametrize("query, expected_count", [({"feature_type": "point"}, 0),
                                                  ({"parent_features": ['USGS-020200'], "feature_type": "point"}, 0)],
                        ids=["point", "invalid_points"])
def test_usgs_monitoring_features_invalid_query(query, expected_count, monkeypatch):
    """Test USGS search by region  """

    response = get_url_text(get_text("invalid_url.txt"), 400)
    mock_get_url = MagicMock(side_effect=list([response]))
    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url)

    # TODO should there be some kind of exception handling for invalid queries that don't return anything?

    response_dict = response.__dict__
    is_400_response = 'HTTP Status 400' in response_dict['text'] and response_dict['status_code'] == 400

    if is_400_response is True:
        count = 0
        assert count == expected_count


def test_usgs_get_data(monkeypatch):

    mock_get_url = MagicMock(side_effect=list([get_url_text(get_text("usgs_get_data_rdb_09110000.rdb")),
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
    assert var_metadata['quality'] == ResultQualityEnum.VALIDATED
    assert var_metadata['sampling_medium'] == SamplingMedium.WATER
    assert var_metadata['sampling_feature_id'] == 'USGS-09110000'
    assert var_metadata['datasource'] == 'USGS'
    assert var_metadata['datasource_variable'] == '00060'

    assert usgs_metadata_df['USGS-09110000__WT__MIN']['statistic'] == 'MIN'
    assert usgs_metadata_df['USGS-09110000__WT__MAX']['statistic'] == 'MAX'

# set the following header names
mean_rdc = 'USGS-09110000__RDC__MEAN'
mean_wt = 'USGS-09110000__WT__MEAN'
min_wt = 'USGS-09110000__WT__MIN'
max_wt = 'USGS-09110000__WT__MAX'

# set short variable names for ResultQualityEnum values
VAL = ResultQualityEnum.VALIDATED
UNVAL = ResultQualityEnum.UNVALIDATED
REJECTED = ResultQualityEnum.REJECTED
EST = ResultQualityEnum.ESTIMATED
NOT_SUP = ResultQualityEnum.NOT_SUPPORTED
@pytest.mark.parametrize('query, usgs_response, expected_shape, expected_columns, expected_record_counts, expected_quality_metadata',
                         [({'statistic': ['MEAN']}, 'usgs_get_data_09110000_MEAN.json', (4, 3), [mean_rdc, mean_wt], [4, 4], [VAL, VAL]),
                          ({'statistic': ['MIN', 'MAX']}, 'usgs_get_data_09110000_MIN_MAX.json', (4, 3), [min_wt, max_wt], [4, 4], [VAL, VAL]),
                          # quality: all VAL, query VAL'
                          ({'result_quality': [VAL]}, 'usgs_get_data_09110000_VALIDATED.json', (4, 3), [mean_rdc, mean_wt], [4, 4], [VAL, VAL]),
                          # quality: all VAL, query UNVAL
                          ({'result_quality': [UNVAL]}, 'usgs_get_data_09110000_VALIDATED.json', None, None, None, None),
                          # quality: all VAL, query REJECTED (not supported)
                          ({'result_quality': [REJECTED]}, 'usgs_get_data_09110000_VALIDATED.json', None, None, None, None),
                          # quality: mix VAL-UNVAL, query VAL
                          ({'result_quality': [VAL]}, 'usgs_get_data_09110000_VALIDATED_UNVALIDATED.json', (4, 3), [mean_rdc, mean_wt], [4, 2], [VAL, VAL]),
                          # quality: VAL-UNVAL, query UNVAL
                          ({'result_quality': [UNVAL]}, 'usgs_get_data_09110000_VALIDATED_UNVALIDATED.json', (2, 2), [mean_wt], [2], [UNVAL]),
                          # quality: mix VAL-UNVAL, query VAL-UNVAL
                          ({'result_quality': [VAL, UNVAL]}, 'usgs_get_data_09110000_VALIDATED_UNVALIDATED.json', (4, 3), [mean_rdc, mean_wt], [4, 4], [VAL, f'{VAL};{UNVAL}']),
                          # quality: mix VAL-UNVAL-EST, query VAL-UNVAL
                          ({'result_quality': [VAL, UNVAL]}, 'usgs_get_data_09110000_VALIDATED_UNVALIDATED_ESTIMATED.json', (4, 5), [mean_rdc, mean_wt, min_wt, max_wt], [4, 1, 4, 4], [UNVAL, VAL, VAL, VAL]),
                          # query: mix VAL-UNVAL-EST, query EST
                          ({'result_quality': [EST]}, 'usgs_get_data_09110000_VALIDATED_UNVALIDATED_ESTIMATED.json', (2, 2), [mean_wt], [2], [EST]),
                          # query: mix VAL-UNVAL-EST, no query (includes NOT_SUPPORTED)
                          ({}, 'usgs_get_data_09110000_VALIDATED_UNVALIDATED_ESTIMATED.json', (4, 5), [mean_rdc, mean_wt, min_wt, max_wt], [4, 4, 4, 4, 4], [UNVAL, f'{VAL};{NOT_SUP};{EST}', VAL, VAL]),
                          ],
                         ids=['statistic: mean', 'statistic: min-max',
                              'quality: all VAL, query VAL', 'quality: all VAL, query UNVAL', 'quality: all VAL, query REJECTED (not supported)',
                              'quality: mix VAL-UNVAL, query VAL', 'quality: VAL-UNVAL, query UNVAL', 'quality: mix VAL-UNVAL, query VAL-UNVAL',
                              'quality: mix VAL-UNVAL-EST, query VAL-UNVAL', 'query: mix VAL-UNVAL-EST, query EST', 'query: mix VAL-UNVAL-EST, no query (includes NOT_SUPPORTED)'])
def test_usgs_get_data_with_queries(query, usgs_response, expected_shape, expected_columns, expected_record_counts,
                                    expected_quality_metadata, monkeypatch):

    get_rdb = get_url_text(get_text("usgs_get_data_rdb_09110000.rdb"))
    mock_get_url_mean = MagicMock(side_effect=list([get_rdb,
                                                    get_url(get_json(usgs_response))]))

    monkeypatch.setattr(basin3d.plugins.usgs, 'get_url', mock_get_url_mean)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    # check filtering by query
    usgs_data = get_timeseries_data(synthesizer=synthesizer, monitoring_features=["USGS-09110000"],
                                    observed_property_variables=['RDC', 'WT'], start_date='2019-10-25',
                                    end_date='2019-10-28', **query)

    if expected_shape is not None:
        # check the dataframe
        usgs_df = usgs_data.data
        assert isinstance(usgs_df, pd.DataFrame) is True
        expected_columns.append('TIMESTAMP')
        for column_name in list(usgs_df.columns):
            assert column_name in expected_columns
        assert usgs_df.shape == expected_shape

        # check metadata
        usgs_metadata_store = usgs_data.metadata
        # check record counts
        for idx, column_name in enumerate(expected_columns):
            if column_name == 'TIMESTAMP':
                continue
            var_metadata = usgs_metadata_store.get(column_name)
            assert var_metadata['records'] == expected_record_counts[idx]
            result_quality = var_metadata['quality']
            expected_quality = expected_quality_metadata[idx].split(';')
            assert all(qual in result_quality for qual in expected_quality) and all(qual in expected_quality for qual in result_quality.split(';')) is True

    else:
        assert usgs_data.data is None
        assert usgs_data.metadata is None
