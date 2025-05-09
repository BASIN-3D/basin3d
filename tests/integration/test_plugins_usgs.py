import pytest

from typing import Iterator

from pydantic import ValidationError

from basin3d.core.models import Base
from basin3d.core.schema.enum import ResultQualityEnum, TimeFrequencyEnum
from basin3d.synthesis import register


@pytest.mark.integration
def test_measurement_timeseries_tvp_observations_usgs():
    """ Test USGS Timeseries data query"""

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query0 = {
        "monitoring_feature": ["USGS-09110990", "USGS-09111250"],
        "observed_property": [],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "VALIDATED"
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query0)

    query1 = {
        "monitoring_feature": ["USGS-09110990", "USGS-09111250"],
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "VALIDATED"
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query1)

    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 2
    else:
        pytest.fail("Returned object must be iterator")

    query2 = {
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "VALIDATED"
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query2)

    query2a = {
        "monitoring_feature": [(1, 2, 3)],
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "VALIDATED"
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query2a)

    query3 = {
        "monitoring_feature": ["USGS-09110990", "USGS-09111250"],
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "statistic": ["MEAN"],
        "results_quality": "VALIDATED"
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query3)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 2
    else:
        pytest.fail("Returned object must be iterator")

    query4 = {
        "monitoring_feature": ["USGS-09110990", "USGS-09111250"],
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": None,
        "results_quality": ResultQualityEnum.VALIDATED
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query4)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 2
    else:
        pytest.fail("Returned object must be iterator")

    query5 = {
        "monitoring_feature": [(-106.9, -106.8, 38.65, 38.67), (-106.7, -106.5, 38.9, 39.0)],
        "observed_property": ["RDC"],
        "start_date": "2024-04-01",
        "end_date": "2024-04-10",
        "aggregation_duration": None,
        "results_quality": ResultQualityEnum.VALIDATED
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query5)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 3
    else:
        pytest.fail("Returned object must be iterator")

    query6 = {
        "monitoring_feature": [(-106.7, -106.5, 38.9, 39.0), "USGS-09106800"],
        "observed_property": ["RDC"],
        "start_date": "2024-04-01",
        "end_date": "2024-04-10",
        "aggregation_duration": None,
        "results_quality": ResultQualityEnum.VALIDATED
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query6)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 1
    else:
        pytest.fail("Returned object must be iterator")

    query7 = {
        "monitoring_feature": ["USGS-09110990"],
        "observed_property": ["WT"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": None,
        "results_quality": ResultQualityEnum.VALIDATED
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query7)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 0
    else:
        pytest.fail("Returned object must be iterator")


@pytest.mark.integration
@pytest.mark.parametrize("query, feature_type", [({"id": "USGS-13"}, "region"),
                                                 ({"id": "USGS-0102"}, "subregion"),
                                                 ({"id": "USGS-011000"}, "basin"),
                                                 ({"id": "USGS-01020004"}, "subbasin"),
                                                 ({"id": "USGS-09129600"}, "point"),
                                                 ({"id": "USGS-383103106594200", "feature_type": "POINT"}, "point")],
                         ids=["region", "subregion", "basin", "subbasin", "point", "point_long_id"])
def test_usgs_monitoring_feature(query, feature_type):
    """ Test USGS search by monitoring feature id """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()


@pytest.mark.integration
@pytest.mark.parametrize("query, expected_count", [({"datasource": "USGS"}, 2889),
                                                   ({"monitoring_feature": ['USGS-02']}, 1),
                                                   ({"feature_type": "region"}, 21),
                                                   ({"feature_type": "subregion"}, 222),
                                                   ({"feature_type": "basin"}, 379),
                                                   ({"feature_type": "subbasin"}, 2267),
                                                   ({"feature_type": "watershed"}, 0),
                                                   ({"feature_type": "subwatershed"}, 0),
                                                   ({"feature_type": "site"}, 0),
                                                   ({"feature_type": "plot"}, 0),
                                                   ({"feature_type": "vertical_path"}, 0),
                                                   ({"feature_type": "horizontal_path"}, 0),
                                                   ({"feature_type": "point"}, 0),
                                                   ({"monitoring_feature": ["USGS-09129600"], "feature_type": "point"}, 1),
                                                   ({"parent_feature": ['USGS-02']}, 118),
                                                   ({"parent_feature": ['USGS-02020004'], "feature_type": "point"}, 54),
                                                   ({"parent_feature": ['USGS-0202'], "feature_type": "subbasin"}, 8),
                                                   ({"parent_feature": ['USGS-020200'], "feature_type": "point"}, 0),
                                                   ({"monitoring_feature": [(-106.7, -106.5, 38.5, 39.9)], "feature_type": "point"}, 57),
                                                   ({"monitoring_feature": [(-106.7, -106.5, 38.9, 39.0), (-106.7, -106.5, 38.5, 39.0)], "feature_type": "point"}, 8),
                                                   ({"monitoring_feature": [(-106.7, -106.5, 38.9, 39.0), "USGS-09129600"], "feature_type": "point"}, 2),
                                                   ({"monitoring_feature": [(-106.71, -106.7, 39.58, 39.59)], "feature_type": "point"}, 0),
                                                   ],
                         ids=["all", "region_by_id", "region", "subregion",
                              "basin", "subbasin",
                              "watershed", "subwatershed",
                              "site", "plot",
                              "vertical_path",
                              "horizontal_path",
                              "point", "point_by_id", "all_by_region",
                              "points_by_subbasin",
                              "subbasin_by_subregion", "invalid_points",
                              "single_bbox_many_sites", "2_bbox_overlap", "mix", "empty"])
def test_usgs_monitoring_features(query, expected_count):
    """Test USGS monitoring features """

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
