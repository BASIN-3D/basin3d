import pytest

from typing import Iterator

from pydantic import ValidationError

from basin3d.core.models import Base, RelatedSamplingFeature
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
        "monitoring_feature": [(-106.9, 38.65, -106.8, 38.67), (-106.7, 38.85, -106.5, 39.0)],
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
        "monitoring_feature": [(-106.7, 38.85, -106.5, 39.0), "USGS-09106800"],
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
@pytest.mark.parametrize("query, feature_type, result_parent_feature_id, result_parent_feature_type",
                         [({"id": "USGS-13"}, "region", None, None),
                          ({"id": "USGS-0102"}, "subregion", "USGS-01", "REGION"),
                          ({"id": "USGS-011000"}, "basin", "USGS-0110", "SUBREGION"),
                          ({"id": "USGS-01020004"}, "subbasin", "USGS-010200", "BASIN"),
                          ({"id": "USGS-09129600", "feature_type": "point"}, "point", "USGS-14020002", "SUBBASIN"),
                          ({"id": "USGS-383103106594200", "feature_type": "POINT"}, "point", "USGS-14020002", "SUBBASIN")],
                         ids=["region", "subregion", "basin", "subbasin", "point", "point_long_id"])
def test_usgs_monitoring_feature(query, feature_type, result_parent_feature_id, result_parent_feature_type):
    """ Test USGS search by monitoring feature id """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()
    if result_parent_feature_id is None:
        assert monitoring_feature.related_sampling_feature_complex == []
    else:
        result_related_sampling_feature = monitoring_feature.related_sampling_feature_complex[0]
        assert isinstance(result_related_sampling_feature, RelatedSamplingFeature)
        assert result_related_sampling_feature.related_sampling_feature == result_parent_feature_id
        assert result_related_sampling_feature.related_sampling_feature_type == result_parent_feature_type


def test_usgs_monitoring_feature_get_invalid():
    """ Test USGS search by monitoring feature id """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(id="USGS-09129600")

    assert response.data is None


@pytest.mark.integration
@pytest.mark.parametrize("query, expected_count", [({"datasource": "USGS"}, 3131),  # datasource - all hucs
                                                   ({"monitoring_feature": ['USGS-02']}, 1),  # region by id
                                                   ({"feature_type": "region"}, 22),  # region
                                                   ({"feature_type": "subregion"}, 246),  #subregion
                                                   ({"feature_type": "basin"}, 407),  # basin
                                                   ({"feature_type": "subbasin"}, 2456),  # subbasin
                                                   # feature types not supported
                                                   ({"feature_type": "watershed"}, 0),
                                                   ({"feature_type": "subwatershed"}, 0),
                                                   ({"feature_type": "site"}, 0),
                                                   ({"feature_type": "plot"}, 0),
                                                   ({"feature_type": "vertical_path"}, 0),
                                                   ({"feature_type": "horizontal_path"}, 0),
                                                   # invalid query for point: must also have a monitoring feature or parent feature specification
                                                   ({"feature_type": "point"}, 0),
                                                   # valid queries
                                                   ({"monitoring_feature": ["USGS-09129600"], "feature_type": "point"}, 1),
                                                   ({"parent_feature": ['USGS-020200'], "feature_type": "point"}, 22304),
                                                   ({"parent_feature": ['USGS-02020004'], "feature_type": "point"}, 3944),
                                                   ({"parent_feature": ['USGS-02']}, 108),  # should return all supported huc levels (subregion, basin, subbasin) in this region
                                                   ({"parent_feature": ['USGS-0202'], "feature_type": "subbasin"}, 8),
                                                   ({"monitoring_feature": [(-106.7, 38.5, -106.5, 39.9)], "feature_type": "point"}, 173),
                                                   ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0), (-106.7, 38.5, -106.5, 39.0)], "feature_type": "point"}, 33),
                                                   ({"monitoring_feature": [(-106.7, 38.9, -106.5, 39.0), "USGS-09129600"], "feature_type": "point"}, 4),
                                                   ({"monitoring_feature": [(-106.71, 39.58, -106.7, 39.59)], "feature_type": "point"}, 0),
                                                   ],
                         ids=["datasouce - all hucs", "region_by_id", "region", "subregion",
                              "basin", "subbasin",
                              "watershed", "subwatershed", "site", "plot", "vertical_path", "horizontal_path",
                              "point_invalid",
                              "single_point_monitoring_feature", "points_by_basin", "points_by_subbasin",
                              "all_by_region", "subbasin_by_subregion",
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
