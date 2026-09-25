import pytest

from typing import Iterator

from pydantic import ValidationError

from basin3d.core.schema.enum import ResultQualityEnum
from basin3d.synthesis import register


# NOTE: the environmental variables for arm user name and token must be available for these tests to work


@pytest.mark.integration
def test_measurement_timeseries_tvp_observations_arm():
    """ Test ARM Timeseries data query"""

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])

    # validation error -- no observed property
    query0 = {
        "monitoring_feature": ["ARM-SGP-E12", "ARM-SGP-E15"],
        "observed_property": [],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "MINUTE",
        "results_quality": "VALIDATED"
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query0)

    query1 = {
        "monitoring_feature": ["ARM-SGP-E12", "ARM-SGP-E15"],
        "observed_property": ["AT"],
        "start_date": "2023-04-01",
        "end_date": "2023-05-04",
        "aggregation_duration": "MINUTE",
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query1)

    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 1
    else:
        pytest.fail("Returned object must be iterator")

    # no monitoring feature
    query2 = {
        "observed_property": ["AT"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "",
        "results_quality": "VALIDATED"
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query2)

    # incorrect monitoring feature bbox
    query3 = {
        "monitoring_feature": [(1, 2, 3)],
        "observed_property": ["AT"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": "DAY",
        "results_quality": "VALIDATED"
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query3)

    query4 = {
        "monitoring_feature": ["ARM-SGP-E12", "ARM-SGP-E15"],
        "observed_property": ["W_SPD"],
        "start_date": "2023-04-01",
        "end_date": "2023-05-04",
        "aggregation_duration": "MINUTE",
        "statistic": ["MEAN"],
        "results_quality": "VALIDATED"
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query4)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            result_values = timeseries.result.value
            assert len(result_values) == 48955
            mf = timeseries.feature_of_interest
            assert mf.id == "ARM-SGP-E15"
            count += 1

        assert count == 1
    else:
        pytest.fail("Returned object must be iterator")

    # SGP-E33, SGP-E37, SGP-E39, SGP-E12, SGP-E13, SGP-E32
    query5 = {
        "monitoring_feature": [(-98.7, 35.85, -97.5, 36.3), (-98.7, 35.85, -95.5, 37.0)],
        "observed_property": ["APA"],
        "start_date": "2025-04-01",
        "end_date": "2025-04-05",
        "aggregation_duration": "MINUTE",
        "results_quality": ResultQualityEnum.VALIDATED
    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query5)
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1

        assert count == 6
    else:
        pytest.fail("Returned object must be iterator")


@pytest.mark.integration
@pytest.mark.parametrize("query, expected_count", [({"datasource": "ARM"}, 66),  # datasource_all_points
                                                   ({"monitoring_feature": ['ARM-SGP-E15']}, 1),  # one_point_by_name
                                                   ({"feature_type": "point"}, 66),  # all_points
                                                   # valid queries
                                                   ({"monitoring_feature": ["ARM-TMP-M1", "ARM-GUC-M1", "ARM-SGP-E12"], "feature_type": "point"}, 3),  # multiple_named
                                                   ({"monitoring_feature": [(-98.7, 35.85, -95.5, 37.0)], "feature_type": "point"}, 15),  # one_bbox
                                                   ({"monitoring_feature": ['ARM-GUC-M1', (-98.7, 35.85, -95.5, 37.0)], "feature_type": "point"}, 16),  # non-overlapping_name_and_bbox
                                                   ({"monitoring_feature": ['ARM-SGP-12', (-98.7, 35.85, -95.5, 37.0)], "feature_type": "point"}, 15),  # overlapping_name_and_bbox
                                                   ({"monitoring_feature": [(-98.7, 35.85, -97.5, 36.3), (-97.5, 36.3, -95.5, 37.0)], "feature_type": "point"}, 11),  # non-overlapping_bboxs
                                                   ({"monitoring_feature": [(-98.7, 35.85, -97.5, 36.3), (-98.7, 35.85, -95.5, 37.0)], "feature_type": "point"}, 15),  # overlapping_bboxs
                                                   ({"monitoring_feature": [(-106.71, 39.58, -106.7, 39.59)], "feature_type": "point"}, 0),  # empty bbox
                                                   ],
                         ids=["datasource_all_points", "one_point_by_name", "all_points", "multiple_named", "one_bbox",
                              "non-overlapping_name_and_bbox", "overlapping_name_and_bbox", "non-overlapping_bboxs",
                              "overlapping_bboxs", "empty_bbox"])
def test_arm_monitoring_features(query, expected_count):
    """Test USGS monitoring features """

    synthesizer = register(['basin3d.plugins.arm.ARMDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0
    for mf in monitoring_features:
        count += 1
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    assert count == expected_count
