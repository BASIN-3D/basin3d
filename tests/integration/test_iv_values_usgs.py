from typing import Iterator

import pytest
from pydantic import ValidationError

from basin3d.core.schema.enum import ResultQualityEnum, TimeFrequencyEnum
from basin3d.synthesis import register


@pytest.mark.integration
def test_measurement_timeseries_tvp_observations_usgs_iv():
    """ Test USGS Timeseries data query"""

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query0 = {
        "monitoring_feature": ["USGS-09110990", "USGS-09111250"],
        "observed_property": [],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.NONE,
        "results_quality": ResultQualityEnum.VALIDATED
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query0)

    query1 = {
        "monitoring_feature": ["USGS-09110990", "USGS-09111250"],
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.NONE,
        "results_quality": ResultQualityEnum.VALIDATED
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
        "monitoring_feature": ["USGS-09110990", "USGS-09111250"],
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.NONE,
        "statistic": ["MEAN"],
        "results_quality": ResultQualityEnum.VALIDATED

    }
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query2)

    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            timeseries.to_json()
            count += 1
            # checking to make sure the query statistic is not the statistic in timeseries data,
            # and checking to make sure that when aggregation duration is NONE the statistic in timeseries data has the value 'NOT_SUPPORTED'
            assert timeseries.statistic.get_basin3d_vocab() == 'NOT_SUPPORTED'
        expected_msgs = "USGS Instantaneous Values service does not support statistics and cannot be specified when aggregation_duration = NONE. Specified statistic arguments will be ignored."
        msgs = measurement_timeseries_tvp_observations.synthesis_response.messages[0].msg
        assert msgs == expected_msgs
        assert count == 2

    else:
        pytest.fail("Returned object must be iterator")

    query3 = {
        "observed_property": ["RDC"],
        "start_date": "2020-04-01",
        "end_date": "2020-04-30",
        "aggregation_duration": TimeFrequencyEnum.NONE,
        "statistic": ["MEAN"],
        "results_quality": ResultQualityEnum.VALIDATED
    }

    with pytest.raises(ValidationError):
        synthesizer.measurement_timeseries_tvp_observations(**query3)
