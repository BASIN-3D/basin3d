from datetime import date
from math import ceil
from unittest.mock import Mock, call, patch

import pytest

from basin3d.plugins.usgs import (_calculate_date_ranges, _convert_discharge,
                                  _filter_timeseries_metadata, _get_huc_lookup,
                                  _get_monitoring_location_observed_properties, _get_unique_sites,
                                  _get_usgs_results, _load_huc_obj, _load_point_obj,
                                  _parse_usgs_response, _tsm_query_filter)
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP


# ================================
# TESTS for _calculate_date_ranges -- ChatGPT helped write these tests
@pytest.mark.parametrize(
    'start_date,end_date,limit,mode,expected',
    [
        pytest.param(date(2026, 1, 1), date(2026, 1, 1), 10, 'daily', [(date(2026, 1, 1), date(2026, 1, 1), 1)], id='daily_single_day'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 5), 10, 'daily', [(date(2026, 1, 1), date(2026, 1, 5), 5)], id='daily_range_fits_in_single_tuple'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 7), 3, 'daily', [(date(2026, 1, 1), date(2026, 1, 3), 3), (date(2026, 1, 4), date(2026, 1, 6), 3), (date(2026, 1, 7), date(2026, 1, 7), 1)], id='daily_range_is_split_at_limit'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 1), 96, 'continuous', [(date(2026, 1, 1), date(2026, 1, 1), 96)], id='continuous_single_day'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 7), 250, 'continuous', [(date(2026, 1, 1), date(2026, 1, 2), 192), (date(2026, 1, 3), date(2026, 1, 4), 192), (date(2026, 1, 5), date(2026, 1, 6), 192), (date(2026, 1, 7), date(2026, 1, 7), 96)], id='continuous_minimum_number_of_tuples'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 6), 288, 'continuous', [(date(2026, 1, 1), date(2026, 1, 3), 288), (date(2026, 1, 4), date(2026, 1, 6), 288)], id='continuous_exact_multiple_of_limit'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 5), 200, 'continuous', [(date(2026, 1, 1), date(2026, 1, 2), 192), (date(2026, 1, 3), date(2026, 1, 4), 192), (date(2026, 1, 5), date(2026, 1, 5), 96)], id='continuous_limit_not_multiple_of_96'),
        pytest.param(date(2026, 1, 30), date(2026, 2, 3), 2, 'daily', [(date(2026, 1, 30), date(2026, 1, 31), 2), (date(2026, 2, 1), date(2026, 2, 2), 2), (date(2026, 2, 3), date(2026, 2, 3), 1)], id='crosses_month_boundary'),
        pytest.param(date(2025, 12, 30), date(2026, 1, 2), 2, 'daily', [(date(2025, 12, 30), date(2025, 12, 31), 2), (date(2026, 1, 1), date(2026, 1, 2), 2)], id='crosses_year_boundary'),
        pytest.param(date(2028, 2, 28), date(2028, 3, 1), 2, 'daily', [(date(2028, 2, 28), date(2028, 2, 29), 2), (date(2028, 3, 1), date(2028, 3, 1), 1)], id='leap_day'),
    ],
)
def test_calculate_date_ranges_valid_cases(
    start_date,
    end_date,
    limit,
    mode,
    expected,
):
    assert _calculate_date_ranges(
        start_date=start_date,
        end_date=end_date,
        total_data_object_limit=limit,
        mode=mode,
    ) == expected


@pytest.mark.parametrize(
    'start_date,end_date,limit,mode,error_message',
    [
        pytest.param(date(2026, 1, 2), date(2026, 1, 1), 100, 'daily', 'end_date must be on or after start_date', id='end_date_before_start_date_raises'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 2), 0, 'daily', 'total_data_object_limit must be greater than zero', id='zero_limit_raises'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 2), -1, 'daily', 'total_data_object_limit must be greater than zero', id='negative_limit_raises'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 2), 95, 'continuous', 'cannot hold one continuous day', id='continuous_limit_below_one_day_raises'),
        pytest.param(date(2026, 1, 1), date(2026, 1, 2), 100, 'hourly', "mode must be 'daily' or 'continuous'", id='invalid_mode_raises'),
    ],
)
def test_calculate_date_ranges_invalid_cases(
    start_date,
    end_date,
    limit,
    mode,
    error_message,
):
    with pytest.raises(ValueError, match=error_message):
        _calculate_date_ranges(
            start_date=start_date,
            end_date=end_date,
            total_data_object_limit=limit,
            mode=mode,  # type: ignore[arg-type]
        )


def test_ranges_are_sequential_and_non_overlapping():
    result = _calculate_date_ranges(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 10),
        total_data_object_limit=250,
        mode='continuous',
    )

    for previous, current in zip(result, result[1:]):
        assert (current[0] - previous[1]).days == 1


def test_ranges_cover_full_requested_period():
    start = date(2026, 1, 1)
    end = date(2026, 1, 10)

    result = _calculate_date_ranges(
        start_date=start,
        end_date=end,
        total_data_object_limit=250,
        mode='continuous',
    )

    assert result[0][0] == start
    assert result[-1][1] == end


@pytest.mark.parametrize(
    'mode,objects_per_day,limit',
    [
        pytest.param('daily', 1, 3, id='daily_returns_minimum_number_of_tuples'),
        pytest.param('continuous', 96, 96, id='continuous_one_day_limit_returns_minimum_number_of_tuples'),
        pytest.param('continuous', 96, 250, id='continuous_partial_day_capacity_returns_minimum_number_of_tuples'),
        pytest.param('continuous', 96, 1000, id='continuous_large_limit_returns_minimum_number_of_tuples'),
    ],
)
def test_returns_minimum_number_of_tuples(mode, objects_per_day, limit):
    start = date(2026, 1, 1)
    end = date(2026, 1, 31)

    result = _calculate_date_ranges(
        start_date=start,
        end_date=end,
        total_data_object_limit=limit,
        mode=mode,
    )

    total_days = (end - start).days + 1
    max_days_per_tuple = limit // objects_per_day
    expected_minimum = ceil(total_days / max_days_per_tuple)

    assert len(result) == expected_minimum


@pytest.mark.parametrize(
    'mode,objects_per_day,limit',
    [
        pytest.param('daily', 1, 4, id='daily_expected_counts_match_number_of_days'),
        pytest.param('continuous', 96, 250, id='continuous_expected_counts_match_number_of_days'),
    ],
)
def test_expected_counts_match_number_of_days(mode, objects_per_day, limit):
    result = _calculate_date_ranges(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 10),
        total_data_object_limit=limit,
        mode=mode,
    )

    for start, end, expected_count in result:
        number_of_days = (end - start).days + 1
        assert expected_count == number_of_days * objects_per_day


@pytest.mark.parametrize(
    'mode,objects_per_day,limit',
    [
        pytest.param('daily', 1, 3, id='total_expected_count_daily'),
        pytest.param('continuous', 96, 250, id='total_expected_count_continuous'),
    ],
)
def test_total_expected_count(mode, objects_per_day, limit):
    result = _calculate_date_ranges(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 10),
        total_data_object_limit=limit,
        mode=mode,
    )

    assert sum(count for _, _, count in result) == 10 * objects_per_day


# ================================
# TESTS for _tsm_query_filter -- ChatGPT helped write these tests

@pytest.mark.parametrize(
    'query_kwargs,tsm_properties,ignore_stats,expected',
    [
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-04'}, False,
                     False, id='tsm_entirely_before_query_range'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-11', 'end': '2026-01-20'}, False,
                     False, id='tsm_entirely_after_query_range'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-06'}, False,
                     True, id='tsm_overlaps_start_of_query_range'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-09', 'end': '2026-01-20'}, False,
                     True, id='tsm_overlaps_end_of_query_range'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-06', 'end': '2026-01-09'}, False,
                     True, id='tsm_contained_within_query_range'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-20'}, False,
                     True, id='query_range_contained_within_tsm'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-05'}, False,
                     True, id='tsm_end_equals_query_start_date'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-10', 'end': '2026-01-20'}, False,
                     True, id='tsm_begin_equals_query_end_date'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-04'}, False,
                     False, id='tsm_before_open_ended_query'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-05'}, False,
                     True, id='tsm_end_equals_open_ended_query_start_date'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     True, id='tsm_overlaps_open_ended_query'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-10', 'end': '2026-01-20'}, False,
                     True, id='tsm_after_open_ended_query_start_date'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'FLOW', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     False, id='parameter_code_not_in_observed_property'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 11)},
                     {'parameter_code': 'FLOW', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     False, id='parameter_code_not_in_observed_property and bad date'),
        pytest.param({'observed_property': ['FLOW', 'TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     True, id='parameter_code_matches_one_of_multiple_observed_properties'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MEAN']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MEAN', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     True, id='statistic_matches'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MEAN']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MAX', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     False, id='statistic_does_not_match'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MIN', 'MEAN', 'MAX']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MEAN', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     True, id='statistic_matches_one_of_multiple_statistics'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01T00:00:00', 'end': '2026-01-10T23:59:59'}, False,
                     True, id='tsm_datetime_values_are_compared_as_dates'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MEAN']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MAX', 'begin': '2026-01-01', 'end': '2026-01-10'}, True,
                     True, id='statistic_does_not_match_but_statistics_are_ignored'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MEAN']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MEAN', 'begin': '2026-01-01', 'end': '2026-01-10'}, True,
                     True, id='statistic_matches_and_statistics_are_ignored'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MEAN']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MAX', 'begin': '2026-01-01', 'end': '2026-01-10'}, False,
                     False, id='statistic_does_not_match_and_statistics_are_not_ignored'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MIN', 'MEAN']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MAX', 'begin': '2026-01-01', 'end': '2026-01-10'}, True,
                     True, id='statistic_not_in_multiple_statistics_but_statistics_are_ignored'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 11), 'statistic': ['MEAN']},
                     {'parameter_code': 'TEMP', 'statistic_id': 'MAX', 'begin': '2026-01-01', 'end': '2026-01-10'}, True,
                     False, id='statistics_ignored_but_date_filter_still_applies'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'statistic': ['MEAN']},
                     {'parameter_code': 'FLOW', 'statistic_id': 'MAX', 'begin': '2026-01-01', 'end': '2026-01-10'}, True,
                     False, id='statistics_ignored_but_observed_property_filter_still_applies'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01', 'end': '2026-01-10'}, True,
                     True, id='statistics_ignored_when_query_has_no_statistic'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '2026-01-01T00:00:00.000001', 'end': '2026-01-10T00:00:00.000001'}, False,
                     True, id='another_timeseries_time_format'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': None, 'end': None}, False,
                     False, id='timeseries_datetime_values_null'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5)},
                     {'parameter_code': 'TEMP', 'begin': '01/01/2026', 'end': '02/01/2026'}, False,
                     False, id='timeseries_datetime_bad_format'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': '01/01/2026', 'end': '2026-01-10'}, False,
                     False, id='timeseries_datetime_start_bad_format'),
        pytest.param({'observed_property': ['TEMP'], 'start_date': date(2026, 1, 5), 'end_date': date(2026, 1, 10)},
                     {'parameter_code': 'TEMP', 'begin': None, 'end': '2026-01-10'}, False,
                     False, id='timeseries_missing_start_date'),
    ],
)
def test_tsm_query_filter(query_kwargs, tsm_properties, ignore_stats, expected):
    query = QueryMeasurementTimeseriesTVP(
        monitoring_feature=['TEST:001'],
        **query_kwargs,
    )

    assert _tsm_query_filter(tsm_properties, query, ignore_stats) is expected


# ================================
# TESTS for _filter_timeseries_metadata -- ChatGPT helped write these tests

@pytest.fixture
def query():
    return QueryMeasurementTimeseriesTVP(
        monitoring_feature=['TEST:001'],
        observed_property=['TEMP'],
        start_date=date(2026, 1, 1),
    )


@pytest.mark.parametrize(
    'tsm_results,filter_results,expected_observed_properties,expected_unique_timeseries,expected_unique_sites',
    [
        pytest.param([{'properties': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                      {'properties': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}],
                     [True, True],
                     {'site-1': {'TEMP'}}, {'tsm-1': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}, {'site-1'},
                     id='duplicate_tsm_objects_only_one_timeseries_maintained'),
        pytest.param([{'properties': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                      {'properties': {'id': 'tsm-2', 'monitoring_location_id': 'site-1', 'parameter_code': 'FLOW'}}],
                     [True, True],
                     {'site-1': {'TEMP', 'FLOW'}}, {'tsm-1': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'},
                                                    'tsm-2': {'id': 'tsm-2', 'monitoring_location_id': 'site-1', 'parameter_code': 'FLOW'}}, {'site-1'},
                     id='duplicate_monitoring_location_only_one_unique_site_maintained'),
        pytest.param([{'properties': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                      {'properties': {'id': 'tsm-2', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}],
                     [True, True],
                     {'site-1': {'TEMP'}}, {'tsm-1': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'},
                                            'tsm-2': {'id': 'tsm-2', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}, {'site-1'},
                     id='different_timeseries_ids_same_site_and_observed_property'),
        pytest.param([{'properties': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                      {'properties': {'id': 'tsm-2', 'monitoring_location_id': 'site-2', 'parameter_code': 'FLOW'}},
                      {'properties': {'id': 'tsm-3', 'monitoring_location_id': 'site-3', 'parameter_code': 'STAGE'}}],
                     [True, False, True],
                     {'site-1': {'TEMP'}, 'site-2': {'FLOW'}, 'site-3': {'STAGE'}}, {'tsm-1': {'id': 'tsm-1', 'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'},
                                                                                     'tsm-3': {'id': 'tsm-3', 'monitoring_location_id': 'site-3', 'parameter_code': 'STAGE'}}, {'site-1', 'site-3'},
                     id='filtered_tsm_objects_not_added_to_unique_timeseries_or_sites'),
    ],
)
def test_filter_timeseries_metadata(query, tsm_results, filter_results,
                                    expected_observed_properties, expected_unique_timeseries, expected_unique_sites):
    """Test the various duplicate and filtering cases"""

    observed_properties = {}
    unique_timeseries = {}
    unique_sites = set()

    with patch('basin3d.plugins.usgs._tsm_query_filter', side_effect=filter_results, ) as mock_tsm_query_filter:
        _filter_timeseries_metadata(tsm_results, query, observed_properties, unique_timeseries, unique_sites, False)

    assert observed_properties == expected_observed_properties
    assert unique_timeseries == expected_unique_timeseries
    assert unique_sites == expected_unique_sites
    assert mock_tsm_query_filter.call_count == len(tsm_results)


def test_filter_timeseries_metadata_preserves_existing_values(query):
    """Preserve the mutable collections that are inputs to the function"""
    tsm_results = [
        {
            'properties': {
                'id': 'tsm-2',
                'monitoring_location_id': 'site-1',
                'parameter_code': 'FLOW',
            },
        },
    ]
    observed_properties = {
        'site-1': {'TEMP'},
    }
    unique_timeseries = {
        'tsm-1': {
            'id': 'tsm-1',
            'monitoring_location_id': 'site-1',
            'parameter_code': 'TEMP',
        },
    }
    unique_sites = {'site-1'}

    with patch('basin3d.plugins.usgs._tsm_query_filter', return_value=True):
        _filter_timeseries_metadata(tsm_results, query, observed_properties, unique_timeseries, unique_sites, False)

    assert observed_properties == {
        'site-1': {'TEMP', 'FLOW'},
    }
    assert unique_timeseries == {
        'tsm-1': {
            'id': 'tsm-1',
            'monitoring_location_id': 'site-1',
            'parameter_code': 'TEMP',
        },
        'tsm-2': {
            'id': 'tsm-2',
            'monitoring_location_id': 'site-1',
            'parameter_code': 'FLOW',
        },
    }
    assert unique_sites == {'site-1'}


def test_filtered_tsm_objects_still_update_observed_properties(query):
    """Assert that the available observed properties get captured even if the timeseries is filtered out"""
    tsm_results = [
        {
            'properties': {
                'id': 'tsm-1',
                'monitoring_location_id': 'site-1',
                'parameter_code': 'TEMP',
            },
        },
        {
            'properties': {
                'id': 'tsm-2',
                'monitoring_location_id': 'site-2',
                'parameter_code': 'FLOW',
            },
        },
    ]
    observed_properties = {}
    unique_timeseries = {}
    unique_sites = set()

    with patch('basin3d.plugins.usgs._tsm_query_filter', return_value=False):
        _filter_timeseries_metadata(tsm_results, query, observed_properties, unique_timeseries, unique_sites, False)

    assert observed_properties == {
        'site-1': {'TEMP'},
        'site-2': {'FLOW'},
    }
    assert unique_timeseries == {}
    assert unique_sites == set()


def test_tsm_query_filter_called_with_properties_and_query(query):
    """This protects against accidentally passing tsm_obj rather than tsm_obj['properties'] -- thanks ChatGPT"""
    tsm = {
        'id': 'tsm-1',
        'monitoring_location_id': 'site-1',
        'parameter_code': 'TEMP',
    }
    tsm_results = [
        {'properties': tsm},
    ]

    with patch('basin3d.plugins.usgs._tsm_query_filter', return_value=True) as mock_tsm_query_filter:
        _filter_timeseries_metadata(tsm_results, query, {}, {}, set(), False)

    mock_tsm_query_filter.assert_called_once_with(tsm, query, False)


# ================================
# TESTS for _parse_usgs_response - help from ChatGPT

@pytest.mark.parametrize(
    'response_json,initial_response_result,expected_response_result,expected_next_url',
    [
        pytest.param({'features': [{'id': 'feature-1'}], 'links': [{'rel': 'self'}]},
                     [], [{'id': 'feature-1'}], None,
                     id='response_json_with_no_next_link'),
        pytest.param({'features': [{'id': 'feature-1'}], 'links': [{'rel': 'next', 'href': 'foo'}]},
                     [], [{'id': 'feature-1'}], 'foo',
                     id='response_json_with_valid_next_link'),
        pytest.param({'features': [{'id': 'feature-1'}]},
                     [], [{'id': 'feature-1'}], None,
                     id='response_json_with_no_links_key'),
        pytest.param({'links': [{'rel': 'next', 'href': 'foo'}]},
                     [], [], 'foo',
                     id='response_json_with_no_features_key'),
        pytest.param({'features': [{'id': 'feature-2'}]},
                     [{'id': 'feature-1'}], [{'id': 'feature-1'}, {'id': 'feature-2'}], None,
                     id='preserve_incoming_mutable_response_result'),
        pytest.param({'features': []},
                     [{'id': 'feature-1'}], [{'id': 'feature-1'}], None,
                     id='empty_features_preserves_existing_response_result'),
        pytest.param({}, [{'id': 'feature-1'}],
                     [{'id': 'feature-1'}], None,
                     id='empty_response_json_preserves_existing_response_result'),
    ],
)
def test_parse_usgs_response_valid_cases(response_json, initial_response_result,
                                         expected_response_result, expected_next_url):
    usgs_response = Mock()
    usgs_response.json.return_value = response_json
    response_result = initial_response_result
    synthesis_messages = []

    next_url = _parse_usgs_response(usgs_response, response_result, synthesis_messages, 'URL1')

    assert next_url == expected_next_url
    assert response_result == expected_response_result
    assert synthesis_messages == []


@pytest.mark.parametrize(
    'response_json,error_message',
    [
        pytest.param({'features': [{'id': 'feature-3'}], 'links': [{'rel': 'next'}]},
                     'Request response contains a next link without a valid href; Cannot continue pagination for initial request URL1',
                     id='missing_next_href_warns_and_clears_results'),
        pytest.param({'features': [{'id': 'feature-3'}], 'links': [{'rel': 'next', 'href': ''}]},
                     'Request response contains a next link without a valid href; Cannot continue pagination for initial request URL1',
                     id='empty_next_href_warns_and_clears_results'),
        pytest.param({'features': [{'id': 'feature-3'}], 'links': [{'rel': 'next', 'href': 'page-2'}, {'rel': 'next', 'href': 'page-3'}]},
                     'Request response contains multiple next links; Cannot continue pagination for initial request URL1',
                     id='duplicate_next_links_warn_and_clear_results'),
        pytest.param({'features': [{'id': 'feature-3'}], 'links': [{'rel': 'next'}, {'rel': 'next', 'href': 'page-2'}]},
                     'Request response contains multiple next links; Cannot continue pagination for initial request URL1',
                     id='duplicate_next_links_with_one_missing_href_warn_and_clear_results'),
    ],
)
def test_parse_usgs_response_invalid_pagination(caplog, response_json, error_message):
    caplog.clear()

    usgs_response = Mock()
    usgs_response.json.return_value = response_json
    response_result = [
        {'id': 'feature-1'},
        {'id': 'feature-2'},
    ]
    synthesis_messages = []

    with caplog.at_level('ERROR'):
        next_url = _parse_usgs_response(usgs_response, response_result, synthesis_messages, 'URL1')

    assert next_url is None
    assert response_result == []
    assert error_message in caplog.text
    assert synthesis_messages and synthesis_messages[0] == error_message


def test_parse_usgs_response_preserves_response_result_list_identity():
    """Confirm extension of list inside function is reflected outside the function"""
    usgs_response = Mock()
    usgs_response.json.return_value = {
        'features': [
            {'id': 'feature-2'},
        ],
    }
    response_result = [
        {'id': 'feature-1'},
    ]
    original_response_result = response_result

    _parse_usgs_response(usgs_response, response_result, [], 'URL1')

    assert response_result is original_response_result
    assert response_result == [
        {'id': 'feature-1'},
        {'id': 'feature-2'},
    ]

def test_missing_next_href_logs_error_and_clears_results(caplog):
    """Error in pagination clears the response list outside of the function"""
    caplog.clear()
    usgs_response = Mock()
    usgs_response.json.return_value = {
        'features': [
            {'id': 'feature-3'},
        ],
        'links': [
            {'rel': 'next'},
        ],
    }
    response_result = [
        {'id': 'feature-1'},
        {'id': 'feature-2'},
    ]

    error_msg = ('Request response contains a next link without a valid href; '
                 'Cannot continue pagination for initial request URL1')

    with caplog.at_level('ERROR'):
        next_url = _parse_usgs_response(usgs_response, response_result, [], 'URL1')

    assert next_url is None
    assert response_result == []
    assert any(record.levelname == 'ERROR' and error_msg in record.message for record in caplog.records)


# ================================
# TESTS for _parse_usgs_response - help from ChatGPT

@pytest.mark.parametrize(
    'huc_info_response,expected_lookup',
    [
        pytest.param([{'id': '0101', 'properties': {'hydrologic_unit_name': 'Upper Basin'}},
                      {'id': '0102', 'properties': {'hydrologic_unit_name': 'Lower Basin'}}],
                     {'0101': 'Upper Basin', '0102': 'Lower Basin'}, id='normal_huc_response_returns_expected_lookup'),
        pytest.param([{'id': '0101'}],
                     {'0101': 'not provided'}, id='missing_properties_returns_default_name'),
        pytest.param([{'id': '0101', 'properties': {}}],
                     {'0101': 'not provided'}, id='missing_hydrologic_unit_name_returns_default_name'),
        pytest.param([], {}, id='empty_huc_response_returns_empty_lookup'),
    ],
)
def test_get_huc_lookup(huc_info_response, expected_lookup):
    datasource_location = 'foo'
    http_connection = object()
    huc_filter = 'id=0101'
    synthesis_messages = []

    with patch('basin3d.plugins.usgs._get_usgs_results', return_value=(huc_info_response, False)) as mock_get_usgs_results:
        result = _get_huc_lookup(datasource_location, http_connection, huc_filter, synthesis_messages,)

    assert result == expected_lookup

    mock_get_usgs_results.assert_called_once_with(
        http_connection,
        (
            'foo'
            '/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000'
            '&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0'
            '&id=0101'
        ),
        synthesis_messages,
    )


def test_get_huc_lookup_returns_empty_on_pagination_error():

    with patch('basin3d.plugins.usgs._get_usgs_results', return_value=([], True)):
        result = _get_huc_lookup('foo', object(), 'id=0101', [])

    assert result == {}


# ================================
# TESTS for _get_usgs_results - with help from Chat GPT
def test_get_usgs_results_single_request_success():
    http_connection = Mock()
    usgs_response = Mock(status_code=200)
    http_connection.get.return_value = usgs_response
    synthesis_messages = []

    with patch('basin3d.plugins.usgs._parse_usgs_response', return_value=None) as mock_parse_usgs_response:
        result, has_pagination_error = _get_usgs_results(http_connection, 'foo', synthesis_messages)

    assert result == []
    assert has_pagination_error is False
    assert synthesis_messages == []
    http_connection.get.assert_called_once_with('foo')
    mock_parse_usgs_response.assert_called_once_with(usgs_response, result, synthesis_messages, 'foo')


def test_get_usgs_results_two_requests_successful_pagination():
    http_connection = Mock()
    first_response = Mock(status_code=200)
    second_response = Mock(status_code=200)
    http_connection.get.side_effect = [first_response, second_response]
    synthesis_messages = []

    def parse_response(response, results, synthesis_messages, initial_url):
        if response is first_response:
            results.append({'id': 'feature-1'})
            return 'foo-2'

        results.append({'id': 'feature-2'})
        return None

    with patch('basin3d.plugins.usgs._parse_usgs_response', side_effect=parse_response) as mock_parse_usgs_response:
        result, has_pagination_error = _get_usgs_results(http_connection, 'foo-1', synthesis_messages)

    assert result == [{'id': 'feature-1'}, {'id': 'feature-2'}]
    assert has_pagination_error is False
    assert synthesis_messages == []

    assert http_connection.get.call_args_list == [call('foo-1'), call('foo-2')]
    assert mock_parse_usgs_response.call_count == 2


@pytest.mark.parametrize('ignore_error_msg', [False, True])
def test_get_usgs_results_non_200_response_on_first_request(ignore_error_msg):
    http_connection = Mock()
    usgs_response = Mock(status_code=500)
    usgs_response.json = {'error': 'server error'}
    http_connection.get.return_value = usgs_response
    synthesis_messages = []

    with patch('basin3d.plugins.usgs._parse_usgs_response') as mock_parse_usgs_response:
        result, has_pagination_error = _get_usgs_results(http_connection, 'foo-1', synthesis_messages,
                                                         ignore_error_msg=ignore_error_msg)

    assert result == []
    assert has_pagination_error is False
    assert len(synthesis_messages) == 1
    assert 'Problem with request to foo-1' in synthesis_messages[0]
    assert 'part of the pagination' not in synthesis_messages[0]
    mock_parse_usgs_response.assert_not_called()


def test_get_usgs_results_non_200_response_during_pagination():
    http_connection = Mock()
    first_response = Mock(status_code=200)
    second_response = Mock(status_code=500)
    second_response.json = {'error': 'server error'}
    http_connection.get.side_effect = [first_response, second_response]
    synthesis_messages = []

    def parse_response(response, results, synthesis_messages, initial_url):
        results.append({'id': 'feature-1'})
        return 'foo-2'

    with patch('basin3d.plugins.usgs._parse_usgs_response', side_effect=parse_response) as mock_parse_usgs_response:
        result, has_pagination_error = _get_usgs_results(http_connection, 'foo-1', synthesis_messages)

    assert result == []
    assert has_pagination_error is True
    assert len(synthesis_messages) == 1
    assert 'Problem with request to foo-2' in synthesis_messages[0]
    assert 'part of the pagination' in synthesis_messages[0]
    assert 'foo-1' in synthesis_messages[0]

    assert http_connection.get.call_args_list == [call('foo-1'), call('foo-2')]
    mock_parse_usgs_response.assert_called_once()


def test_get_usgs_results_request_page_limit_reached():
    http_connection = Mock()
    first_response = Mock(status_code=200)
    second_response = Mock(status_code=200)
    http_connection.get.side_effect = [first_response, second_response]
    synthesis_messages = []

    with patch('basin3d.plugins.usgs._parse_usgs_response', side_effect=['foo-2', 'foo-3']) as mock_parse_usgs_response:
        result, has_pagination_error = _get_usgs_results(http_connection, 'foo-1', synthesis_messages, request_page_limit=2)

    assert result == []
    assert has_pagination_error is True
    assert len(synthesis_messages) == 1
    assert 'Pagination exceeded the default basin3d request limit' in synthesis_messages[0]
    assert '2' in synthesis_messages[0]
    assert 'foo-1' in synthesis_messages[0]

    assert http_connection.get.call_count == 2
    assert mock_parse_usgs_response.call_count == 2


def test_get_usgs_results_preserves_passed_results():
    http_connection = Mock()
    usgs_response = Mock(status_code=200)
    http_connection.get.return_value = usgs_response
    synthesis_messages = []
    results = [{'id': 'existing-feature'}]
    original_results = results

    def parse_response(response, results, synthesis_messages, initial_url):
        results.append({'id': 'new-feature'})
        return None

    with patch('basin3d.plugins.usgs._parse_usgs_response', side_effect=parse_response):
        result, has_pagination_error = _get_usgs_results(http_connection, 'foo-1', synthesis_messages, results=results)

    assert result is original_results
    assert has_pagination_error is False
    assert result == [{'id': 'existing-feature'}, {'id': 'new-feature'}]


def test_get_usgs_results_clears_passed_results_if_pagination_fails():
    http_connection = Mock()
    first_response = Mock(status_code=200)
    second_response = Mock(status_code=500)
    second_response.json = {'error': 'server error'}
    http_connection.get.side_effect = [first_response, second_response]
    synthesis_messages = []
    results = [{'id': 'existing-feature'}]
    original_results = results

    with patch('basin3d.plugins.usgs._parse_usgs_response', return_value='foo-2'):
        result, has_pagination_error = _get_usgs_results(http_connection, 'foo-1', synthesis_messages, results=results)

    assert result is original_results
    assert has_pagination_error is True
    assert result == []
    assert results == []


def test_get_usgs_results_page_limit_can_ignore_configured_limit_message():
    http_connection = Mock()
    first_response = Mock(status_code=200)
    http_connection.get.return_value = first_response
    synthesis_messages = []
    results = [{'id': 'partial-result'}]

    with patch('basin3d.plugins.usgs._parse_usgs_response', return_value='next-page'):
        result, has_pagination_error = _get_usgs_results(
            http_connection, 'foo-1', synthesis_messages, results=results,
            request_page_limit=1, ignore_error_msg=True)

    assert result is results
    assert result == []
    assert results == []
    assert has_pagination_error is True
    assert synthesis_messages == []
    http_connection.get.assert_called_once_with('foo-1')


@pytest.mark.parametrize('ignore_error_msg', [False, True])
def test_get_usgs_results_negative_page_limit(ignore_error_msg):
    http_connection = Mock()
    synthesis_messages = []

    result, has_pagination_error = _get_usgs_results(
        http_connection, 'foo-1', synthesis_messages,
        request_page_limit=-1, ignore_error_msg=ignore_error_msg)

    assert result == []
    http_connection.get.assert_not_called()

    if ignore_error_msg:
        assert has_pagination_error is False
        assert synthesis_messages == []
    else:
        assert has_pagination_error is True
        assert len(synthesis_messages) == 1
        assert synthesis_messages[0] == (
            'Page request limit cannot be negative. No results are returned. '
            'See instructions for how to set a custom request page limit.'
        )


# ================================
# TESTS for _get_monitoring_location_observed_properties - with help from Chat GPT

@pytest.mark.parametrize(
    'tsm_filters,tsm_results,expected_lookup',
    [
        pytest.param(['parameter_code=TEMP'], [[{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}]],
                     {'site-1': {'TEMP'}},
                     id='single_filter_single_site_and_property'),
        pytest.param(['parameter_code=TEMP'], [[{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                                                {'properties': {'monitoring_location_id': 'site-2', 'parameter_code': 'TEMP'}}]],
                     {'site-1': {'TEMP'}, 'site-2': {'TEMP'}},
                     id='single_filter_multiple_sites'),
        pytest.param(['parameter_code=TEMP'], [[{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                                                {'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}]],
                     {'site-1': {'TEMP'}},
                     id='single_filter_same_site_and_property'),
        pytest.param(['monitoring_location_id=site-1'], [[{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                                                          {'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'FLOW'}}]],
                     {'site-1': {'TEMP', 'FLOW'}},
                     id='single_filter_multiple_properties_for_same_site'),
        pytest.param(['filter-1', 'filter-2'], [[{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}],
                                                [{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}]],
                     {'site-1': {'TEMP'}},
                     id='same_results_from_multiple_filters_are_deduplicated'),
        pytest.param(['filter-1', 'filter-2'], [[{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                                                 {'properties': {'monitoring_location_id': 'site-2', 'parameter_code': 'FLOW'}}],
                                                [{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}},
                                                 {'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'STAGE'}}]],
                     {'site-1': {'TEMP', 'STAGE'}, 'site-2': {'FLOW'}},
                     id='overlapping_results_are_merged'),
        pytest.param(['filter-1', 'filter-2'], [[{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}],
                                                [{'properties': {'monitoring_location_id': 'site-2', 'parameter_code': 'FLOW'}}]],
                     {'site-1': {'TEMP'}, 'site-2': {'FLOW'}},
                     id='mutually_exclusive_results_are_combined'),
        pytest.param(['filter-1', 'filter-2', 'filter-3'], [[],
                                                            [{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP'}}],
                                                            []],
                     {'site-1': {'TEMP'}},
                     id='some_empty_results_do_not_affect_lookup'),
        pytest.param(['filter-1', 'filter-2'], [[], []],
                     {},
                     id='all_empty_results_return_empty_lookup'),
    ],
)
def test_get_monitoring_location_observed_properties(tsm_filters, tsm_results, expected_lookup):
    datasource_location = 'foo'
    http_connection = object()
    synthesis_messages = []

    with patch('basin3d.plugins.usgs._get_usgs_results',
               side_effect=[(result, False) for result in tsm_results]) as mock_get_usgs_results:
        result = _get_monitoring_location_observed_properties(datasource_location, http_connection,
                                                              tsm_filters, synthesis_messages)

    assert result == expected_lookup
    assert mock_get_usgs_results.call_count == len(tsm_filters)

def test_get_monitoring_location_observed_properties_calls_get_usgs_results():
    datasource_location = 'foo'
    http_connection = object()
    synthesis_messages = []
    tsm_filters = ['parameter_code=TEMP', 'parameter_code=FLOW']

    with patch('basin3d.plugins.usgs._get_usgs_results',
               side_effect=[([], False), ([], False)]) as mock_get_usgs_results:
        result = _get_monitoring_location_observed_properties(datasource_location, http_connection,
                                                              tsm_filters, synthesis_messages)

    assert result == {}

    url_base = (
        'foo'
        '/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000'
        '&properties=id,parameter_code,monitoring_location_id'
        '&skipGeometry=true&offset=0'
        '&computation_period_identifier=Points%2CDaily&{}'
    )

    assert mock_get_usgs_results.call_args_list == [
        call(http_connection, url_base.format('parameter_code=TEMP'), synthesis_messages, request_page_limit=10, ignore_error_msg=True),
        call(http_connection, url_base.format('parameter_code=FLOW'), synthesis_messages, request_page_limit=10, ignore_error_msg=True)]


def test_get_monitoring_location_observed_properties_stops_on_pagination_error():
    datasource_location = 'foo'
    http_connection = object()
    synthesis_messages = []
    tsm_filters = ['parameter_code=TEMP', 'parameter_code=FLOW']

    with patch(
        'basin3d.plugins.usgs._get_usgs_results',
        side_effect=[([{'properties': {'monitoring_location_id': 'site-1', 'parameter_code': 'TEMP',}}], False),
                     ([], True)]) as mock_get_usgs_results:

        result = _get_monitoring_location_observed_properties(
            datasource_location, http_connection, tsm_filters, synthesis_messages)

    assert result == {}
    assert mock_get_usgs_results.call_count == 2
    assert len(synthesis_messages) == 1
    assert synthesis_messages[0] == ('A pagination error occurred while attempting to retrieve the timeseries information for foo/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&parameter_code=FLOW. '
                                     'Observed properties for the returned monitoring features will be missing. See instructions for how to increase the request page limit.')


# ================================
# TESTS for _get_unique_sites - with help from Chat GPT

@pytest.mark.parametrize(
    'site_responses,initial_unique_usgs_sites,expected_unique_usgs_sites',
    [
        pytest.param([{'id': 'site-1'}, {'id': 'site-1'}],
                     {},
                     {'site-1': {'id': 'site-1'}},
                     id='duplicate_monitoring_locations_are_deduplicated'),
        pytest.param([], {}, {},
                     id='empty_site_response_returns_empty_lookup'),
        pytest.param([{'id': 'site-1'}],
                     {'site-1': {'id': 'site-1', 'name': 'existing'}},
                     {'site-1': {'id': 'site-1', 'name': 'existing'}},
                     id='existing_monitoring_location_is_preserved'),
        pytest.param([{'id': 'site-2'}, {'id': 'site-3'}],
                     {'site-1': {'id': 'site-1'}},
                     {'site-1': {'id': 'site-1'}, 'site-2': {'id': 'site-2'}, 'site-3': {'id': 'site-3'}},
                     id='all_new_monitoring_locations_are_added'),
        pytest.param([{'id': 'site-1'}, {'id': 'site-2'}],
                     {'site-1': {'id': 'site-1', 'name': 'existing'}},
                     {'site-1': {'id': 'site-1', 'name': 'existing'}, 'site-2': {'id': 'site-2'}},
                     id='existing_and_new_monitoring_locations_are_merged'),
    ],
)
def test_get_unique_sites(site_responses, initial_unique_usgs_sites, expected_unique_usgs_sites):
    unique_usgs_sites = initial_unique_usgs_sites
    original_unique_usgs_sites = unique_usgs_sites

    _get_unique_sites(site_responses, unique_usgs_sites)

    assert unique_usgs_sites is original_unique_usgs_sites
    assert unique_usgs_sites == expected_unique_usgs_sites


@pytest.mark.parametrize(
    'data,data_str,parameter,units,expected_data,expected_units',
    [
        pytest.param(100.0, '100', '00060', 'ft^3/s', 2.8316847, 'm^3/s', id='parameter_00060_converts_discharge'),
        pytest.param(100.0, '100', '00061', 'ft^3/s', 2.8316847, 'm^3/s', id='parameter_00061_converts_discharge'),
        pytest.param(-999999.0, '-999999', '00060', 'ft^3/s', -999999, 'm^3/s', id='parameter_00060_preserves_missing_value_sentinel'),
        pytest.param(-999999.0, '-999999', '00061', 'ft^3/s', -999999, 'm^3/s', id='parameter_00061_preserves_missing_value_sentinel'),
        pytest.param(100.0, '100', '00065', 'ft', 100.0, 'ft', id='non_discharge_parameter_is_unchanged'),
        pytest.param(0.0, '0', '00060', 'ft^3/s', 0.0, 'm^3/s', id='zero_discharge_converts_to_zero'),
    ],
)
def test_convert_discharge(data, data_str, parameter, units, expected_data, expected_units):

    result_data, result_units = _convert_discharge(data, data_str, parameter, units)

    assert result_data == pytest.approx(expected_data)
    assert result_units == expected_units


def test_convert_discharge_missing_value_is_integer():
    result_data, result_units = _convert_discharge(-999999.0, '-999999', '00060', 'ft^3/s')

    assert result_data == -999999
    assert isinstance(result_data, int)
    assert result_units == 'm^3/s'
