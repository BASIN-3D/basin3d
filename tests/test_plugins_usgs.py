import importlib
import json
import pytest

from pydantic import ValidationError
from unittest.mock import MagicMock, call
from typing import Iterator

from basin3d.core.connection import HTTPConnectionApiKey
from basin3d.core.models import Base
from basin3d.core.schema.enum import ResultQualityEnum, TimeFrequencyEnum, StatisticEnum
from basin3d.synthesis import register
from tests.utilities import get_json


def get_url(data, status=200):
    """
    Creates a get_url call for mocking with the specified return data
    :param data:
    :param status:
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
    :param status:
    :return:
    """

    return type('Dummy', (object,), {
        "text": text,
        "status_code": status,
        "url": "/testurl"})


@pytest.mark.parametrize('additional_query_params',
                         [({"monitoring_feature": ["USGS-09110990", "USGS-09111250"], "observed_property": []}),
                          ({"monitoring_feature": [(4, 2, 2)], "observed_property": ["RDC"]}),
                          ({"monitoring_feature": [(4, 2, 2, 3)], "observed_property": ["RDC"]}),
                          ({"observed_property": ["RDC"]})],
                         ids=['missing-variables', 'malformed_mf', 'malformed_bbox-mf', 'missing-monitoring_features'])
def test_measurement_timeseries_tvp_observations_usgs_errors(additional_query_params):
    """ Test USGS Timeseries data query"""

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


@pytest.mark.parametrize('additional_filters, usgs_response, usgs_api_calls, expected_results',
                         [
                          # ======== 1 all-good-mf-single-bbox
                          # tsm_bbox_day_1 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&bbox=-106.9,38.65,-106.8,38.67
                          # ml_mlid_1 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09110000,USGS-09112500
                          # daily_1a - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=84b54ac4c95041e5b03b39fbf728db10&time=2020-04-01T00%3A00%3A00Z%2F2020-04-10T23%3A59%3A59Z&approval_status=Approved
                          # daily_1b - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=c2c6677bf2f4432ca1150416ffc487a0&time=2020-04-01T00%3A00%3A00Z%2F2020-04-10T23%3A59%3A59Z&approval_status=Approved
                          ({"monitoring_feature": [(-106.9, 38.65, -106.8, 38.67)], "observed_property": ["RDC"], "result_quality": [ResultQualityEnum.VALIDATED], "start_date": "2020-04-01", "end_date": "2020-04-10"},
                           ["usgs_tsm_bbox_day_1.json", "usgs_ml_mlid_1.json", "usgs_daily_1a.json", "usgs_daily_1b.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&bbox=-106.9,38.65,-106.8,38.67',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09110000,USGS-09112500',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=84b54ac4c95041e5b03b39fbf728db10&time=2020-04-01T00%3A00%3A00Z%2F2020-04-10T23%3A59%3A59Z&approval_status=Approved',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=c2c6677bf2f4432ca1150416ffc487a0&time=2020-04-01T00%3A00%3A00Z%2F2020-04-10T23%3A59%3A59Z&approval_status=Approved'
                           ],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED, ResultQualityEnum.VALIDATED], "mvp_count": 2, "result_count": [10, 10], "missing_values_count": [0, 0]}),
                          # ======== 2 all-good-mf-multi-bbox
                          # tsm_bbox_day_1
                          # tsm_bbox_day_2 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&bbox=-106.7,38.85,-106.5,39.0
                          # ml_mlid_2 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09112500,USGS-09110000,USGS-09107000
                          # daily_1a
                          # daily_1b
                          # daily_2 - (changed approval status to Provisional) https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9db5335b910447e593a5e32dc92dd143&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z&approval_status=Approved
                          ({"monitoring_feature": [(-106.9, 38.65, -106.8, 38.67),  (-106.7, 38.85, -106.5, 39.0)], "observed_property": ["RDC"], "start_date": "2024-04-01", "end_date": "2024-04-10"},
                           ["usgs_tsm_bbox_day_1.json", "usgs_tsm_bbox_day_2.json", "usgs_ml_mlid_2.json", "usgs_daily_1a.json",  "usgs_daily_1b.json", "usgs_daily_2.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&bbox=-106.9,38.65,-106.8,38.67',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&bbox=-106.7,38.85,-106.5,39.0',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000,USGS-09110000,USGS-09112500',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=84b54ac4c95041e5b03b39fbf728db10&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=c2c6677bf2f4432ca1150416ffc487a0&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9db5335b910447e593a5e32dc92dd143&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z'
                           ],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED, ResultQualityEnum.VALIDATED, ResultQualityEnum.UNVALIDATED], "mvp_count": 3, "result_count": [10, 10, 10], "missing_values_count": [0, 0, 0]}),
                          # ======== 3 all-good-mf-full-overlap
                          # tsm_bbox_day_2
                          # tsm_ml_day_3 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09107000
                          # ml_mlid_3 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000
                          # daily_2
                          ({"monitoring_feature": [(-106.7, 38.85, -106.5, 39.0), "USGS-09107000"], "observed_property": ["RDC"], "start_date": "2024-04-01", "end_date": "2024-04-10"},
                           ["usgs_tsm_bbox_day_2.json", "usgs_tsm_ml_day_3.json", "usgs_ml_mlid_3.json", "usgs_daily_2.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&bbox=-106.7,38.85,-106.5,39.0',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9db5335b910447e593a5e32dc92dd143&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z'
                           ],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.UNVALIDATED], "mvp_count": 1, "result_count": [10], "missing_values_count": [0]}),
                          # ======== 4 all-good-mf-strings
                          # tsm_ml_day_4 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09110990,USGS-09111250
                          # usgs_ml_mlid_4 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09110990,USGS-09111250
                          # daily_1a -- faking it for ts id=9b2825ab44424a65a4d5f71a96c4e4ea
                          # daily_4 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=a05d6d5b0d2e4f49b88301de5ae6e60b&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z&approval_status=Approved
                          ({"monitoring_feature": ["USGS-09110990", "USGS-09111250"], "observed_property": ["RDC"], "result_quality": [ResultQualityEnum.VALIDATED], "start_date": "2024-04-01", "end_date": "2024-04-10"},
                           ["usgs_tsm_ml_day_4.json", "usgs_ml_mlid_4.json", "usgs_daily_1a.json", "usgs_daily_4.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09110990,USGS-09111250',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09110990,USGS-09111250',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9b2825ab44424a65a4d5f71a96c4e4ea&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z&approval_status=Approved',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=a05d6d5b0d2e4f49b88301de5ae6e60b&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z&approval_status=Approved'
                           ],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED, ResultQualityEnum.VALIDATED], "mvp_count": 2, "result_count": [10, 10], "missing_values_count": [0, 0]}),
                          # ======== 5 all-data-filtered-no-end-date
                          # tsm_ml_day_3 - faking it with the results from the bbox query
                          # ml_mlid_3
                          # ml_daily_empty - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9db5335b910447e593a5e32dc92dd143&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z&approval_status=Provisional
                          ({"monitoring_feature": ["USGS-09107000"], "observed_property": ["RDC"], "start_date": "2023-04-01"},
                           ["usgs_tsm_ml_day_3.json", "usgs_ml_mlid_3.json", "usgs_daily_empty.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9db5335b910447e593a5e32dc92dd143&time=2023-04-01T00%3A00%3A00Z%2F..'
                           ],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED], "mvp_count": 0, "result_count": [], "missing_values_count": [],
                            "synthesis_msgs": []}),
                          # ======== 6 two_properties_and_stat
                          # usgs_tsm_mlid_day_6_09107000 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09107000
                          # ml_mlid_6_09107000 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000
                          # daily_6a - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9db5335b910447e593a5e32dc92dd143&time=2025-05-01T00%3A00%3A00Z%2F2025-05-10T23%3A59%3A59Z
                          # daily_6b - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=db04af0efd04425e889df4b75056c85f&time=2025-05-01T00%3A00%3A00Z%2F2025-05-10T23%3A59%3A59Z
                          ({"monitoring_feature": ["USGS-09107000"], "observed_property": ["RDC", "WT"], "start_date": "2025-05-01", "end_date": "2025-05-10", "statistic": "MEAN"},
                            ["usgs_tsm_mlid_day_6_09107000.json", "usgs_ml_mlid_6_09107000.json", "usgs_daily_6a.json", "usgs_daily_6b.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9db5335b910447e593a5e32dc92dd143&time=2025-05-01T00%3A00%3A00Z%2F2025-05-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=db04af0efd04425e889df4b75056c85f&time=2025-05-01T00%3A00%3A00Z%2F2025-05-10T23%3A59%3A59Z'
                           ],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED, ResultQualityEnum.VALIDATED], "mvp_count": 2, "result_count": [10, 10], "missing_values_count": [0, 0]}),
                          # ======== 7 invalid monitoring feature
                          ({"monitoring_feature": ["USG-0910700"], "observed_property": ["RDC"], "start_date": "2023-04-01", "end_date": "2023-04-10"},
                            ['usgs_foo.json'],
                            [],
                            {"statistic": None, "result_quality": [], "mvp_count": 0, "result_count": [], "missing_values_count": [],
                             "synthesis_msgs": ["No monitoring features for USGS were specified or they were not specified with the USGS prefix."]}),
                          # ======== 8 no USGS monitoring feature
                          ({"monitoring_feature": ["EPA-0910700"], "observed_property": ["RDC"], "start_date": "2023-04-01", "end_date": "2023-04-10"},
                           ['usgs_foo.json'],
                           [],
                           {"statistic": None, "result_quality": [], "mvp_count": 0, "result_count": [], "missing_values_count": [],
                            "synthesis_msgs": ["No monitoring features for USGS were specified or they were not specified with the USGS prefix."]}),
                          # ======== 9 by_huc_stat_min - need new files
                          # tsm_huc_day_9 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&hydrologic_unit_code=140200
                          # ml_mlid_9 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000,USGS-09110000,USGS-09112200,USGS-09112500,USGS-09114500,USGS-09119000,USGS-09124500,USGS-09127000,USGS-09132095,USGS-09136100,USGS-09144250,USGS-09147500,USGS-09149500,USGS-09152500,USGS-383926107593001,USGS-385106106571000,USGS-385553107243301
                          # daily_9 (faking all 17 ts by repeating this one) - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=014e7726d74646ec91f79751487a30df&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z
                          ({"monitoring_feature": ["USGS-140200"], "observed_property": ["WT"], "start_date": "2024-07-01", "end_date": "2024-07-10", "statistic": "MIN"},
                           ['usgs_tsm_huc_day_9.json', 'usgs_ml_mlid_9.json'] + ['usgs_daily_9.json'] * 17,
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&hydrologic_unit_code=140200',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS'
                            '&id=USGS-09107000,USGS-09110000,USGS-09112200,USGS-09112500,USGS-09114500,USGS-09119000,USGS-09124500,USGS-09127000,USGS-09132095,USGS-09136100,USGS-09144250,USGS-09147500,USGS-09149500,USGS-09152500,USGS-383926107593001,USGS-385106106571000,USGS-385553107243301',
                            # daily calls
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=014e7726d74646ec91f79751487a30df&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=0d1e3d47d85f4705a69c6233ec83a018&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=17f6c52d54f24947aac0aad2a6bc6c8a&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=2ebf9a889df24edc90a6d5a1997bfd6f&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',  # this one is empty
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=32eb8813e8da4d239e2078694d4212ac&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=433997ce82394055b8042d0fd592879a&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=438bbd5df86a42c5afb8e7b9f45daaa7&time=2024-07-10T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=49517c2cf72e49759f4ce4985d1679a1&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=87214973aa904052bfa79d6c1ccc4084&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=8a76611cfade4ec58b4dd7d4b34a0a58&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=9f7719f4cfff4885b9fa7bd00e4e5ccc&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=a4d006f93269436fb9bf353ef935f2f7&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=b08417cb3cd74510abcc226d7f361c7a&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=d9838256e3bc455b9259a1019a543086&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=e4949a3154ef461d8c2cee21429363f8&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=ef6911fe6353499f9cc3800a898c2efa&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=f3a6c72936fb43b9bfca034603b120aa&time=2024-07-01T00%3A00%3A00Z%2F2024-07-10T23%3A59%3A59Z'
                           ],
                           {"statistic": StatisticEnum.MIN, "result_quality": [ResultQualityEnum.VALIDATED] * 17, "mvp_count": 17, "result_count": [10] * 17, "missing_values_count": [0] * 17,
                            "synthesis_msgs": []}),
                          # ======== 10 continuous_with_no_stat - can reuse 09107000 and get new continuous data
                          # tsm_ml_continuous_10 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Points&monitoring_location_id=USGS-09107000
                          # usgs_ml_mlid_3.json
                          # continuous_10 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=b87d52f245b24236be8b0e4c224de218&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z
                          ({"monitoring_feature": ["USGS-09107000"], "observed_property": ["RDC"], "start_date": "2024-04-01", "end_date": "2024-04-10", "aggregation_duration": "NONE"},
                           ["usgs_tsm_ml_continuous_10.json", "usgs_ml_mlid_3.json", "usgs_continuous_10.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Points&monitoring_location_id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=b87d52f245b24236be8b0e4c224de218&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z'
                           ],
                           {"statistic": StatisticEnum.INSTANT, "result_quality": [ResultQualityEnum.VALIDATED], "mvp_count": 1, "result_count": [948], "missing_values_count": [0],
                            "synthesis_msgs": []}),
                          # ======== 11 continuous_with_invalid_stat - same api calls and responses as above b/d stat is ignored.
                          # usgs_tsm_ml_continuous_10
                          # usgs_ml_mlid_3.json
                          # usgs_continuous_10
                          ({"monitoring_feature": ["USGS-09107000"], "observed_property": ["RDC"], "start_date": "2024-04-01", "end_date": "2024-04-10", "aggregation_duration": "NONE", "statistic": "MIN"},
                           ["usgs_tsm_ml_continuous_10.json", "usgs_ml_mlid_3.json", "usgs_continuous_10.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Points&monitoring_location_id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=b87d52f245b24236be8b0e4c224de218&time=2024-04-01T00%3A00%3A00Z%2F2024-04-10T23%3A59%3A59Z'
                           ],
                           {"statistic": StatisticEnum.INSTANT, "result_quality": [ResultQualityEnum.VALIDATED], "mvp_count": 1, "result_count": [948], "missing_values_count": [0],
                            "synthesis_msgs": ['USGS continuous data service only supports statistic INSTANT. The other statistics (e.g., MEAN, MIN, MAX) cannot be specified when aggregation_duration = NONE and will be ignored.']}),
                          # ======== 12 query_with_no_timeseries_found
                          # usgs_tsm_empty_12 - query does not really produce this result -- faking it.
                          ({"monitoring_feature": ["USGS-09107000"], "observed_property": ["RDC", "WT"], "start_date": "2025-05-01", "end_date": "2025-05-10"},
                           ["usgs_tsm_empty_12.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09107000',
                           ],
                           {"statistic": None, "result_quality": [], "mvp_count": 0, "result_count": [], "missing_values_count": [],
                           "synthesis_msgs": ['No timeseries could be found for the specified query arguments.']}),
                          # ======== 13 changing_units_in_ts_results
                          # tsm_ml_day_3
                          # ml_mlid_3
                          # daily_13 -- daily_6b with some units changed
                          ({"monitoring_feature": ["USGS-09107000"], "observed_property": ["WT"], "start_date": "2025-05-01", "end_date": "2025-05-10", "statistic": "MEAN"},
                           ["usgs_tsm_ml_day_3.json", "usgs_ml_mlid_3.json", "usgs_daily_13.json"],
                           [
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Daily&monitoring_location_id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09107000',
                            'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=50000&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=db04af0efd04425e889df4b75056c85f&time=2025-05-01T00%3A00%3A00Z%2F2025-05-10T23%3A59%3A59Z'
                           ],
                           {"statistic": StatisticEnum.MEAN, "result_quality": [ResultQualityEnum.VALIDATED], "mvp_count": 1, "result_count": [8], "missing_values_count": [0],
                            "synthesis_msgs": [
                                "Data value id c8eb32ff-64f2-49ec-be4e-8952283ec4ef has unit different than its time series metadata. Skipping",
                                "Data value id 3ebdb133-f1ec-4fad-952a-84303545f914 has unit different than its time series metadata. Skipping",
                            ]}),
                         ],
                         ids=['all-good-mf-single-bbox', 'all-good-mf-multi-bbox', 'all-good-mf-full-overlap',
                              'all-good-mf-strings', 'all-data-filtered-no-end-date', 'two_properties_and_stat', 'invalid_monitoring_feature',
                              'no_usgs_monitoring_feature', 'by_huc_stat_min', 'continuous_with_no_stat', 'continuous_with_invalid_stat',
                              'query_with_no_timeseries_found', 'changing_units_in_ts_results'])
def test_measurement_timeseries_tvp_observations_usgs(additional_filters, usgs_response, usgs_api_calls, expected_results, monkeypatch):
    """ Test USGS Timeseries data query"""

    resources = [get_url(get_json(usgs_res)) for usgs_res in usgs_response]

    mock_get_url = MagicMock(side_effect=list(resources))

    monkeypatch.setattr(HTTPConnectionApiKey, 'get', mock_get_url)
    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    query = {
        "aggregation_duration": TimeFrequencyEnum.DAY,
        **additional_filters
    }

    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    # loop through generator and serialized the object, get actual object and compare
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        mvp_count = 0
        for timeseries in measurement_timeseries_tvp_observations:
            data = json.loads(timeseries.to_json())
            assert data["statistic"]["attr_mapping"]["basin3d_vocab"] == expected_results.get("statistic")
            for result_quality in data["result_quality"]:
                assert result_quality["attr_mapping"]["basin3d_vocab"] == expected_results.get("result_quality")[mvp_count]
            result_count = 0
            missing_value_count = 0
            for result_value in data["result"]["value"]:
                result_count += 1
                if result_value[1] == -999999:
                    missing_value_count += 1
            assert result_count == expected_results.get("result_count")[mvp_count]
            assert missing_value_count == expected_results.get("missing_values_count")[mvp_count]
            mvp_count += 1
        assert mvp_count == expected_results.get("mvp_count")

        expected_msgs = expected_results.get('synthesis_msgs', [])
        actual_msgs = [message.msg for message in measurement_timeseries_tvp_observations.synthesis_response.messages]
        assert actual_msgs == expected_msgs
    else:
        pytest.fail("Returned object must be iterator")

    assert list(mock_get_url.call_args_list) == [call(url) for url in usgs_api_calls]


@pytest.mark.parametrize("query, feature_type, usgs_resource",
                         [
                          # USGS-13 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&id=13
                          ({"id": "USGS-13"}, "region", "usgs_huc_13.json"),
                          # USGS-0102 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&id=0102
                          ({"id": "USGS-0102"}, "subregion", "usgs_huc_0102.json"),
                          # USGS-011000 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&id=011000
                          ({"id": "USGS-011000"}, "basin", "usgs_huc_011000.json"),
                          # USGS-01020004 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&id=01020004
                          ({"id": "USGS-01020004"}, "subbasin", "usgs_huc_01020004.json"),
                          ({"id": "USGS-01020004", "feature_type": "subregion"}, None, "usgs_huc_01020004.json")
                         ],
ids=["region", "subregion", "basin", "subbasin", "invalid_feature_type"])
def test_usgs_monitoring_feature_id_huc(query, feature_type, usgs_resource, monkeypatch):
    """Test USGS search by region  """

    def mock_get_url(*args, **kwargs):
        return get_url(get_json(usgs_resource))

    monkeypatch.setattr(HTTPConnectionApiKey, 'get', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    if feature_type:
        assert monitoring_feature is not None
        assert isinstance(monitoring_feature, Base)
        assert monitoring_feature.id == query["id"]
        assert monitoring_feature.feature_type == feature_type.upper()

    else:
        assert monitoring_feature is None


@pytest.mark.parametrize("query, feature_type",
                         [
                          # point
                          # usgs_tsm_ml_09129600 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&monitoring_location_id=USGS-09129600
                          # usgs_ml_mlid_09129600 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09129600
                          ({"id": "USGS-09129600", "feature_type": "point"}, "point")],
                         ids=["point"])
def test_usgs_monitoring_feature_id_point(query, feature_type, monkeypatch):
    """Test USGS search by region  """

    mock_get_url = MagicMock(side_effect=list([get_url(get_json('usgs_tsm_ml_09129600.json')),
                                               get_url(get_json('usgs_ml_mlid_09129600.json'))]))

    monkeypatch.setattr(HTTPConnectionApiKey, 'get', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    response = synthesizer.monitoring_features(**query)
    monitoring_feature = response.data

    assert monitoring_feature is not None
    assert isinstance(monitoring_feature, Base)
    assert monitoring_feature.id == query["id"]
    assert monitoring_feature.feature_type == feature_type.upper()


@pytest.mark.parametrize("query, expected_count, usgs_resource, expected_msgs",
                         [
                          # region - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&hydrologic_unit_classification_code=R
                          # subregion - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&hydrologic_unit_classification_code=S
                          # basin - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&hydrologic_unit_classification_code=B
                          # subbasin - https://api.waterdata.usgs.gov/ogcapi/v0/collections/hydrologic-unit-codes/items?f=json&lang=en-US&limit=10000&properties=id,hydrologic_unit_name&skipGeometry=true&offset=0&hydrologic_unit_classification_code=U
                          ({"datasource": "USGS"}, 3131, ["usgs_huc_region.json", "usgs_huc_subregion.json", "usgs_huc_basin.json", "usgs_huc_subbasin.json"], []),  # datasource - all hucs
                          ({"monitoring_feature": ['USGS-13']}, 1, ["usgs_huc_13.json"], []),  # region by id
                          ({"feature_type": "region"}, 22, ["usgs_huc_region.json"], []),  # region
                          ({"feature_type": "subregion"}, 246, ["usgs_huc_subregion.json"], []),  #subregion
                          ({"feature_type": "basin"}, 407, ["usgs_huc_basin.json"], []),  # basin
                          ({"feature_type": "subbasin"}, 2456, ["usgs_huc_subbasin.json"], []),  # subbasin
                          # feature types not supported
                          ({"feature_type": "watershed"}, 0, [], []),
                          ({"feature_type": "subwatershed"}, 0, [], []),
                          ({"feature_type": "site"}, 0, [], []),
                          ({"feature_type": "plot"}, 0, [], []),
                          ({"feature_type": "vertical_path"}, 0, [], []),
                          ({"feature_type": "horizontal_path"}, 0, [], []),
                          ({"parent_feature": ['USGS-02']}, 108, ["usgs_huc_subregion.json", "usgs_huc_basin.json", "usgs_huc_subbasin.json"], []),  # should return all supported huc levels (subregion, basin, subbasin) in this region
                          ({"parent_feature": ['USGS-0202'], "feature_type": "subbasin"}, 8, ["usgs_huc_subbasin.json"], []),
                          ({"parent_feature": ['USGS-0202'], "feature_type": "region"}, 0, [],
                           ['Specified parent feature 0202 is not a huc that hierarchically above the specified feature type REGION. '
                            'It will not be considered in the query to USGS resources.',
                            'The specified parent features are not at least one step up in the huc hierarchy from the specified feature_type REGION']),
                          ({"parent_feature": ['USGS-0202'], "feature_type": "subregion"}, 0, [],
                           ['Specified parent feature 0202 is not a huc that hierarchically above the specified feature type SUBREGION. '
                            'It will not be considered in the query to USGS resources.',
                            'The specified parent features are not at least one step up in the huc hierarchy from the specified feature_type SUBREGION']),
                          ({"monitoring_feature": [(-106.7, 38.85, -106.5, 39.0)], "feature_type": "region"}, 0, [],
                           ['USGS plugin only supports bounding box queries for feature_type = points. '
                            'Huc identifiers that match the specified monitoring_feature_ids will be returned.',
                            'No named monitoring features were specified; only bounding boxes. '
                            'Named monitoring features are required if no feature type is specified.'
                            ]),
                         ],
                         ids=["all", "region_by_id", "region", "subregion", "basin", "subbasin", "watershed", "subwatershed",
                              "site", "plot", "vertical_path", "horizontal_path", "all_by_region", "subbasin_by_subregion",
                              "mismatched_parent_feature_type", "parent_feature_same_as_feature_type", "bbox_for_huc_invalid"])
def test_usgs_monitoring_features_hucs(query, expected_count, usgs_resource, expected_msgs, monkeypatch):
    """Test USGS non-point features"""

    resources = [get_url(get_json(usgs_res)) for usgs_res in usgs_resource]

    mock_get_url = MagicMock(side_effect=list(resources))
    monkeypatch.setattr(HTTPConnectionApiKey, 'get', mock_get_url)

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

    if expected_msgs:
        msgs = monitoring_features.synthesis_response.messages
        for idx, msg in enumerate(msgs):
            assert msg.msg == expected_msgs[idx]


@pytest.mark.parametrize("query, expected_count, usgs_resource",
                         [
                          # tsm_huc_02020004 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&hydrologic_unit_code=02020004
                          # ml_huc_02020004 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&hydrologic_unit_code=02020004
                          ({"parent_feature": ['USGS-02020004'], "feature_type": "point"}, 3944, ["usgs_tsm_huc_02020004.json", "usgs_ml_huc_02020004.json"])],
                         ids=["points_by_subbasin"])
def test_usgs_monitoring_features_points_by_parent(query, expected_count, usgs_resource, monkeypatch):
    """Test USGS points by subbasin"""

    resources = [get_url(get_json(usgs_res)) for usgs_res in usgs_resource]

    mock_get_url = MagicMock(side_effect=list(resources))
    monkeypatch.setattr(HTTPConnectionApiKey, 'get', mock_get_url)

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


@pytest.mark.parametrize("query, usgs_resource, expected_count, expected_site_set",
                         [
                          # a point_by_id
                          # usgs_tsm_ml_09129600.json
                          # usgs_ml_mlid_09129600.json
                          ({"monitoring_feature": ["USGS-09129600"], "feature_type": "point"},
                            ["usgs_tsm_ml_09129600.json", "usgs_ml_mlid_09129600.json"],
                            1, ("USGS-09129600", )),
                          # b single_bbox_one_site
                          # usgs_tsm_bbox_day_2.json
                          # usgs_ml_mlid_3.json
                          ({"monitoring_feature": [(-106.7, 38.85, -106.5, 39.0)], "feature_type": "point"},
                            ["usgs_tsm_bbox_day_2.json", "usgs_ml_mlid_3.json"],
                            1, ("USGS-09107000", )),
                          # c single_bbox_many_sites
                          # tsm_bbox_c - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-106.7,38.5,-106.5,39.9
                          # ml_bbox_c - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-106.7,38.5,-106.5,39.9
                          ({"monitoring_feature": [(-106.7, 38.5, -106.5, 39.9)], "feature_type": "point"},
                           ["usgs_tsm_bbox_c.json", "usgs_ml_bbox_c.json"],
                            173, None),
                          # d 2_bbox_overlap
                          # usgs_tsm_bbox_day_2.json
                          # usgs_tsm_bbox_d.json - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-106.7,38.5,-106.5,39.0
                          # usgs_ml_bbox_d1 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-106.7,38.85,-106.5,39.0
                          # usgs_ml_bbox_d2 - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-106.7,38.5,-106.5,39.0
                          ({"monitoring_feature": [(-106.7, 38.85, -106.5, 39.0), (-106.7, 38.5, -106.5, 39.0)], "feature_type": "point"},
                            ["usgs_tsm_bbox_day_2.json", "usgs_tsm_bbox_d.json", "usgs_ml_bbox_d1.json", "usgs_ml_bbox_d2.json"],
                            33, None),
                          # e mix
                          # usgs_tsm_bbox_day_2
                          # usgs_tsm_ml_09129600
                          # usgs_ml_mlid_09129600
                          # usgs_ml_bbox_f.json
                          ({"monitoring_feature": [(-106.7, 38.85, -106.5, 39.0), "USGS-09129600"], "feature_type": "point"},
                            ["usgs_tsm_bbox_day_2.json", "usgs_tsm_ml_09129600.json", "usgs_ml_mlid_09129600.json", "usgs_ml_bbox_f.json"],
                            5, ("USGS-09107000", "USGS-09106800", "USGS-385637106305601", "USGS-385637106362901", "USGS-09129600")),
                          # f mix_full_overlap
                          # usgs_tsm_bbox_day_2.json
                          # usgs_tsm_ml_day_3.json
                          # usgs_ml_mlid_3.json
                          # usgs_ml_bbox_f.json - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-106.7,38.85,-106.5,39.0
                          ({"monitoring_feature": [(-106.7, 38.85, -106.5, 39.0), "USGS-09107000"], "feature_type": "point"},
                            ["usgs_tsm_bbox_day_2.json", "usgs_tsm_ml_day_3.json", "usgs_ml_mlid_3.json", "usgs_ml_bbox_f.json"],
                            4, ("USGS-09107000", "USGS-09106800", "USGS-385637106305601", "USGS-385637106362901")),
                          # g mix_many_overlap
                          # tsm_ml_mlid_g - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&monitoring_location_id=USGS-09106800,USGS-09110000,USGS-09107000
                          # usgs_tsm_bbox_day_2.json [[https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-106.7,38.85,-106.5,39.0]]
                          # tsm_bbox_g - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-90.6,34.4,-90.5,34.6
                          # 1 new + 2 duplicate below: ml_mlid_g  - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&id=USGS-09106800,USGS-09110000,USGS-09107000
                          # 4: usgs_ml_bbox_f.json [[https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-106.7,38.85,-106.5,39.0]]
                          # 31: ml_bbox_g - https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-90.6,34.4,-90.5,34.6
                          ({"monitoring_feature": [(-106.7, 38.85, -106.5, 39.0), "USGS-09106800", "USGS-09110000", (-90.6, 34.4, -90.5, 34.6), "USGS-09107000"], "feature_type": "point"},
                            ["usgs_tsm_ml_mlid_g.json", "usgs_tsm_bbox_day_2.json", "usgs_tsm_bbox_g.json", "usgs_ml_mlid_g.json", "usgs_ml_bbox_f.json", "usgs_ml_bbox_g.json"],
                            36, ("USGS-09106800", "USGS-09107000", "USGS-09110000", "USGS-385637106305601", "USGS-385637106362901", "USGS-07047970",
                                "USGS-07287700", "USGS-342400090325501", "USGS-342402090344901", "USGS-342410090343201", "USGS-342430090340101",
                                "USGS-342500090323501", "USGS-342550090344001", "USGS-342827090324001", "USGS-342849090323501", "USGS-342900090323501",
                                "USGS-342915090315501", "USGS-342946090344701", "USGS-342957090344001", "USGS-342958090344701", "USGS-343007090322801",
                                "USGS-343047090301501", "USGS-343058090321201", "USGS-343104090352501", "USGS-343106090320501", "USGS-343110090352501",
                                "USGS-343116090353101", "USGS-343141090314801", "USGS-343152090310301", "USGS-343224090351101", "USGS-343239090301801",
                                "USGS-343308090296001", "USGS-343318090300301", "USGS-343331090303001", "USGS-343344090302001", "USGS-343508090310901")),
                          # h empty
                          # usgs_daily_empty.json
                          # usgs_daily_empty.json
                          ({"monitoring_feature": [(-106.71, 38.58, -106.7, 39.59)], "feature_type": "point"},
                            ["usgs_daily_empty.json", "usgs_daily_empty.json"],
                            0, None),
                          # j wrong-datasource
                          # none
                          ({"datasource": "FOO", "monitoring_feature": [(-106.7, 38.5, -106.5, 39.9)], "feature_type": "point"},
                            [],
                            0, None),
                         ],
                         ids=["point_by_id", "single_bbox_one_site", "single_bbox_many_sites", "2_bbox_overlap", "mix",
                              "mix_full_overlap", "mix_many_overlap", "empty", "wrong-datasource"])
def test_usgs_monitoring_features_general(query, usgs_resource, expected_count, expected_site_set, monkeypatch):
    """Test USGS point by id"""

    resources = [get_url(get_json(usgs_res)) for usgs_res in usgs_resource]

    mock_get_url = MagicMock(side_effect=list(resources))
    monkeypatch.setattr(HTTPConnectionApiKey, 'get', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    count = 0
    site_set = set()

    for mf in monitoring_features:
        count += 1
        site_set.add(mf.id)
        print(
            f"{mf.id} ({mf.feature_type}) {mf.description} {mf.coordinates and [(p.x, p.y) for p in mf.coordinates.absolute.horizontal_position]}")
        if 'feature_type' in query:
            assert mf.feature_type == query['feature_type'].upper()

    assert count == expected_count
    if expected_site_set:
        assert site_set == set(expected_site_set)


@pytest.mark.parametrize("query, expected_count", [({"feature_type": "point"}, 0),
                                                   ({"parent_feature": ['USGS-020200'], "feature_type": "region"}, 0)],
                         ids=["point", "invalid_parent_specification"])
def test_usgs_monitoring_features_invalid_query(query, expected_count):
    """Test USGS search by region  """

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    results = []
    results.extend(monitoring_features)
    assert len(results) == expected_count


@pytest.mark.parametrize("query, usgs_resource, expected_count, expected_msgs",
                         [
                          # ml short page: https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-90.6,34.4,-90.5,34.6
                          ({"monitoring_feature": [(-90.6, 34.4, -90.5, 34.6)],"feature_type": "point"},
                           ["usgs_tsm_bbox_g.json", "usgs_ml_bbox_g_short_page.json"], 0,
                           ("Pagination exceeded the default basin3d request limit: 1 for initial request https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?f=json&lang=en-US&limit=10000&properties=id,agency_code,monitoring_location_name,site_type,hydrologic_unit_code,altitude,altitude_accuracy,vertical_datum,time_zone_abbreviation,uses_daylight_savings,revision_note&skipGeometry=false&offset=0&agency_code=USGS&bbox=-90.6,34.4,-90.5,34.6. "
                            "No results will be returned. See instructions for increasing the limit.")),
                          # tsm short page: https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-90.6,34.4,-90.5,34.6
                          ({"monitoring_feature": [(-90.6, 34.4, -90.5, 34.6)], "feature_type": "point"},
                           ["usgs_tsm_bbox_g_short_page.json", "usgs_ml_bbox_g.json"], 31,
                           ("A pagination error occurred while attempting to retrieve the timeseries information for https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1000&properties=id,parameter_code,monitoring_location_id&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-90.6,34.4,-90.5,34.6. "
                            "Observed properties for the returned monitoring features will be missing. See instructions for how to increase the request page limit.")),
                         ],
                         ids=["ml_paged", "tsm_paged"])
def test_monitoring_feature_page_limit(query, usgs_resource, expected_msgs, expected_count, monkeypatch):
    monkeypatch.setenv('USGS_PAGE_REQUEST_LIMIT', '1')
    import basin3d.plugins.usgs as usgs
    importlib.reload(usgs)

    assert usgs.PAGE_REQUEST_LIMIT == 1

    resources = [get_url(get_json(usgs_res)) for usgs_res in usgs_resource]

    mock_get_url = MagicMock(side_effect=list(resources))
    monkeypatch.setattr(HTTPConnectionApiKey, 'get', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])
    monitoring_features = synthesizer.monitoring_features(**query)

    results = []
    results.extend(monitoring_features)

    assert len(results) == expected_count
    actual_msgs = [message.msg for message in monitoring_features.synthesis_response.messages]
    assert actual_msgs[0] == expected_msgs


@pytest.mark.parametrize("pagination_case, query, usgs_response, expected_count, expected_msgs",
                         [
                          # pagination-error-monitoring-locations
                          # tsm_bbox_z - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=10000&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-90.6,34.4,-90.5,34.6
                          ("monitoring-locations",
                           {"monitoring_feature": [(-90.6, 34.4, -90.5, 34.6)], "feature_type": "point", "observed_property": ["RDC"], "start_date": "1975-04-01", "end_date": "1975-04-10"},
                           ["usgs_tsm_bbox_z.json", "usgs_ml_bbox_g_short_page.json"], 0,
                           ["Pagination exceeded the default basin3d request limit: 1",
                            "No results will be returned."]),
                          # pagination-error-time-series-metadata
                          # tsm_bbox_z_short_page - https://api.waterdata.usgs.gov/ogcapi/v0/collections/time-series-metadata/items?f=json&lang=en-US&limit=1&properties=id,parameter_code,monitoring_location_id,statistic_id,begin,end,unit_of_measure&skipGeometry=true&offset=0&computation_period_identifier=Points%2CDaily&bbox=-90.6,34.4,-90.5,34.6
                          ("time-series-metadata",
                           {"monitoring_feature": [(-90.6, 34.4, -90.5, 34.6)], "feature_type": "point", "observed_property": ["RDC"], "start_date": "1975-04-01", "end_date": "1975-04-10"},
                           ["usgs_tsm_bbox_z_short_page.json"], 0,
                           ["Pagination exceeded the default basin3d request limit: 1",
                            "No results will be returned."]),
                          # pagination-error-daily
                          # daily_short_page (faking it here) - https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=2&properties=time,value,unit_of_measure,approval_status&skipGeometry=true&offset=0&agency_code=USGS&sortby=time&time_series_id=84b54ac4c95041e5b03b39fbf728db10&time=2020-04-01T00%3A00%3A00Z%2F2020-04-10T23%3A59%3A59Z&approval_status=Approved
                          ("daily",
                           {"monitoring_feature": [(-90.6, 34.4, -90.5, 34.6)], "feature_type": "point", "observed_property": ["RDC"], "start_date": "1975-04-01", "end_date": "1975-04-10"},
                           ["usgs_tsm_bbox_z.json", "usgs_ml_bbox_g.json", "usgs_daily_short_page.json"], 0,
                           ["has data collection frequency higher than 15-min."]),
                         ],
                         ids=["pagination-error-monitoring-locations", "pagination-error-time-series-metadata", "pagination-error-daily"])
def test_measurement_timeseries_tvp_observations_page_limit(
        pagination_case, query, usgs_response, expected_count, expected_msgs, monkeypatch):
    """Test measurement TVP observations when pagination reaches its limit."""

    monkeypatch.setenv("USGS_PAGE_REQUEST_LIMIT", "1")

    import basin3d.plugins.usgs as usgs
    importlib.reload(usgs)

    assert usgs.PAGE_REQUEST_LIMIT == 1

    resources = [get_url(get_json(response)) for response in usgs_response]

    mock_get_url = MagicMock(side_effect=list(resources))
    monkeypatch.setattr(HTTPConnectionApiKey, "get", mock_get_url)

    synthesizer = register(["basin3d.plugins.usgs.USGSDataSourcePlugin"])

    observations = synthesizer.measurement_timeseries_tvp_observations(**query)

    results = list(observations)

    assert len(results) == expected_count

    actual_msgs = [message.msg for message in observations.synthesis_response.messages]

    for expected_msg in expected_msgs:
        assert any(expected_msg in actual_msg for actual_msg in actual_msgs)

    if pagination_case == "daily":
        assert not any(
            "Pagination exceeded the default basin3d request limit" in actual_msg
            for actual_msg in actual_msgs
        )