from typing import Iterator

import datetime as dt
import os
import pandas as pd
import pytest

from basin3d.core.types import TimeFrequency, ResultQuality, SamplingMedium
from basin3d.synthesis import register, SynthesisException, get_timeseries_data


def test_register():
    """Test basic plugin registration"""

    from tests.testplugins import alpha
    from basin3d.plugins import usgs
    synthesizer = register()

    datasources = synthesizer.datasources
    assert len(datasources) == 2

    assert datasources[0].id_prefix == usgs.USGSDataSourcePlugin.get_id_prefix()
    assert datasources[0].id == 'USGS'
    assert datasources[0].location == 'https://waterservices.usgs.gov/nwis/'

    assert datasources[1].id_prefix == alpha.AlphaSourcePlugin.get_id_prefix()
    assert datasources[1].id == 'Alpha'
    assert datasources[1].location == 'https://asource.foo/'


@pytest.mark.parametrize("query", [{"pk": "A-123"}, {"pk": "A-123", "feature_type": "region"}],
                         ids=['not-found', 'too-many-for-pk'])
def test_monitoring_feature_not_found(query):
    """Test not found """

    synthesizer = register(['testplugins.alpha.AlphaSourcePlugin'])
    pytest.raises(Exception, synthesizer.monitoring_features, **query)


def test_monitoring_features_found():
    """Test  found """

    synthesizer = register(['testplugins.alpha.AlphaSourcePlugin'])
    monitoring_featurues = synthesizer.monitoring_features()
    if isinstance(monitoring_featurues, Iterator):
        count = 0
        for mf in monitoring_featurues:
            count += 1

        assert count == 2
    else:
        assert monitoring_featurues is not None


@pytest.mark.parametrize("query", [{"pk": "A-123"}, {"pk": "A-123", "feature_type": "region"}],
                         ids=['not-found', 'too-many-for-pk'])
def test_measurement_timeseries_tvp_observation_errors(query):
    """Test not found """

    synthesizer = register(['testplugins.alpha.AlphaSourcePlugin'])
    pytest.raises(Exception, synthesizer.measurement_timeseries_tvp_observations, **query)


def test_measurement_timeseries_tvp_observations_count():
    """Test  found """

    synthesizer = register(['testplugins.alpha.AlphaSourcePlugin'])
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_features=['test'], observed_property_variables=['test'], start_date='2016-02-01')
    if isinstance(measurement_timeseries_tvp_observations, Iterator):
        count = 0
        for mf in measurement_timeseries_tvp_observations:
            count += 1

        assert count == 3
    else:
        assert measurement_timeseries_tvp_observations is not None


@pytest.mark.parametrize("plugins, query, expected_count",
                         [(['basin3d.plugins.usgs.USGSDataSourcePlugin'], {}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'USGS'}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha'}, 4),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'FOO'}, 0)
                          ],
                         ids=['USGS-only', 'USGS-plus', 'Alpha-plus', 'Bad-DataSource'])
def test_observed_properties(plugins, query, expected_count):
    """Test observed properties search"""

    synthesizer = register(plugins)
    observed_properties = synthesizer.observed_properties(**query)

    # TODO are there other things to test?
    count = 0
    for op in observed_properties:
        print(op)
        count += 1

    assert count == expected_count


@pytest.mark.parametrize("plugins, query, expected_count",
                         [(['basin3d.plugins.usgs.USGSDataSourcePlugin'], {}, 168),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'USGS'}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha'}, 4),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'FOO'}, 0)
                          ],
                         ids=['USGS-only', 'USGS-plus', 'Alpha-plus', 'Bad-DataSource'])
def test_observed_property_variables(plugins, query, expected_count):
    """ Test observed property variables """

    synthesizer = register(plugins)
    observed_properties = synthesizer.observed_property_variables(**query)

    # TODO are there other things to test?
    count = 0
    for op in observed_properties:
        print(op)
        count += 1

    assert count == expected_count


def test_get_timeseries_data():
    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])

    # missing argument
    with pytest.raises(SynthesisException):
        get_timeseries_data(synthesizer=synthesizer,
                            monitoring_features=[], observed_property_variables=['ACT'], start_date='2019-01-01')

    # missing required parameter
    with pytest.raises(SynthesisException):
        get_timeseries_data(synthesizer=synthesizer,
                            observed_property_variables=['ACT'], start_date='2019-01-01')

    alpha_df, alpha_metadata = get_timeseries_data(
        synthesizer=synthesizer,
        monitoring_features=['A-1', 'A-2', 'A-3'], observed_property_variables=['ACT', 'Al'], start_date='2016-02-01')
    # check the dataframe
    assert isinstance(alpha_df, pd.DataFrame) is True
    assert list(alpha_df.columns) == ['TIMESTAMP', 'A-1__ACT', 'A-2__ACT']
    assert alpha_df.shape == (9, 3)
    alpha_data = alpha_df.get('A-1__ACT')
    assert list(alpha_data.values) == [num * 0.3454 for num in range(1, 10)]
    assert list(alpha_data.index) == [dt.datetime(2016, 2, num) for num in range(1, 10)]
    # make sure the temporary data directory is removed
    temp_dir = os.path.join(os.getcwd(), 'temp_data')
    assert os.path.isdir(temp_dir) is False
    # check the metadata store
    var_metadata = alpha_metadata.get('A-1__ACT')
    assert var_metadata['data_start'] == dt.datetime(2016, 2, 1)
    assert var_metadata['data_end'] == dt.datetime(2016, 2, 9)
    assert var_metadata['records'] == 9
    assert var_metadata['units'] == 'nm'
    assert var_metadata['basin_3d_variable'] == 'ACT'
    assert var_metadata['basin_3d_variable_full_name'] == 'Acetate (CH3COO)'
    assert var_metadata['statistic'] == 'MEAN'
    assert var_metadata['temporal_aggregation'] == TimeFrequency.DAY
    assert var_metadata['quality'] == ResultQuality.RESULT_QUALITY_CHECKED
    assert var_metadata['sampling_medium'] == SamplingMedium.WATER
    assert var_metadata['sampling_feature_id'] == 'A-1'
    assert var_metadata['datasource'] == 'Alpha'
    assert var_metadata['datasource_variable'] == 'Acetate'

    var_metadata = alpha_metadata.get('A-3__Al')
    assert var_metadata['data_start'] is None
    assert var_metadata['data_end'] is None
    assert var_metadata['records'] == 0
    assert var_metadata['units'] == 'mg/L'
    assert var_metadata['basin_3d_variable'] == 'Al'
    assert var_metadata['basin_3d_variable_full_name'] == 'Aluminum (Al)'
    assert var_metadata['statistic'] == 'MEAN'
    assert var_metadata['temporal_aggregation'] == TimeFrequency.DAY
    assert var_metadata['quality'] == ResultQuality.RESULT_QUALITY_CHECKED
    assert var_metadata['sampling_medium'] == SamplingMedium.WATER
    assert var_metadata['sampling_feature_id'] == 'A-3'
    assert var_metadata['datasource'] == 'Alpha'
    assert var_metadata['datasource_variable'] == 'Aluminum'
