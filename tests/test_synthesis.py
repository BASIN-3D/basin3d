import datetime
import os
import shutil

import pandas as pd
import pytest
from pydantic import ValidationError

from basin3d.core.schema.enum import ResultQualityEnum, TimeFrequencyEnum
from basin3d.core.schema.query import QueryMeasurementTimeseriesTVP, QueryMonitoringFeature
from basin3d.core.synthesis import DataSourceModelIterator
from basin3d.core.types import SamplingMedium
from basin3d.synthesis import TimeseriesOutputType, get_timeseries_data, register, PandasTimeseriesData, \
    HDFTimeseriesData


def test_register():
    """Test basic plugin registration"""

    synthesizer = register(["basin3d.plugins.usgs.USGSDataSourcePlugin",
                            "tests.testplugins.alpha.AlphaSourcePlugin"])

    datasources = synthesizer.datasources
    assert len(datasources) == 2

    from basin3d.plugins import usgs
    from tests.testplugins import alpha
    assert datasources[0].id_prefix == usgs.USGSDataSourcePlugin.get_id_prefix()
    assert datasources[0].id == 'USGS'
    assert datasources[0].location == 'https://waterservices.usgs.gov/nwis/'

    assert datasources[1].id_prefix == alpha.AlphaSourcePlugin.get_id_prefix()
    assert datasources[1].id == 'Alpha'
    assert datasources[1].location == 'https://asource.foo/'


@pytest.mark.parametrize("query", [{"id": "A-123"}, {"id": "A-123", "feature_type": "region"}],
                         ids=['not-found', 'too-many-for-id'])
def test_monitoring_feature_not_found(query):
    """Test not found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    pytest.raises(Exception, synthesizer.monitoring_features, **query)


def test_monitoring_features_found():
    """Test  found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    monitoring_featurues = synthesizer.monitoring_features()
    if isinstance(monitoring_featurues, DataSourceModelIterator):
        count = 0
        assert monitoring_featurues.synthesis_response is not None
        assert monitoring_featurues.synthesis_response.dict() == {'data': None,
                                                                  'messages': [],
                                                                  'query': {'datasource': None,
                                                                            'feature_type': None,
                                                                            'monitoring_features': None,
                                                                            'parent_features': None}}
        assert isinstance(monitoring_featurues.synthesis_response.query, QueryMonitoringFeature)

        for mf in monitoring_featurues:
            count += 1

        assert monitoring_featurues.synthesis_response is not None
        assert monitoring_featurues.synthesis_response.dict() == {'data': None,
                                                                  'messages': [{'level': 'WARN',
                                                                                'msg': 'message1',
                                                                                'where': ['Alpha',
                                                                                          'MonitoringFeature']},
                                                                               {'level': 'WARN',
                                                                                'msg': 'message2',
                                                                                'where': ['Alpha',
                                                                                          'MonitoringFeature']},
                                                                               {'level': 'WARN',
                                                                                'msg': 'message3',
                                                                                'where': ['Alpha',
                                                                                          'MonitoringFeature']}],
                                                                  'query': {'datasource': None,
                                                                            'feature_type': None,
                                                                            'monitoring_features': None,
                                                                            'parent_features': None}}

        assert count == 2
    else:
        assert monitoring_featurues is not None


@pytest.mark.parametrize("query", [{"id": "A-123"}, {"id": "A-123", "feature_type": "region"}],
                         ids=['not-found', 'too-many-for-id'])
def test_measurement_timeseries_tvp_observation_errors(query):
    """Test not found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    pytest.raises(Exception, synthesizer.measurement_timeseries_tvp_observations, **query)


def test_measurement_timeseries_tvp_observations_count():
    """Test  found """

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])
    measurement_timeseries_tvp_observations = synthesizer.measurement_timeseries_tvp_observations(
        monitoring_features=['test'], observed_property_variables=['test'], start_date='2016-02-01')
    if isinstance(measurement_timeseries_tvp_observations, DataSourceModelIterator):
        count = 0
        assert measurement_timeseries_tvp_observations.synthesis_response is not None
        assert measurement_timeseries_tvp_observations.synthesis_response.query is not None
        assert isinstance(measurement_timeseries_tvp_observations.synthesis_response.query,
                          QueryMeasurementTimeseriesTVP)
        assert measurement_timeseries_tvp_observations.synthesis_response.query.monitoring_features == ['test']
        assert measurement_timeseries_tvp_observations.synthesis_response.query.observed_property_variables == ['test']
        assert measurement_timeseries_tvp_observations.synthesis_response.query.start_date == datetime.date(2016, 2, 1)

        for mf in measurement_timeseries_tvp_observations:
            count += 1

        assert count == 4
    else:
        assert measurement_timeseries_tvp_observations is not None


@pytest.mark.parametrize("plugins, query, expected_count",
                         [(['basin3d.plugins.usgs.USGSDataSourcePlugin'], {}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'USGS'}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha'}, 4),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'FOO'}, 0)
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
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'USGS'}, 43),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'Alpha'}, 4),
                          (['basin3d.plugins.usgs.USGSDataSourcePlugin',
                            'tests.testplugins.alpha.AlphaSourcePlugin'], {"datasource_id": 'FOO'}, 0)
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


def test_get_timeseries_data_errors():
    """Test for error conditions in get timeseries data call"""
    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])

    # missing argument
    with pytest.raises(ValidationError):
        get_timeseries_data(synthesizer=synthesizer,
                            monitoring_features=[], observed_property_variables=['ACT'], start_date='2019-01-01')

    # missing required parameter
    with pytest.raises(ValidationError):
        get_timeseries_data(synthesizer=synthesizer,
                            observed_property_variables=['ACT'], start_date='2019-01-01')

    # output directory doesn't exist
    with pytest.raises(ValidationError):
        get_timeseries_data(synthesizer=synthesizer, output_path='./foo',
                            observed_property_variables=['ACT'], start_date='2019-01-01')


@pytest.mark.parametrize('output_type, output_path, cleanup',
                         [(TimeseriesOutputType.PANDAS, None, True),
                          (TimeseriesOutputType.HDF, None, True),
                          (TimeseriesOutputType.PANDAS, './pandas', True),
                          (TimeseriesOutputType.HDF, './hdf', True),
                          (TimeseriesOutputType.PANDAS, None, False),
                          (TimeseriesOutputType.HDF, None, False),
                          (TimeseriesOutputType.PANDAS, './pandas', False),
                          (TimeseriesOutputType.HDF, './hdf', False)],
                         ids=['pandas-cleanup', 'hdf-cleanup', 'pandas-output-cleanup',
                              'hdf-output-cleanup', 'pandas', 'hdf', 'pandas-output',
                              'hdf-output'])
def test_get_timeseries_data(output_type, output_path, cleanup):
    """Test processing for get_timeseries_data basic functionality"""

    # Create temporary directory
    if output_path:
        os.mkdir(output_path)
    try:
        synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])

        alpha_result = get_timeseries_data(synthesizer=synthesizer, output_path=output_path, output_type=output_type,
                                           cleanup=cleanup,
                                           monitoring_features=['A-1', 'A-2', 'A-3', 'A-4'],
                                           observed_property_variables=['ACT', 'Al'], start_date='2016-02-01')

        assert alpha_result
        assert output_type is TimeseriesOutputType.PANDAS and isinstance(alpha_result, PandasTimeseriesData) or \
               output_type is TimeseriesOutputType.HDF and isinstance(alpha_result, HDFTimeseriesData)

        alpha_df = alpha_result.data
        alpha_metadata_df = alpha_result.metadata
        alpha_metadata_nodata_df = alpha_result.metadata_no_observations

        if isinstance(alpha_result, PandasTimeseriesData):
            assert isinstance(alpha_df, pd.DataFrame) is True

        else:
            assert isinstance(alpha_df, object) is True

            # Since this is an HDF file, the output directory is not remove
            assert alpha_result.output_path is not None

            assert alpha_result.hdf.attrs['aggregation_duration'] == 'DAY'
            assert list(alpha_result.hdf.attrs['monitoring_features']) == ['A-1', 'A-2', 'A-3', 'A-4']
            assert list(alpha_result.hdf.attrs['observed_property_variables']) == ['ACT', 'Al']
            assert alpha_result.hdf.attrs['query_start_time']
            assert alpha_result.hdf.attrs['query_end_time']
            assert alpha_result.hdf.attrs['start_date']
            assert list(alpha_result.hdf.attrs['variables_data']) == ['A-1__ACT__MEAN', 'A-2__ACT__MAX', 'A-4__Al__MAX']
            assert list(alpha_result.hdf.attrs['variables_nodata']) == ['A-3__Al__MEAN']

        # Check the output path
        if not cleanup:
            # No files should have been cleaned up
            assert alpha_result.output_path and os.path.exists(alpha_result.output_path)
        elif isinstance(alpha_result, PandasTimeseriesData):
            # if cleanup is true and the output is pandas,
            #  there should not be an output path
            assert alpha_result.output_path is None
        elif output_path:
            # If there is an output path, it should exist
            assert alpha_result.output_path and os.path.exists(alpha_result.output_path)

        # check the dataframe
        assert list(alpha_df.columns) == ['TIMESTAMP', 'A-1__ACT__MEAN', 'A-2__ACT__MAX', 'A-4__Al__MAX']
        assert alpha_df.shape == (9, 4)
        alpha_data = alpha_df.get('A-1__ACT__MEAN')
        assert list(alpha_data.values) == [num * 0.3454 for num in range(1, 10)]
        assert list(alpha_data.index) == [datetime.datetime(2016, 2, num) for num in range(1, 10)]

        # check the metadata with observations
        # Get synthesized variable field names and values
        var_metadata = alpha_metadata_df['A-1__ACT__MEAN']
        assert var_metadata['data_start'] == datetime.datetime(2016, 2, 1)
        assert var_metadata['data_end'] == datetime.datetime(2016, 2, 9)
        assert var_metadata['records'] == 9
        assert var_metadata['units'] == 'nm'
        assert var_metadata['basin_3d_variable'] == 'ACT'
        assert var_metadata['basin_3d_variable_full_name'] == 'Acetate (CH3COO)'
        assert var_metadata['statistic'] == 'MEAN'
        assert var_metadata['temporal_aggregation'] == TimeFrequencyEnum.DAY
        assert var_metadata['quality'] == ResultQualityEnum.CHECKED
        assert var_metadata['sampling_medium'] == SamplingMedium.WATER
        assert var_metadata['sampling_feature_id'] == 'A-1'
        assert var_metadata['datasource'] == 'Alpha'
        assert var_metadata['datasource_variable'] == 'Acetate'

        assert 'A-3__Al__MEAN' in alpha_result.variables_no_observations
        assert 'A-3__Al__MEAN' not in alpha_result.variables

        assert list(alpha_metadata_df.columns) == ['TIMESTAMP', 'A-1__ACT__MEAN', 'A-2__ACT__MAX', 'A-4__Al__MAX']
        assert len(alpha_metadata_df) == 20

        # check the metadata with no observations
        # Get synthesized variable field names and values
        var_metadata = alpha_metadata_nodata_df['A-3__Al__MEAN']
        assert var_metadata['data_start'] is None
        assert var_metadata['data_end'] is None
        assert var_metadata['records'] == 0
        assert var_metadata['units'] == 'mg/L'
        assert var_metadata['basin_3d_variable'] == 'Al'
        assert var_metadata['basin_3d_variable_full_name'] == 'Aluminum (Al)'
        assert var_metadata['statistic'] == 'MEAN'
        assert var_metadata['temporal_aggregation'] == TimeFrequencyEnum.DAY
        assert var_metadata['quality'] == ResultQualityEnum.CHECKED
        assert var_metadata['sampling_medium'] == SamplingMedium.WATER
        assert var_metadata['sampling_feature_id'] == 'A-3'
        assert var_metadata['datasource'] == 'Alpha'
        assert var_metadata['datasource_variable'] == 'Aluminum'

        assert list(alpha_metadata_nodata_df.columns) == ['TIMESTAMP', 'A-3__Al__MEAN']
        assert len(alpha_metadata_nodata_df) == 20
    finally:
        # remove temporary directory
        if output_path and os.path.exists(output_path):
            shutil.rmtree(output_path)


@pytest.mark.parametrize('filters, expected_results',
                         [({'monitoring_features': ['A-1', 'A-2'], 'statistic': None},
                           {'has_data': True, 'columns': ['TIMESTAMP', 'A-1__ACT__MEAN', 'A-2__ACT__MAX'],
                            'df_shape': (9, 3),
                            'no_observations_variable': None}),
                          ({'monitoring_features': ['A-1', 'A-2', 'A-3', 'A-4'], 'statistic': ['MEAN']},
                           {'has_data': True, 'columns': ['TIMESTAMP', 'A-1__ACT__MEAN'], 'df_shape': (9, 2),
                            'no_observations_variable': 'A-3__Al__MEAN'}),
                          ({'monitoring_features': ['A-3', 'A-4'], 'statistic': ['MIN']},
                           {'has_data': False, 'columns': None, 'df_shape': None,
                            'no_observations_variable': None})],
                         ids=['monitoring_features', 'statistic', 'monitoring_feature_and_statistic'])
def test_get_timeseries_data_filtering(filters, expected_results):
    """Test processing for get_timeseries_data statistic"""

    synthesizer = register(['tests.testplugins.alpha.AlphaSourcePlugin'])

    alpha_result = get_timeseries_data(synthesizer=synthesizer, output_path=None,
                                       output_type=TimeseriesOutputType.PANDAS, cleanup=True,
                                       monitoring_features=filters['monitoring_features'],
                                       observed_property_variables=['ACT', 'Al'], start_date='2016-02-01',
                                       statistic=filters['statistic'])
    assert alpha_result
    alpha_df = alpha_result.data
    if expected_results['has_data']:
        assert isinstance(alpha_df, pd.DataFrame)
        assert list(alpha_df.columns) == expected_results['columns']
        assert alpha_df.shape == expected_results['df_shape']
    else:
        assert alpha_df is None

    no_observation_variable = expected_results['no_observations_variable']
    if no_observation_variable:
        assert no_observation_variable in alpha_result.variables_no_observations
        assert no_observation_variable not in alpha_result.variables
    else:
        assert len(alpha_result.variables_no_observations) == 0
