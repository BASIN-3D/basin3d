import datetime as dt
import json
import logging
import pytest

from geopandas import GeoDataFrame
from os.path import dirname
from unittest.mock import MagicMock

import basin3d.core.connection
from basin3d.synthesis import register
from basin3d.views import watershed_workflow as b3dww
from tests.utilities import get_text, get_json


def get_url_json(data, status=200):
    """
    Creates a get_url call for mocking with the specified return data
    :param data:
    :param status:
    :return:
    """
    return type('Dummy', (object,), {
        "content": data,
        "status_code": status,
        "url": "/testurl"})


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

# test USGS
def test_get_monitoring_features(monkeypatch):
    mock_get_url = MagicMock(side_effect=list(
        [get_url(get_json("usgs_tsm_bbox_day_2.json")),
         get_url(get_json("usgs_ml_mlid_3.json")),]))

    from basin3d.plugins import usgs
    monkeypatch.setattr(basin3d.core.connection.HTTPConnectionApiKey, 'get', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    monitoring_feature_geopandas = b3dww.get_monitoring_features(
        synthesizer, datasource=['USGS'], feature_type='point',
        monitoring_feature=[(-106.7, 38.85, -106.5, 39.0)])

    assert isinstance(monitoring_feature_geopandas, GeoDataFrame)
    assert list(monitoring_feature_geopandas.columns) == [
        'id', 'name', 'feature_type', 'description', 'data_source', 'elevation', 'geometry']
    assert monitoring_feature_geopandas.shape == (1,7)
    geometry = monitoring_feature_geopandas.get_geometry(0)
    assert float(geometry.x[0]) == -106.566696581035
    assert float(geometry.y[0]) == 38.8602712673025
    assert monitoring_feature_geopandas.get('id')[0] == 'USGS-09107000'
    assert monitoring_feature_geopandas.get('feature_type')[0] == 'POINT'
    assert monitoring_feature_geopandas.get('name')[0] == 'TAYLOR RIVER AT TAYLOR PARK, CO.'
    assert monitoring_feature_geopandas.get('data_source')[0].id == 'USGS'
    assert monitoring_feature_geopandas.get('elevation')[0] == 9332.33


# test empty result with message
def test_get_monitoring_features_msg(caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    caplog.clear()

    mock_get_url = MagicMock(side_effect=list(
        [get_url(get_json("usgs_daily_empty.json")), get_url(get_json("usgs_daily_empty.json"))]))

    from basin3d.plugins import usgs
    monkeypatch.setattr(basin3d.core.connection.HTTPConnectionApiKey, 'get', mock_get_url)

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    monitoring_feature_geopandas = b3dww.get_monitoring_features(
        synthesizer, datasource=['USGS'], feature_type='point',
        monitoring_feature=[(-90.6, 34.4, -90.5, 34.43)])

    log_msg = caplog.records[-1]
    assert log_msg.msg == "No monitoring features found for query parameters: {'datasource': ['USGS'], 'feature_type': 'point', 'monitoring_feature': [(-90.6, 34.4, -90.5, 34.43)]}"
    assert monitoring_feature_geopandas.size == 0
    assert list(monitoring_feature_geopandas.columns) == [
        'id', 'name', 'feature_type', 'description', 'data_source', 'elevation', 'geometry']


# test EPA -- only names supported
def test_get_monitoring_features_epa(caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    caplog.clear()

    mock_get_url = MagicMock(side_effect=list([get_url_json(get_text('epa_loc_single.json')), get_url_json(get_text('epa_mock.json'))]))

    from basin3d.plugins import epa
    monkeypatch.setattr(epa, 'get_url', mock_get_url)

    synthesizer = register(['basin3d.plugins.epa.EPADataSourcePlugin'])

    monitoring_feature_geopandas = b3dww.get_monitoring_features(
        synthesizer, datasource=['EPA'], feature_type='point',
        monitoring_feature=['EPA-0801417-CB-AS-1'])

    assert isinstance(monitoring_feature_geopandas, GeoDataFrame)
    assert list(monitoring_feature_geopandas.columns) == [
        'id', 'name', 'feature_type', 'description', 'data_source', 'elevation', 'geometry']
    assert monitoring_feature_geopandas.shape == (1, 7)
    geometry = monitoring_feature_geopandas.get_geometry(0)
    assert float(geometry.x[0]) == -107.25
    assert float(geometry.y[0]) == 37.95
    assert monitoring_feature_geopandas.get('id')[0] == 'EPA-0801417-CB-AS-1'
    assert monitoring_feature_geopandas.get('feature_type')[0] == 'POINT'
    assert monitoring_feature_geopandas.get('description')[0] == 'Location is part of USGS huc 14020002; organization Red Mountain Pass Zinc (US EPA Region 8); provider EPA STORET'
    assert monitoring_feature_geopandas.get('data_source')[0].id == 'EPA'
    assert monitoring_feature_geopandas.get('elevation')[0] is None

    monitoring_feature_geopandas = b3dww.get_monitoring_features(
        synthesizer, datasource=['EPA'], feature_type='point',
        monitoring_feature=[(-90.6, 34.4, -90.5, 34.43)])

    log_msg = caplog.records[-1]
    assert log_msg.msg == "No monitoring features found for query parameters: {'datasource': ['EPA'], 'feature_type': 'point', 'monitoring_feature': [(-90.6, 34.4, -90.5, 34.43)]}"
    assert monitoring_feature_geopandas.size == 0
    assert list(monitoring_feature_geopandas.columns) == [
        'id', 'name', 'feature_type', 'description', 'data_source', 'elevation', 'geometry']


# test malformed monitoring feature request
def test_malformed_monitoring_feature(caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    caplog.clear()

    synthesizer = register(['basin3d.plugins.usgs.USGSDataSourcePlugin'])

    monitoring_feature_geopandas = b3dww.get_monitoring_features(
        synthesizer, datasource=['USGS'], feature_type='point')

    assert isinstance(monitoring_feature_geopandas, GeoDataFrame)

    log_msg = caplog.records[-1]
    assert log_msg.msg == "No monitoring features found for query parameters: {'datasource': ['USGS'], 'feature_type': 'point'}"
    assert monitoring_feature_geopandas.size == 0
    assert list(monitoring_feature_geopandas.columns) == [
        'id', 'name', 'feature_type', 'description', 'data_source', 'elevation', 'geometry']
