from unittest.mock import Mock, MagicMock

import pytest
import requests

import basin3d
from basin3d.core.access import post_url
from basin3d.core.connection import HTTPConnectionOAuth2, InvalidOrMissingCredentials, HTTPConnectionTokenAuth


@pytest.fixture
def mock_datasource():
    """Minimal DataSource object"""
    mock_datasource = MagicMock()
    mock_datasource.location = "http://foo.com/"
    return mock_datasource


@pytest.fixture
def mock_datasource_http_token(mock_datasource):
    """Mock DataSource object with http token credentials"""
    mock_datasource.credentials = b"username: FOO\npassword: oldisfjowe84uwosdijf"
    mock_datasource.location = "http://foo.com/"
    return mock_datasource


@pytest.fixture
def mock_datasource_http_oauth2(mock_datasource):
    """Mock DataSource object with http oauth2 credentials"""
    mock_datasource.credentials = b"client_id: FOO\nclient_secret: oldisfjowe84uwosdijf"
    mock_datasource.location = "http://foo.com"
    return mock_datasource


def test_http_oauth(monkeypatch):
    """Test the basic function of the oauth conection"""

    monkeypatch.setattr(HTTPConnectionOAuth2, '_load_credentials', lambda x, y: ("FOO", "oldisfjowe84uwosdijf"))
    conn = HTTPConnectionOAuth2(Mock())
    assert conn
    assert conn.client_id == "FOO"
    assert conn.client_secret == "oldisfjowe84uwosdijf"


def test_http_oauth_invalid_creds(monkeypatch, mock_datasource):
    """Test the basic function of the oauth connection with invalid credentials"""

    mock_datasource.credentials = b"client_id: FOO\n"
    pytest.raises(InvalidOrMissingCredentials, HTTPConnectionOAuth2, mock_datasource)


def test_http_oauth_datasource_credentials(monkeypatch, mock_datasource_http_oauth2):
    """Test the basic function of the oauth connection"""

    mock_datasource = mock_datasource_http_oauth2
    mock_get_url = Mock()
    mock_post_url = Mock()
    monkeypatch.setattr(basin3d.core.connection, 'get_url', mock_get_url)
    monkeypatch.setattr(basin3d.core.connection, 'post_url', mock_post_url)
    conn = HTTPConnectionOAuth2(mock_datasource)
    assert conn
    assert conn.client_id == "FOO"
    assert conn.client_secret == "oldisfjowe84uwosdijf"

    pytest.raises(Exception, conn.get, 'foo/')
    pytest.raises(Exception, conn.post, 'foo/')

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"token_type": "Bearer", "access_token": "9384u203iojdnskfn"}

    def mock_post(*args, **kwargs):
        return mock_response

    monkeypatch.setattr(requests, 'post', mock_post)

    conn.get('foo/', headers={'content-type': 'plain/text'})
    mock_get_url.assert_called()
    mock_get_url.assert_called_with('foo/',
                                    headers={'Authorization': 'Bearer 9384u203iojdnskfn', 'content-type': 'plain/text'},
                                    params=None,
                                    verify=False)

    conn.post('foo/', params={"foo": "bar"}, headers={'content-type': 'plain/text'})
    mock_post_url.assert_called()
    mock_post_url.assert_called_with('foo/', headers={'Authorization': 'Bearer 9384u203iojdnskfn',
                                                      'content-type': 'plain/text'},
                                     params={'foo': 'bar'}, verify=False)

    conn.logout()


def test_http_oauth_datasource_credentials_notok(monkeypatch, mock_datasource_http_oauth2):
    """Test the basic function of the oauth connection"""

    mock_datasource = mock_datasource_http_oauth2
    mock_get_url = Mock()
    mock_post_url = Mock()
    monkeypatch.setattr(basin3d.core.connection, 'get_url', mock_get_url)
    monkeypatch.setattr(basin3d.core.connection, 'post_url', mock_post_url)
    conn = HTTPConnectionOAuth2(mock_datasource)
    assert conn
    assert conn.client_id == "FOO"
    assert conn.client_secret == "oldisfjowe84uwosdijf"

    pytest.raises(Exception, conn.get, 'foo/')
    pytest.raises(Exception, conn.post, 'foo/')

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = None

    def mock_post(*args, **kwargs):
        return mock_response

    monkeypatch.setattr(requests, 'post', mock_post)

    pytest.raises(Exception, conn.get, 'foo/')
    mock_get_url.assert_not_called()


def test_http_token(monkeypatch):
    """Test the basic function of the oauth conection"""

    monkeypatch.setattr(HTTPConnectionOAuth2, '_load_credentials', lambda x, y: ("FOO", "oldisfjowe84uwosdijf"))
    conn = HTTPConnectionOAuth2(Mock())
    assert conn
    assert conn.client_id == "FOO"
    assert conn.client_secret == "oldisfjowe84uwosdijf"


def test_http_token_invalid_creds(monkeypatch, mock_datasource):
    """Test the basic function of the oauth connection with invalid credentials"""

    mock_datasource.credentials = b"client_id: FOO\n"
    mock_get_url = Mock()
    monkeypatch.setattr(basin3d.core.connection, 'get_url', mock_get_url)
    pytest.raises(InvalidOrMissingCredentials, HTTPConnectionTokenAuth, mock_datasource)


def test_http_token_datasource_credentials_ok(monkeypatch, mock_datasource_http_token):
    """Test the basic function of the token connection"""

    mock_datasource = mock_datasource_http_token
    mock_get_url = Mock()
    monkeypatch.setattr(basin3d.core.connection, 'get_url', mock_get_url)
    conn = HTTPConnectionTokenAuth(mock_datasource)
    assert conn
    assert conn.userpass == {'password': 'oldisfjowe84uwosdijf', 'username': 'FOO'}

    pytest.raises(InvalidOrMissingCredentials, conn.login)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [0, {"token": "90weujflkdshgureig"}]

    mock_submit_url = MagicMock(return_value=mock_response)

    monkeypatch.setattr(basin3d.core.connection.HTTPConnectionTokenAuth, '_submit_url', mock_submit_url)

    conn.get('foo/')
    mock_submit_url.assert_called()
    mock_submit_url.assert_called_with('foo/', None, None)

    conn.post('foo/', params={"foo": "bar"})
    mock_submit_url.assert_called()
    mock_submit_url.assert_called_with('foo/', {'foo': 'bar'}, None, post_url)


def test_http_token_datasource_credentials_notok(monkeypatch, mock_datasource_http_token):
    """Test the basic function of the token connection"""

    mock_datasource = mock_datasource_http_token
    mock_get_url = Mock()
    monkeypatch.setattr(basin3d.core.connection, 'get_url', mock_get_url)
    conn = HTTPConnectionTokenAuth(mock_datasource)
    assert conn
    assert conn.userpass == {'password': 'oldisfjowe84uwosdijf', 'username': 'FOO'}

    pytest.raises(Exception, conn.get, 'foo/')

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = None

    def mock_post(*args, **kwargs):
        return mock_response

    monkeypatch.setattr(requests, 'post', mock_post)

    pytest.raises(Exception, conn.get, 'foo/')
    mock_get_url.assert_not_called()


@pytest.mark.parametrize('response_json',
                         [[0, {"token": "90weujflkdshgureig"}], "{'token': '90weujflkdshgureig'}", None, "FOO"],
                         ids=["json_dict", 'json_str', "invalid_mission", "invalid_failed"])
def test_http_token_submit_url(monkeypatch, mock_datasource_http_token, response_json):
    """Test Submit url method"""
    mock_datasource = mock_datasource_http_token
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_json
    mock_get_url = Mock(return_value=mock_response)
    monkeypatch.setattr(basin3d.core.connection, 'get_url', mock_get_url)
    conn = HTTPConnectionTokenAuth(mock_datasource)
    assert conn
    assert conn.userpass == {'password': 'oldisfjowe84uwosdijf', 'username': 'FOO'}

    def mock_get(*args, **kwargs):
        return mock_response

    monkeypatch.setattr(requests, 'get', mock_get)

    if response_json is None or response_json == "FOO":
        pytest.raises(InvalidOrMissingCredentials, conn._submit_url, "foo/", {}, {}, mock_get_url)
    else:
        conn._submit_url("foo/", {"foo": "bat", "bar": [1, 2, 3, 4]}, {}, mock_get_url)
