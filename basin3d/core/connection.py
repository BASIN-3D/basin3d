"""

.. currentmodule:: basin3d.core.connection

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` connection classes
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""
import requests
import yaml
from basin3d.core import monitor

from basin3d.core.access import get_url, post_url

logger = monitor.get_logger(__name__)


class HTTPConnectionDataSource(object):
    """
    Class for handling Authentication and authorization of
    :class:`basin3d.models.DataSource` over HTTP


    :param datasource: the datasource to authenticate and authorize via HTTP
    :type datasource: :class:`basin3d.models.DataSource` instance
    """

    def __init__(self, datasource, *args, credentials=None, verify_ssl=False, **kwargs):
        self.datasource = datasource
        self.credentials = credentials
        self.verify_ssl = verify_ssl

    def login(self):
        """
        Login to the :class:`basin3d.models.DataSource`

        :return: JSON response
        :rtype: dict
        """
        raise NotImplementedError

    def logout(self):
        """
        Login out of the :class:`basin3d.models.DataSource`

        :return: None
        """
        raise NotImplementedError

    def get(self, url_part, params=None, headers=None):
        """
        The resources at the spedicfied url

        :param url_part:
        :param params:
        :param headers:
        :return:
        """
        raise NotImplementedError

    def post(self, url_part, params=None, headers=None):
        """
        The resources at the spedicfied url

        :param url_part:
        :param params:
        :param headers:
        :return:
        """
        raise NotImplementedError

    @classmethod
    def get_credentials_format(cls):
        """
        This returnes the format that the credentials are stored in the DB
        :return: The format for the credentials
        """
        raise NotImplementedError


class InvalidOrMissingCredentials(Exception):
    """The credentials are invalid or missing"""
    pass


class HTTPConnectionOAuth2(HTTPConnectionDataSource):
    """
    Class for handling Authentication and authorization of
    :class:`basin3d.models.DataSource` over HTTP with OAuth2


    :param datasource: the datasource to authenticate and authorize via HTTP
    :type datasource: :class:`basin3d.models.DataSource` instance
    :param auth_token_path: The url part for requesting a token
    :param revoke_token_path: The url part for revoking a valid token
    :param auth_scope: The scope of the token being requested (e.g read, write, group)
    :param grant_type: The type of oauth2 grant (e.g client_credentials, password,
            refresh_token, authorization_code)

    """

    CREDENTIALS_FORMAT = 'client_id:\nclient_secret:\n'

    def __init__(self, datasource, *args, auth_token_path="o/token/",
                 revoke_token_path="o/revoke_token/", auth_scope="read",
                 grant_type="client_credentials",
                 **kwargs):

        super(HTTPConnectionOAuth2, self).__init__(datasource, *args, **kwargs)
        self.token = None
        self.auth_token_path = auth_token_path
        self.revoke_token_path = revoke_token_path
        self.auth_scope = auth_scope
        self.grant_type = grant_type
        self.client_id, self.client_secret = self._load_credentials(datasource)

    def _validate_credentials(self):
        """
        Validate the Data Source credentials

        :return: TRUE if the credentials are valid
        :rtype: boolean
        """

        # There should be a client_id and client secret
        return "client_id" in self.credentials.keys() and "client_secret" in self.credentials.keys() \
               and self.credentials["client_id"] and self.credentials["client_secret"]

    def _load_credentials(self, datasource):
        """
        Get the credentials from Data Source. If the
        credentials are invalid `None` is returned.

        :param datasource: The datasource object
        :type datasource: :class:`basin3d.models.DataSource`
        :return: tuple of client_id and client_secret
        :rtype: tuple
        """

        self.credentials = datasource.credentials  # Access the credentials

        # If there are credentials then make the api call
        if self.credentials:
            self.credentials = yaml.load(self.credentials, Loader=yaml.FullLoader)
            if self._validate_credentials():
                return self.credentials["client_id"], self.credentials["client_secret"]

        raise InvalidOrMissingCredentials("client_id and client_secret are missing or invalid")

    def login(self):
        """
        Get a token

        OAuth Client credentials (client_id, client_secret) stored in the
        DataSource.

        - *Url:* `https://<datasource location>/<auth_token_path>`
        - *Scope:* <token_scope>
        - *Grant Type:* <grant_type>
        - *Client Id:* stored in encrypted :class:`basin3d.models.DataSource` field
        - *Client Secret:* stored in encrypted :class:`basin3d.models.DataSource` field

        Example JSON Response::

            {
                "access_token": "<your_access_token>",
                "token_type": "Bearer",
                "expires_in": 36000,
                "refresh_token": "<your_refresh_token>",
                "scope": "read"
            }


        """

        # Build the authentication url
        url = '{}{}'.format(self.datasource.location, self.auth_token_path)
        try:

            # Login to the Data Source
            res = requests.post(url, params={"scope": self.auth_scope, "grant_type": self.grant_type},
                                auth=(self.client_id, self.client_secret),
                                verify=self.verify_ssl)

            # Validate the response
            if res.status_code != requests.codes.ok:
                logger.error("Authentication  error {}: {}".format(url, res.content))
                return None

            # Get the JSON content (This has the token)
            result_json = res.json()
            self.token = result_json
        except Exception as e:
            logger.error("Authentication  error {}: {}".format(url, e))
            # TODO: create exception for this
            # Access is denied!!
            raise Exception("AccessDenied")

    def get(self, url_part, params=None, headers=None):
        """
        Login Data Source if not already logged in.
        Access url with the Authorization header and the access token

        Authorization Header:
            - Authorization": "{token_type} {access_token}

        :param url_part: The url part to request
        :param params: additional parameters for the request
        :type params: dict
        :param headers: request headers
        :return: None
        :raises: PermissionDenied
        """
        self._validate_token()

        # Prepare the Authorization header
        auth_headers = {"Authorization": "{token_type} {access_token}".format(**self.token)}
        if headers:
            auth_headers.update(headers)

        return get_url(url_part, params=params, headers=auth_headers, verify=self.verify_ssl)

    def post(self, url_part, params=None, headers=None):
        """
        Login Data Source if not already logged in.
        Access url with the Authorization header and the access token

        Authorization Header:
            - Authorization": "{token_type} {access_token}

        :param url_part: The url part to request
        :param params: additional parameters for the request
        :type params: dict
        :param headers: request headers
        :return: None
        :raises: PermissionDenied
        """

        self._validate_token()

        # Prepare the Authorization header
        auth_headers = {"Authorization": "{token_type} {access_token}".format(**self.token)}
        if headers:
            auth_headers.update(headers)

        return post_url(url_part, params=params, headers=auth_headers, verify=self.verify_ssl)

    def logout(self):
        """
        Revokes atoken

        :param token: The current Token
        :return: None
        """

        # Build the authentication url for revoking the token
        url = '{}{}'.format(self.datasource.location, self.revoke_token_path)

        # Request the token to be revoked
        if self.token:
            res = requests.post(url, params={"token": self.token["access_token"],
                                             "client_id": self.client_id},
                                auth=(self.client_id, self.client_secret),
                                verify=self.verify_ssl)

            # Validate the success of the token revocation
            if res.status_code != 200:
                logger.warning("Problem encountered revoking token for '{}' HTTP status {} -- {}".format(
                    self.datasource.name,
                    res.status_code, res.content.decode('utf-8')))

    def _validate_token(self):
        """
        Validate the connection token
        :return:
        """
        if not self.token:
            self.login()
        if not self.token:
            # TODO: create exception for this
            # Access is denied!!
            raise Exception("AccessDenied")


class HTTPConnectionTokenAuth(HTTPConnectionDataSource):
    """
    Class for handling Authentication and authorization of
    :class:`basin3d.models.DataSource` over HTTP with Tokens
    :param datasource: the datasource to authenticate and authorize via HTTP
    :type datasource: :class:`basin3d.models.DataSource` instance
    :param login_path: The url part for requesting a token
    """

    CREDENTIALS_FORMAT = 'username: \npassword: \n'

    def __init__(self, datasource, login_path='api/login'):
        """
        Initialize HTTPTokenAuth
        :param datasource:
        :param login_path:
        """

        super(HTTPConnectionTokenAuth, self).__init__(datasource)
        self.userpass = self._load_credentials()
        self.login_path = login_path

    def _load_credentials(self):
        """
        Load the credentials
        :return:
        """
        credentials = self.datasource.credentials  # Access the credentials
        self.credentials = yaml.load(credentials, Loader=yaml.FullLoader)
        # If there are credentials then get the monitoring features
        if self._validate_credentials(self.credentials):
            return self.credentials
        raise InvalidOrMissingCredentials(
            f'Invalid or Missing Credentials - Data Source: {self.datasource.name}')

    @staticmethod
    def _validate_credentials(credentials):
        """
        Validate the credentials
        :param credentials:
        :return:
        """
        return credentials and "username" in credentials.keys() and "password" in credentials.keys() \
            and credentials["username"] and credentials["password"]

    def login(self):
        """
        Get a Token
        :return: JSON response
        :rtype: dict
        """
        try:
            res = requests.get(f'{self.datasource.location}{self.login_path}',
                               params=self.userpass, verify=self.verify_ssl)
            result_json = res.json()
            if result_json[0] == 0:
                return result_json[1]['token']
            elif '{' in result_json[0]:
                return result_json
            else:
                raise InvalidOrMissingCredentials(
                    f'Datasource \'{self.datasource.name}\' Error ({self.login_path}): {result_json}')

        except InvalidOrMissingCredentials as imc:
            raise imc
        except Exception as e:
            raise InvalidOrMissingCredentials(
                f'Datasource \'{self.datasource.name}\' Error ({self.login_path}): {e}')

    def logout(self):
        pass

    def _submit_url(self, url_part, params=None, headers=None, request_method=get_url):
        """
        Interact with the datasource API
        :param url_part:
        :param params:
        :param headers:
        :param request_method:
        :return:
        """

        token_params = [('token', self.login())]
        if params:

            # loop through parameters
            for item in params.items():

                # separate lists into tuples
                if isinstance(item[1], list):
                    for v in item[1]:
                        token_params.append((item[0], v))
                else:
                    token_params.append(item)

        # Check if the url_part contains the datasource location
        url = url_part
        if not url_part.startswith(self.datasource.location):
            url = f'{self.datasource.location}{url_part}'

        return request_method(url, params=token_params,
                              headers=headers, verify=self.verify_ssl)

    def get(self, url_part, params=None, headers=None):
        """
        Get the url
        :param url_part: relative location of the requeste URL
        :param params: The  query parameters
        :type params: dict or list of 2-tuples
        :param headers:
        :return:
        """
        return self._submit_url(url_part, params, headers)

    def post(self, url_part, params=None, headers=None):
        """
        Post to the url
        :param url_part: relative location of the requeste URL
        :param params: The  query parameters
        :type params: dict or list of 2-tuples
        :param headers:
        :return:
        """
        return self._submit_url(url_part, params, headers, post_url)
