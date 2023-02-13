"""

.. currentmodule:: basin3d.core.access

:platform: Unix, Mac
:synopsis: BASIN-3D ``DataSource`` access classes
:module author: Val Hendrix <vhendrix@lbl.gov>
:module author: Danielle Svehla Christianson <dschristianson@lbl.gov>

.. contents:: Contents
    :local:
    :backlinks: top

"""
from basin3d.core import monitor

logger = monitor.get_logger(__name__)


def get_url(url, params=None, headers=None, verify=True, **kwargs):
    """
    Send a GET request to the specified URL.  Note look up extra
    `kwargs` in `requests.get`
    :param url:
    :param params: request parameters
    :param headers: request headers
    :param verify: verify SSL connection
    :return: Response
    """
    import requests
    response = requests.get(url, params=params, verify=verify, headers=headers, **kwargs)
    logger.info("url:{}".format(response.url))
    return response


def post_url(url, params=None, headers=None, verify=True):
    """
    Send a POST request to the specified URL
    :param url:
    :param params: request parameters
    :param headers: request headers
    :param verify: verify SSL connection
    :return: Response
    """
    import requests
    response = requests.post(url, params=params, verify=verify, headers=headers)
    logger.info("url:{}".format(response.url))
    return response
