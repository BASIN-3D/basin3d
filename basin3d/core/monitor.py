"""

.. currentmodule:: basin3d.core.monitor

:synopsis: The BASIN-3D monitoring module
:module author: Val Hendrix <vhendrix@lbl.gov>

This module holds the logging functionality of BASIN-3D. It supports
logging the messages and their contexts

.. contents:: Contents
    :local:
    :backlinks: top

"""
import contextvars
import logging
import os
import uuid
from functools import wraps
from logging import config
from typing import Any, Callable, Dict, List, Optional, Union

import yaml

LOGGER_NAME = __name__
BASE_PATH = os.path.dirname(__file__)
LOG_CONFIG_PATH = os.path.join(BASE_PATH, "logging.yaml")

#: A unique identifier for a single request context. Use this
#: If you want to identify log records from a single context
#: This is a Context variable which is natively supported in asyncio
#: and are ready to be used without any extra configuration.
synthesis_id: contextvars.ContextVar = contextvars.ContextVar('synthesis_id')

#: A unique identifier for a  basin3d context. Use this
#: If you want to identify log records from a single context
#: This is a Context variable which is natively supported in asyncio
#: and are ready to be used without any extra configuration.
basin3d_where: contextvars.ContextVar = contextvars.ContextVar('basin3d_where')


class Basin3dLogger(logging.Logger):
    """
    Custom logger for adding realtime context information (basin3d_where)
    """

    def info(self, msg, *args, **kwargs):
        kwargs = self._add_extra(**kwargs)
        super().info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        kwargs = self._add_extra(**kwargs)
        super().error(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        kwargs = self._add_extra(**kwargs)
        super().debug(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        kwargs = self._add_extra(**kwargs)
        super().warning(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        kwargs = self._add_extra(**kwargs)
        super().warning(msg, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        kwargs = self._add_extra(**kwargs)
        super().log(level, msg, *args, **kwargs)

    def _add_extra(self, **kwargs):
        """Add BASIN-3D extra, if exists """

        kwargs.setdefault('extra', dict())
        if "basin3d_where" not in kwargs['extra']:
            try:
                kwargs['extra']["basin3d_where"] = basin3d_where.get() or "*"
            except LookupError:
                kwargs['extra']["basin3d_where"] = "*"

        try:
            kwargs['extra']["synthesis_id"] = synthesis_id.get() or "*"
        except LookupError:
            kwargs['extra']["synthesis_id"] = "*"

        return kwargs


def get_logger(name: str = None) -> logging.Logger:
    """
    Get the basin3d logger for the specified name. Using this
    logger provides additional context for BASIN-3D synthesis.


    See :func:`logging.getLogger`

    :param: Name of the logger

    :return: The BASIN-3D Logger object
    """
    logging.setLoggerClass(Basin3dLogger)
    logger = logging.getLogger(name)

    return logger


def get_ctx_synthesis_id() -> Any:
    """
    Get the context for the monitoring synthesis_id
    :return:
    """
    try:
        return synthesis_id.get()
    except LookupError:
        pass

    return None


def set_ctx_synthesis_id() -> Any:
    """
    Set the context for the monitoring synthesis_id.
    :param synthesis_id: The request identifier
    """
    return synthesis_id.set(str(uuid.uuid1())[:8])


def get_ctx_basin3d_where() -> Any:
    """
    Get the context for the monitoring basin3d_where.
    """
    try:
        return basin3d_where.get()
    except LookupError:
        pass

    return None


def set_ctx_basin3d_where(where: Optional[Union[List, str]] = None) -> Union[Any, None]:
    """
    Set the context for the monitoring basin3d_where.
    :param where:
    """

    if isinstance(where, list):
        return basin3d_where.set(".".join(where))
    else:
        return basin3d_where.set(where)


def ctx_synthesis(func) -> Callable:
    """
    Decorator for setting synthesis id context

    :return: func
    """

    # Use of wraps makes sure that stack traces show the original
    # function name and not the wrapped one.
    @wraps(func)
    def func_wrapper(*args, **kwargs):

        s_token = set_ctx_synthesis_id()
        bw_token = set_ctx_basin3d_where()
        try:
            result = func(*args, **kwargs)
        finally:
            synthesis_id.reset(s_token)
            basin3d_where.reset(bw_token)
        return result
    return func_wrapper


def configure(log_config_path: str = None, **kwargs) -> Dict:
    """
    Load YAML python logging configuraiton file

    :param log_config_path: Path to the YAML file for configuring Python logging
    :returns: Logging configuration as dictionary

    **Keyword Args**
    Overwrite default logging config.

    + filters (dict)
    + formatters (dict)
    + handlers (dict)
    + loggers (dict)

    """
    # Load logging config
    logging.setLoggerClass(Basin3dLogger)
    log_config_path = log_config_path or LOG_CONFIG_PATH
    with open(f"{log_config_path}", "r") as f:
        # Expand any environment variables
        config_str = os.path.expandvars(f.read())

        # Load yaml yaml as dict
        config_file: Dict = yaml.load(config_str, Loader=yaml.Loader)
        config_file = _overwrite_config(config_file, config_overwrite=kwargs)
        config.dictConfig(config_file)

    return config_file


def _overwrite_config(config: Dict, config_overwrite: Dict) -> Dict:
    """
    Overwite the config with the specifed values
    :param config: The config to overwriete
    :param config_overwrite:
    :return:
    """

    if config_overwrite:
        # We are overwriting the config
        for key, value in config_overwrite.items():
            if key not in config.keys():
                config[key] = value
            else:
                if isinstance(value, dict):
                    _overwrite_config(config[key], config_overwrite[key])
                elif isinstance(value, (str, list)):
                    # Overwrite the configuration value
                    config[key] = value
                else:
                    raise Exception(f"Invalid config parameter {key}={value}. It must be a dict or string")

    return config