"""

.. currentmodule:: basin3d.monitor

:synopsis: The BASIN-3D monitoring module
:module author: Val Hendrix <vhendrix@lbl.gov>

This module holds the logging functionality of BASIN-3D. It supports
logging the messages and their contexts

.. contents:: Contents
    :local:
    :backlinks: top

Functions
----------------
* :func:`get_logger` - Get the Basin3DLogger.
* :func:`configure` - Configure logging in BASIN-3D



"""
from basin3d.core.monitor import configure, get_logger

__all__ = ['configure', 'get_logger']