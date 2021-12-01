#!/usr/bin/env python

import os
import subprocess
import sys

from setuptools import setup, find_packages

# Get the Quickstart document
with open('README.md') as readme:
    README = readme.read()

# Update version from latest git tags.
# Create a version file in the root directory
version_py = os.path.join(os.path.dirname(__file__), 'basin3d/version.py')
try:
    git_describe = subprocess.check_output(["git", "describe", "--tags"]).rstrip().decode('utf-8')
    version_msg = "# Managed by setup.py via git tags.  **** DO NOT EDIT ****"
    with open(version_py, 'w') as f:
        f.write(version_msg + os.linesep + "__version__='" + git_describe.split("-")[0] + "'")
        f.write(os.linesep + "__release__='" + git_describe + "'" + os.linesep)

except Exception:
    # If there is an exception, this means that git is not available
    # We will used the existing version.py file
    pass

__release__ = "0"
if os.path.exists(version_py):
    with open(version_py) as f:
        code = compile(f.read(), version_py, 'exec')
    exec(code)

if sys.version_info.major == 2:
    sys.exit('Sorry, Python <= 2.X is not supported')

if sys.version_info.major == 3 and sys.version_info.minor < 7:
    sys.exit('Sorry, Python < 3.7 is not supported')

packages = find_packages(exclude=["*.tests", ])

# Get the requirements
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(name='basin3d',
      version=__release__,
      description='BASIN-3D Core Framework',
      long_description=README,
      author='Val Hendrix, Danielle Svehla Christianson, Charuleka Varadharajan, Catherine Wong',
      author_email='vchendrix@lbl.gov, dschristianson@lbl.gov, cvaradharajan@lbl.gov, catwong@lbl.gov' ,
      url='https://github.com/BASIN-3D/basin3d',
      packages=packages,
      data_files=['basin3d/data/basin3d_variables_hydrology.csv',
                  'basin3d/plugins/mapping_usgs.csv'],
      include_package_data=True,
      install_requires=required
      )
