# Changelog

## Version 1.1.0
Refactor core/catalog to use SQLite + SQLAlchemy, enhance EPA Plugin

+ Issue #191 - EPA plugin: Add support for WQP v3.0 web services
+ Issue #192 - EPA plugin: Add failover for GeoServer WFS service
+ Issue #193 - Refactor Metadata Catalog to Use SQLite and SQLAlchemy

## Version 1.0.3
Bug fix to correct EPA plugin for empty results.

+ Issue #189 - EPA plugin: empty results hangs on locations endpoint

## Version 1.0.2
Bug fix to correct EPA plugin for use in djanso-basin3d.

+ Issue #178 - EPA plugin causing incorrect MeasTVPObs return in django-basin3d

## Version 1.0.1
Minor update that correctly handles stream discharge (RDC) for the USGS data that are missing values. Note: it is not a full solution for supporting a common BASIN-3D missing value vocabulary.

+ Issue #158 - Support missing values

## Version 1.0.0
This release that adds documentation improvements and corrects unit issues in the BASIN-3D observed property vocabulary. 
This is a major release; however there are no breaking changes.

+ Issue #145 - update Acknowledgements
+ Issue #176 - units in basin3d observed property vocabulary are amiss
+ Issue #183 - Documentation updates

## Version 0.5.1
This release adds a data source plugin for datasets following the ESS-DIVE Community Hydrologic Monitoring Reporting Format. Details for configuring the dataset for synthesis are described in the online documentation.

## Version 0.5.0
This release adds a new data source plugin to the US EPA Water Quality eXchange, available via the National Water Quality Monitoring Council Water Quality Portal under the provider STORET. Documentation has also been updated to describe plugins in more detail.

## Version 0.4.3
Minor update to remove special handling of FeatureTypeEnum

+ Issue #172 - Change whitespace in enum to underscore

## Version 0.4.2
Update DataSourceModelAccess.retrieve method to use translation layer

+ Issue #168 - Updates to support django bug

## Version 0.4.1
Minor maintenance to handle dependency updates and minor fixes.

+ Issue #162 - Add badges to readme
+ Issue #163 - Pydantic v2 breaks things
+ Issue #165 - mypy issues in newer version
+ Pin Sphinx, ignore doi reference links, fix docs requirements installation instructions

## Version 0.4.0
This release adds a new major enhancement to datasource variable mapping by allowing
multiple datasource variables to be mapped to a single BASIN-3D variable.  Additionally,
the ability to define complex variable mappings where a single datasource variable may 
translate to multiple BASIN-3D attributes. Mapping to attributes other than variables, such as 
STATISTIC and RESULST_QUALITY, is now supported.

This release additionally prepares basin3d for publication in the Python Package Index (PyPI)
https://pypi.org/.

+ Issue #153 - Add Github Action to publish to PyPi
+ Issue #151 - Reformat sphinx docs for publication
+ Issue #149 - Modernize build system
+ Issue #148 - Update mypy configuration
+ Issue #143 - Enable complex mapping
+ Issue #93  - Enable mapping multiple datasource variables to same BASIN-3D variable

## Version 0.3.0
This release has core improvements such as logging, error handling and data quality flag support.
The USGS plugin was enhanced to return hi resolution data from the instantaneous values
service.  The data synthesis function for getting timeseries data was moved to a
new repository (https://github.com/BASIN-3D/basin3d-views).  Simplification make for
installing development dependencies.

  + Issue #25 - Implements infrastructure to handling error messages from Datasource
    plugins
  + Issue #83 - Separate USGS mock from integration tests.
  + Issue #90 - Logging improvements
  + Issue #95 - Add per data point quality flag
  + Issue #113 - Fixes documentation link in README.md
  + Issue #131 - Refactor get_timeseries data to support hi res
  + Issue #105 - Add USGS Instananeous Values to the USGS Plugin
  + Issue #140 - Use default_factory for OPV
  + Issue #122 - [Breaking Change] Separate out integration/view into a separate repo (hdf,panda,csv)


## Version 0.2.0
Project Updates:
   + Issue #94 - [Breaking Change] Add full statistic functionality
   + Issue #56 - Import ABC from collections.abc for Python 3.10 compatibility.
   + Issue #96 - [Breaking Change]  Query information should be added to the return

## Version 0.1.0
Project Updates:
   + Issue #14 - Enable USGS point monitoring features to be called by ID
   + Issue #16 - Enable USGS data acquisition by 2-digit HUC code (expand result quality type)
   + Issue #22 - Expand metadata functionality
   + Issue #49 - [Breaking change] Refactor synthesis.get_timeseries_data to add HDF output

Installation:
   + Issue #99 - Pin pyyaml for deprecation
   + Issue #98 - Correct package installation for development environment

## Version 0.0.3
Documentation for user facing documention for use with sphinx and
deployed in basin3d.readthedocs.org.
   + Issue #77, #71, #67, #33, #35, #36, #37, #39, #20, #60, #42, #52


Testing:
   + Issue #86 - Github Actions failing for mypy on specific types libraries 
   + Issue #46 -  Refactor tests to separate out integration and unit tests

## Version 0.0.2
Project Updates:   
   + Enable Github Actions
   + Temporarily freeze Pandas v1.1.5 due to NaN errors
   + Handle empty data returns
    
Install Updates:
   + Issue #29, #7: Fix install commands

Documentation Updates:
   + Issue #26: Fix typos and errors in README
   + Issue #27, 35: Fix basin3d reference and updates authors
   + Issue #28: Update git clone link in README
   + Issue #38: Add BASIN-3D concepts to documentation
   + Issue #41: Fix autogenerated Sphinx documentation
   + Remove prerequisites section in README
   + Update license to the BASIN-3D license

## Version 0.0.1
Project Updates:
   + Prepare version for open-source release
