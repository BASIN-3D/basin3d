[![Python package](https://github.com/BASIN-3D/basin3d/actions/workflows/main.yml/badge.svg)](https://github.com/BASIN-3D/basin3d/actions/workflows/main.yml)
[![Pypi](https://img.shields.io/pypi/v/basin3d)](https://pypi.org/project/basin3d/)

# basin3d
Broker for Assimilation, Synthesis and Integration of eNvironmental Diverse, Distributed Datasets. 

![basin3d](https://user-images.githubusercontent.com/20212666/112556236-ff1a9b80-8d86-11eb-9009-25b658ce41e0.png)

BASIN-3D is a software ecosystem that synthesizes diverse earth science data from a variety of remote data sources on-demand, without the need for storing data in a single database. It is designed to parse, translate, and synthesize diverse observations from well-curated repositories into standardized formats for scientific uses such as analysis and visualization.

basin3d is the core BASIN-3D application that uses a generalized data synthesis model that applies across a variety of earth science observation types (hydrology, geochemistry, climate etc.). 

basin3d has available plugins that can connect to specific data sources of interest, and map the data source vocabularies to the basin3d synthesis models.



## Getting Started

### Install

Install a source distribution with pip:

    $ pip install basin3d
    
Make sure your installation was successful:

    $ python
    >>> import basin3d
    >>>

## Documentation

See latest basin3d documentation [here](https://basin3d.readthedocs.io/en/latest/)


## Contributing

If you’re interested in contributing to basin3d, check out out our [contributing guidelines](CONTRIBUTING.md). It will help explain why, what, and how to get started.


## Changelog
See the [changelog](https://basin3d.readthedocs.io/en/stable/changelog.html) for a history of updates and changes to basin3d

## Authors

* **Charuleka Varadharajan** - [LBL](https://eesa.lbl.gov/profiles/charuleka-varadharajan/)
* **Valerie Hendrix**  - [LBL](https://crd.lbl.gov/departments/data-science-and-technology/uss/staff/valerie-hendrix)
* **Danielle Svehla Christianson** - [LBL](https://crd.lbl.gov/departments/data-science-and-technology/uss/staff/danielle-christianson/)
* **Catherine Wong**  - [LBL](https://crd.lbl.gov/departments/data-science-and-technology/uss)


## Copyright

Broker for Assimilation, Synthesis and Integration of eNvironmental Diverse, Distributed Datasets (BASIN-3D) Copyright (c) 2019, The
Regents of the University of California, through Lawrence Berkeley National
Laboratory (subject to receipt of any required approvals from the U.S.
Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative
works, and perform publicly and display publicly, and to permit other to do
so.

## License

See [LICENSE](https://basin3d.readthedocs.io/en/stable/license_agreement.html) file for licensing details

## Acknowledgments

This research is supported as part of the Watershed Function Scientific Focus Area and a DOE Early Career Project funded by the U.S. Department of Energy, Office of Science, Office of Biological and Environmental Research under Award no. DE-AC02-05CH11231. This research used resources of the National Energy Research Scientific Computing Center (NERSC), a U.S. Department of Energy Office of Science User Facility operated under Contract No. DE-AC02-05CH11231.

 