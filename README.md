# basin3d-core
Broker for Assimilation, Synthesis and Integration of eNvironmental Diverse, Distributed Datasets.
A data synthesis framework for earth science data.


## Development Practices

* basin3d-core uses the [GitFlow model](https://datasift.github.io/gitflow/IntroducingGitFlow.html) 
  of branching and code versioning in git. 
* Code development will be peformed in a feature/development branch of the repo. Commits will not be made directly to the master branch of basin3d-core repo.  Developers will submit a pull request that is then merged by another team member, if another team member is available.
* Each pull request should contain only related modifications to a feature or bug fix.  
* Sensitive information (secret keys, usernames etc) and configuration data (e.g database host port) should not be checked in to the repo.
* A practice of rebasing with the main repo should be used rather that merge commmits.  

## Getting Started

### Prerequisities
BASIN-D3  requires:

* Python (>= 3.7)


### Get the code

These instructions will get you a copy of the project up and running on your local machine for 
development and testing purposes. 

    $ git clone git@github.com:Watershed-Function-SFA/basin3d-core.git
    $ cd basin3d-core
    

## Develop
Setup virtualenv for development and testing purposes. All basin-3d tests
are in `tests/`.
  
Create an Anaconda environment

    conda create -y -n basin3d-core python=3.7
	
Activate the new environment and prepare it for development

	source activate basin3d-core
	conda develop -npf -n basin3d-core .

Install  basin3d-core and its dependencies

	conda install $(cat requirements.txt) pytest
	pip install pytest-flake8 pytest-mypy pytest-cov sphinx PSphinxTheme
	python setup.py develop
	
Run the tests (mypy and flake8 tests executed by default)

     pytest 
     
Run the tests with covera

    pytest --cov=basin3d
     
Run the tests with coverage ONLY

     pytest --cov=basin3d tests
    

## Documentation
Sphinx is used to generate documentation. You first need
to create a virtual environment for generating the docs.

    $ source activate basin3d-core
    $ pip install -r docs/requirements.txt
    
Generate the documentation
   
    $ cd docs
    $ make html

Review the generated documentation

    $ open _build/html/index.html

# Install

 
Install a source distribution with pip:

    $ pip install basin3d-core-<version>.tar.gz
    
To get started read the [setup](docs/getting_started.rst) documentation

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, 
see the [tags on this repository](https://github.com/Watershed-Function-SFA/basin3d-core/tags). 

Workflow for tagging and building release:

1. checkout the version to tag from `master`
1. `git tag -a v[version]-[release] -m "Tagging release v[version]-[release]"`
1. build distribution with `setup.py`
1. `git push origin v[version]-[release]`

## Authors

* **Charuleka Varadharajan** - [LBL](http://eesa.lbl.gov/profiles/charuleka-varadharajan/)
* **Valerie Hendrix**  - [LBL](https://dst.lbl.gov/people.php?p=ValHendrix)
* **Danielle Svehla Christianson** - [LBL](https://crd.lbl.gov/departments/data-science-and-technology/uss/staff/danielle-christianson/)


## Copyright

Broker for Assimilation, Synthesis and Integration of eNvironmental Diverse, Distributed Datasets Core  (basin3d-core) Copyright (c) 2019, The
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

See [LICENSE](LICENSE) file for licensing details

## Acknowledgments

This research is supported as part of the Watershed Function Scientific Focus Area funded by the U.S. Department of Energy, Office of Science, Office of Biological and Environmental Research under Award no. DE-AC02-05CH11231. This research used resources of the National Energy Research Scientific Computing Center (NERSC), U.S. Department of Energy Office of Science User Facility operated under Contract No. DE-AC02-05CH11231. 
