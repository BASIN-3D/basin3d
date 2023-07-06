## How to contribute to basin3d

## Quick Links
* [Documentation]()
* [Issue Template: Bugs](.github/ISSUE_TEMPLATE/bug_report.md)
* [Issue Template: New Feature](.github/ISSUE_TEMPLATE/feature_request.md)
* [Pull Request Template](.github/pull_request_template.md)
* [Code of Conduct](CODE_OF_CONDUCT.md)


## Development Practices

* basin3d uses the [GitFlow model](https://datasift.github.io/gitflow/IntroducingGitFlow.html) 
  of branching and code versioning in git. 
* Code development will be performed in a feature/development branch of the repo. Commits will not be made directly to the master branch of basin3d repo.  Developers will submit a pull request that is then merged by another team member, if another team member is available.
* Each pull request should contain only related modifications to a feature or bug fix.  
* Sensitive information (secret keys, usernames etc) and configuration data (e.g database host port) should not be checked into the repo.
* A practice of rebasing with the main repo should be used rather than merge commits.  

## Develop

Setup virtualenv for development and testing purposes. All basin3d tests
are in `tests/`.
  
Create an Anaconda environment

    $ conda create -y -n basin3d python=<version>
	
Activate the new environment and prepare it for development

	$ conda activate basin3d
	$ conda develop -npf -n basin3d .

Install basin3d and its dependencies

	$ pip install -e ".[dev]"
	
Run the tests (mypy and flake8 tests executed by default)

    $ pytest 
     
Run the tests with coverage

    $ pytest --cov=basin3d
     
Run the tests with coverage ONLY

    $ pytest --cov=basin3d tests


## Documentation

Sphinx is used to generate documentation. You first need to create a virtual environment for generating the docs.
	
	$ conda activate basin3d
    $ pip install -e ".[docs]"
   
Generate the documentation
	
	$ cd docs
	$ make html

Review the generated documentation

	$ open _build/html/index.html


## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, 
see the [tags on this repository](https://github.com/BASIN-3D/basin3d/tags). 

Workflow for tagging and building release:

1. checkout the version to tag from `main`
1. `$ git tag -a v[version]-[release] -m "Tagging release v[version]-[release]"`
1. build distribution with `$ setup.py`
1. `$ git push origin v[version]-[release]`

## Questions
Our preferred channels of communication are public. Please open a [new discussion topic](https://github.com/BASIN-3D/basin3d/discussions) on Github discussions
